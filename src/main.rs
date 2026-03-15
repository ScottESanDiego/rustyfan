use clap::Parser;
use directories::ProjectDirs;
use nix::unistd::Uid;
use serde::{Deserialize, Serialize};
use std::{
    cmp,
    collections::{HashMap, HashSet},
    fs::{self, File, ReadDir},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CorrelationPoint {
    pwm: u8,
    rpm: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    interval: Option<u64>,
    sensors: Vec<SensorConfig>,
    fans: Vec<FanConfig>,
    associations: Vec<AssociationConfig>,
    calibration: Option<CalibrationConfig>,
}

impl Default for ConfigFile {
    fn default() -> Self {
        Self {
            interval: Some(5),
            sensors: Vec::new(),
            fans: Vec::new(),
            associations: Vec::new(),
            calibration: Some(CalibrationConfig::default()),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct SensorConfig {
    id: String,
    spec: String,
    min_temp: f64,
    max_temp: f64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct FanConfig {
    id: String,
    pwm_spec: String,
    rpm_spec: String,
    correlation_file: PathBuf,
    min_rpm: u32,
    max_rpm: u32,
    fail_safe_pwm: Option<u8>,
    min_expected_rpm: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct AssociationConfig {
    sensor_id: String,
    fan_id: String,
    weight: f64,
    min_contribution_rpm: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct CalibrationConfig {
    min_pwm: u8,
    max_pwm: u8,
    pwm_step: u8,
    delay_ms: u64,
}

impl Default for CalibrationConfig {
    fn default() -> Self {
        Self {
            min_pwm: 0,
            max_pwm: 255,
            pwm_step: 10,
            delay_ms: 2000,
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about = "Controls multiple fans from sensor associations.")]
struct CliArgs {
    #[arg(long, short = 'c', value_name = "FILE_PATH")]
    config: Option<PathBuf>,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    verbose: bool,

    #[arg(long, value_name = "LEVEL")]
    log_level: Option<String>,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    allow_config_fallback: bool,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    validate_only: bool,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    status: bool,

    #[arg(long = "calibrate-fan", value_name = "FAN_ID")]
    calibrate_fan: Option<String>,

    #[arg(long, short = 'i', value_name = "SECONDS")]
    interval: Option<u64>,

    #[arg(long, value_name = "PWM_VAL")]
    min_pwm_cal: Option<u8>,

    #[arg(long, value_name = "PWM_VAL")]
    max_pwm_cal: Option<u8>,

    #[arg(long, value_name = "PWM_STEP")]
    pwm_step_cal: Option<u8>,

    #[arg(long, value_name = "MS")]
    calibration_delay_ms: Option<u64>,
}

struct ErrorRateLimiter {
    last_error_time: std::time::Instant,
    error_count: u32,
    suppressed_count: u32,
}

impl ErrorRateLimiter {
    fn new() -> Self {
        Self {
            last_error_time: std::time::Instant::now(),
            error_count: 0,
            suppressed_count: 0,
        }
    }

    fn should_log(&mut self) -> bool {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_error_time);

        if elapsed > Duration::from_secs(60) {
            if self.suppressed_count > 0 {
                warn!(
                    suppressed_count = self.suppressed_count,
                    "Suppressed similar errors in the last minute"
                );
            }
            self.last_error_time = now;
            self.error_count = 0;
            self.suppressed_count = 0;
        }

        self.error_count = self.error_count.saturating_add(1);
        if self.error_count <= 5 {
            true
        } else {
            self.suppressed_count = self.suppressed_count.saturating_add(1);
            false
        }
    }
}

struct FanHealthMonitor {
    consecutive_failures: u32,
}

impl FanHealthMonitor {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
        }
    }

    fn check_fan_response(
        &mut self,
        target_pwm: u8,
        current_rpm: Result<u32, io::Error>,
        min_rpm_expected: u32,
    ) -> Result<(), String> {
        match current_rpm {
            Ok(rpm) => {
                if target_pwm > 50 && rpm < min_rpm_expected / 2 {
                    self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                    if self.consecutive_failures >= 5 {
                        return Err(format!(
                            "Fan health check failed: PWM={} but RPM={} (expected >= {})",
                            target_pwm,
                            rpm,
                            min_rpm_expected / 2
                        ));
                    }
                } else {
                    self.consecutive_failures = 0;
                }
                Ok(())
            }
            Err(e) => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                if self.consecutive_failures >= 3 {
                    Err(format!("Unable to read fan RPM after 3 attempts: {}", e))
                } else {
                    Ok(())
                }
            }
        }
    }
}

struct AutoModeGuard {
    pwm_enable_path: PathBuf,
    original_value: String,
}

impl AutoModeGuard {
    fn new(pwm_enable_path: PathBuf, original_value: String) -> Self {
        Self {
            pwm_enable_path,
            original_value,
        }
    }

    fn restore(&self) {
        if let Err(e) = fs::write(&self.pwm_enable_path, &self.original_value) {
            error!(
                error = %e,
                path = %self.pwm_enable_path.display(),
                original = %self.original_value,
                "Failed to restore original PWM mode"
            );
        } else {
            info!(
                path = %self.pwm_enable_path.display(),
                original = %self.original_value,
                "Restored original PWM mode"
            );
        }
    }
}

impl Drop for AutoModeGuard {
    fn drop(&mut self) {
        self.restore();
    }
}

struct SensorRuntime {
    config: SensorConfig,
    temperature_path: PathBuf,
    temp_limiter: ErrorRateLimiter,
    last_temp: Option<f64>,
}

#[derive(Clone)]
struct FanPaths {
    pwm_path: PathBuf,
    pwm_enable_path: PathBuf,
    rpm_input_path: PathBuf,
}

struct FanRuntime {
    config: FanConfig,
    paths: FanPaths,
    correlation: Vec<CorrelationPoint>,
    min_positive_rpm: u32,
    pwm_limiter: ErrorRateLimiter,
    health_monitor: FanHealthMonitor,
    last_successful_pwm: Option<u8>,
}

struct RuntimeState {
    sensors: HashMap<String, SensorRuntime>,
    fans: HashMap<String, FanRuntime>,
    associations_by_fan: HashMap<String, Vec<AssociationConfig>>,
    interval_seconds: u64,
}

struct HwmonResolver {
    cache: HashMap<String, PathBuf>,
}

impl HwmonResolver {
    fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    fn hwmon_base_for(&mut self, device_name: &str) -> Result<PathBuf, String> {
        if let Some(path) = self.cache.get(device_name) {
            return Ok(path.clone());
        }

        const HWMON_BASE: &str = "/sys/class/hwmon";
        let entries: ReadDir = fs::read_dir(HWMON_BASE)
            .map_err(|e| format!("Failed to read {}: {}", HWMON_BASE, e))?;

        let mut found_path: Option<PathBuf> = None;
        for entry in entries {
            let entry = entry.map_err(|e| format!("Error reading entry in {}: {}", HWMON_BASE, e))?;
            let path = entry.path();
            if !path.is_dir() || !path.file_name().is_some_and(|name| name.to_string_lossy().starts_with("hwmon")) {
                continue;
            }

            let name_path = path.join("name");
            if !name_path.exists() {
                continue;
            }

            let name_content = fs::read_to_string(&name_path)
                .map_err(|e| format!("Failed to read {}: {}", name_path.display(), e))?;
            if name_content.trim() == device_name {
                if found_path.is_some() {
                    return Err(format!(
                        "Multiple hwmon directories found for device name '{}'",
                        device_name
                    ));
                }
                found_path = Some(path);
            }
        }

        let resolved = found_path.ok_or_else(|| {
            format!("No hwmon directory found for device name '{}'", device_name)
        })?;
        self.cache.insert(device_name.to_string(), resolved.clone());
        Ok(resolved)
    }

    fn resolve_temp_spec(&mut self, spec: &str) -> Result<PathBuf, String> {
        let (device, base_name) = parse_spec(spec)?;
        let base = self.hwmon_base_for(device)?;
        let file_name = if base_name.ends_with("_input") {
            base_name.to_string()
        } else {
            format!("{}_input", base_name)
        };
        Ok(base.join(file_name))
    }

    fn resolve_fan_paths(&mut self, pwm_spec: &str, rpm_spec: &str) -> Result<FanPaths, String> {
        let (fan_device, pwm_name) = parse_spec(pwm_spec)?;
        let fan_base = self.hwmon_base_for(fan_device)?;

        let (rpm_device, rpm_name) = parse_spec(rpm_spec)?;
        let rpm_base = self.hwmon_base_for(rpm_device)?;

        let rpm_input_name = if rpm_name.ends_with("_input") {
            rpm_name.to_string()
        } else {
            format!("{}_input", rpm_name)
        };

        Ok(FanPaths {
            pwm_path: fan_base.join(pwm_name),
            pwm_enable_path: fan_base.join(format!("{}_enable", pwm_name)),
            rpm_input_path: rpm_base.join(rpm_input_name),
        })
    }
}

fn parse_spec(spec: &str) -> Result<(&str, &str), String> {
    spec.split_once('/')
        .ok_or_else(|| format!("Invalid spec format: '{}'. Expected '<device_name>/<filename>'", spec))
}

fn init_logging(verbose: bool, cli_log_level: Option<&str>) -> Result<(), String> {
    let level = match cli_log_level {
        Some(level) => level.to_ascii_lowercase(),
        None => {
            if verbose {
                "debug".to_string()
            } else {
                "info".to_string()
            }
        }
    };

    let filter = EnvFilter::try_new(level)
        .or_else(|_| EnvFilter::try_new("info"))
        .map_err(|e| format!("Failed to initialize log filter: {}", e))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|e| format!("Failed to initialize logger: {}", e))?;

    Ok(())
}

fn find_default_config_path() -> Option<PathBuf> {
    if let Some(proj_dirs) = ProjectDirs::from("com", "rustyfan", "rustyfan") {
        let user_config_path = proj_dirs.config_dir().join("config.toml");
        if user_config_path.exists() {
            return Some(user_config_path);
        }
    }

    let current_dir_path = PathBuf::from("./rustyfan.toml");
    if current_dir_path.exists() {
        return Some(current_dir_path);
    }

    None
}

fn load_config_file(config_path: Option<PathBuf>, allow_fallback: bool) -> Result<ConfigFile, String> {
    let path = match config_path.or_else(find_default_config_path) {
        Some(path) => path,
        None => {
            if allow_fallback {
                warn!("No config file specified/found, using empty defaults due to --allow-config-fallback");
                return Ok(ConfigFile::default());
            }
            return Err("No config file specified and no default config found".to_string());
        }
    };

    info!(config_path = %path.display(), "Loading configuration file");
    match fs::read_to_string(&path) {
        Ok(contents) => match toml::from_str(&contents) {
            Ok(parsed) => Ok(parsed),
            Err(e) => {
                if allow_fallback {
                    warn!(error = %e, config_path = %path.display(), "Failed to parse config; using defaults due to --allow-config-fallback");
                    Ok(ConfigFile::default())
                } else {
                    Err(format!("Failed to parse config file '{}': {}", path.display(), e))
                }
            }
        },
        Err(e) => {
            if allow_fallback {
                warn!(error = %e, config_path = %path.display(), "Failed to read config; using defaults due to --allow-config-fallback");
                Ok(ConfigFile::default())
            } else {
                Err(format!("Failed to read config file '{}': {}", path.display(), e))
            }
        }
    }
}

fn validate_config(config: &ConfigFile, interval_override: Option<u64>) -> Result<(), String> {
    if config.sensors.is_empty() {
        return Err("Config must contain at least one sensor".to_string());
    }
    if config.fans.is_empty() {
        return Err("Config must contain at least one fan".to_string());
    }
    if config.associations.is_empty() {
        return Err("Config must contain at least one sensor->fan association".to_string());
    }

    let mut sensor_ids = HashSet::new();
    for sensor in &config.sensors {
        if sensor.id.trim().is_empty() {
            return Err("Sensor id cannot be empty".to_string());
        }
        if !sensor_ids.insert(sensor.id.clone()) {
            return Err(format!("Duplicate sensor id '{}'", sensor.id));
        }
        if sensor.min_temp >= sensor.max_temp {
            return Err(format!(
                "Sensor '{}' has invalid temperature range: {} >= {}",
                sensor.id, sensor.min_temp, sensor.max_temp
            ));
        }
    }

    let mut fan_ids = HashSet::new();
    for fan in &config.fans {
        if fan.id.trim().is_empty() {
            return Err("Fan id cannot be empty".to_string());
        }
        if !fan_ids.insert(fan.id.clone()) {
            return Err(format!("Duplicate fan id '{}'", fan.id));
        }
        if fan.min_rpm >= fan.max_rpm {
            return Err(format!(
                "Fan '{}' has invalid RPM range: {} >= {}",
                fan.id, fan.min_rpm, fan.max_rpm
            ));
        }
    }

    let mut weight_sums: HashMap<&str, f64> = HashMap::new();
    let mut associations_per_fan: HashMap<&str, usize> = HashMap::new();

    for (assoc_idx, assoc) in config.associations.iter().enumerate() {
        if !sensor_ids.contains(&assoc.sensor_id) {
            return Err(format!(
                "Association[{}] references unknown sensor_id '{}'",
                assoc_idx,
                assoc.sensor_id
            ));
        }
        if !fan_ids.contains(&assoc.fan_id) {
            return Err(format!(
                "Association[{}] references unknown fan_id '{}'",
                assoc_idx,
                assoc.fan_id
            ));
        }
        if !(assoc.weight.is_finite() && assoc.weight > 0.0) {
            return Err(format!(
                "Association[{}] sensor '{}' -> fan '{}' has invalid weight {}",
                assoc_idx, assoc.sensor_id, assoc.fan_id, assoc.weight
            ));
        }

        *weight_sums.entry(&assoc.fan_id).or_insert(0.0) += assoc.weight;
        *associations_per_fan.entry(&assoc.fan_id).or_insert(0) += 1;
    }

    for fan in &config.fans {
        let assoc_count = associations_per_fan.get(fan.id.as_str()).copied().unwrap_or(0);
        if assoc_count == 0 {
            return Err(format!("Fan '{}' has no sensor associations", fan.id));
        }

        let weight_sum = weight_sums.get(fan.id.as_str()).copied().unwrap_or(0.0);
        if !weight_sum.is_finite() || weight_sum <= 0.0 {
            return Err(format!(
                "Fan '{}' associations must have positive total weight",
                fan.id
            ));
        }
    }

    let interval = interval_override.or(config.interval).unwrap_or(5);
    if interval == 0 {
        return Err("Interval must be > 0 seconds".to_string());
    }

    Ok(())
}

fn create_backup_before_save(path: &Path) -> Result<(), io::Error> {
    if path.exists() {
        let backup_path = path.with_extension("json.backup");
        fs::copy(path, backup_path)?;
    }
    Ok(())
}

fn save_correlation(data: &[CorrelationPoint], path: &Path) -> Result<(), io::Error> {
    if let Err(e) = create_backup_before_save(path) {
        warn!(error = %e, path = %path.display(), "Could not create backup before save");
    }

    let temp_path = path.with_extension("tmp");
    let _ = fs::remove_file(&temp_path);

    let file = File::create(&temp_path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, data)
        .map_err(|e| io::Error::other(format!("JSON serialization error: {}", e)))?;
    writer.flush()?;
    writer.get_mut().sync_all()?;
    drop(writer);

    fs::rename(&temp_path, path)?;
    Ok(())
}

fn load_correlation(path: &Path) -> Result<Vec<CorrelationPoint>, io::Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut data: Vec<CorrelationPoint> = serde_json::from_reader(reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("JSON parsing error: {}", e)))?;
    data.sort_unstable();
    Ok(data)
}

fn validate_correlation_data(data: &[CorrelationPoint]) -> Result<(), String> {
    if data.is_empty() {
        return Err("Correlation data is empty".to_string());
    }

    let mut seen_pwms = HashSet::new();
    for point in data {
        if !seen_pwms.insert(point.pwm) {
            return Err(format!("Duplicate PWM value {} found in correlation data", point.pwm));
        }
    }

    for i in 1..data.len() {
        if data[i].pwm <= data[i - 1].pwm {
            return Err(format!(
                "Correlation data not sorted by PWM: {} follows {}",
                data[i].pwm,
                data[i - 1].pwm
            ));
        }
    }

    if !data.iter().any(|p| p.rpm > 0) {
        return Err("No correlation points with RPM > 0".to_string());
    }

    Ok(())
}

fn read_temperature(path: impl AsRef<Path>) -> Result<f64, io::Error> {
    let raw = fs::read_to_string(path)?;
    match raw.trim().parse::<i32>() {
        Ok(value) => Ok(value as f64 / 1000.0),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to parse temperature value: {}", e),
        )),
    }
}

fn read_temperature_with_retry(path: impl AsRef<Path>, max_retries: u32) -> Result<f64, io::Error> {
    let path = path.as_ref();
    let mut last_error: Option<io::Error> = None;

    for attempt in 0..=max_retries {
        match read_temperature(path) {
            Ok(temp) => {
                if !(-100.0..=200.0).contains(&temp) {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Suspicious temperature reading: {:.1}C", temp),
                    );
                    if attempt < max_retries {
                        warn!(temp, "Suspicious temperature reading, retrying");
                        thread::sleep(Duration::from_millis(50));
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
                return Ok(temp);
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < max_retries {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| io::Error::other("Unknown temperature read error")))
}

fn write_pwm(path: impl AsRef<Path>, value: u8) -> Result<(), io::Error> {
    fs::write(path, value.to_string())
}

fn write_pwm_with_retry(path: impl AsRef<Path>, value: u8, max_retries: u32) -> Result<(), io::Error> {
    let path = path.as_ref();
    let mut last_error: Option<io::Error> = None;

    for attempt in 0..=max_retries {
        match write_pwm(path, value) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_error = Some(e);
                if attempt < max_retries {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| io::Error::other("Unknown PWM write error")))
}

fn read_fan_rpm(path: impl AsRef<Path>) -> Result<u32, io::Error> {
    let raw = fs::read_to_string(path)?;
    match raw.trim().parse::<u32>() {
        Ok(value) => Ok(value),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to parse RPM value: {}", e),
        )),
    }
}

fn enable_manual_pwm(path: impl AsRef<Path>) -> Result<(), io::Error> {
    fs::write(path, "1")
}

fn calculate_target_rpm(temp: f64, min_temp: f64, max_temp: f64, min_rpm: u32, max_rpm: u32) -> u32 {
    if temp <= min_temp {
        return min_rpm;
    }
    if temp >= max_temp {
        return max_rpm;
    }

    let temp_range = max_temp - min_temp;
    if temp_range <= 0.0 {
        return max_rpm;
    }

    let rpm_range = max_rpm as f64 - min_rpm as f64;
    let target_rpm = min_rpm as f64 + (temp - min_temp) * rpm_range / temp_range;
    target_rpm.round().clamp(min_rpm as f64, max_rpm as f64) as u32
}

fn find_pwm_for_rpm(target_rpm: u32, correlation_data: &[CorrelationPoint]) -> Option<u8> {
    let min_spinning_pwm = correlation_data.iter().find(|p| p.rpm > 0)?.pwm;

    if target_rpm == 0 {
        return Some(min_spinning_pwm);
    }

    if let Some(point) = correlation_data.iter().find(|point| point.rpm >= target_rpm) {
        if point.rpm > 0 {
            return Some(point.pwm);
        }
        return Some(min_spinning_pwm);
    }

    correlation_data
        .iter()
        .filter(|p| p.rpm > 0)
        .max_by_key(|p| p.rpm)
        .map(|p| p.pwm)
}

fn validate_hardware_access(sensor_path: &Path, rpm_path: &Path) -> Result<(), String> {
    fs::read_to_string(sensor_path)
        .map_err(|e| format!("Cannot read sensor path {}: {}", sensor_path.display(), e))?;
    fs::read_to_string(rpm_path)
        .map_err(|e| format!("Cannot read fan RPM path {}: {}", rpm_path.display(), e))?;
    Ok(())
}

fn build_runtime(config: &ConfigFile, interval_override: Option<u64>) -> Result<RuntimeState, String> {
    let mut resolver = HwmonResolver::new();

    let mut sensors: HashMap<String, SensorRuntime> = HashMap::new();
    for sensor in &config.sensors {
        let temperature_path = resolver.resolve_temp_spec(&sensor.spec)?;
        sensors.insert(
            sensor.id.clone(),
            SensorRuntime {
                config: sensor.clone(),
                temperature_path,
                temp_limiter: ErrorRateLimiter::new(),
                last_temp: None,
            },
        );
    }

    let mut fans: HashMap<String, FanRuntime> = HashMap::new();
    for fan in &config.fans {
        let paths = resolver.resolve_fan_paths(&fan.pwm_spec, &fan.rpm_spec)?;
        let correlation = load_correlation(&fan.correlation_file)
            .map_err(|e| format!("Failed loading correlation for fan '{}': {}", fan.id, e))?;
        validate_correlation_data(&correlation)
            .map_err(|e| format!("Invalid correlation for fan '{}': {}", fan.id, e))?;

        let min_positive_rpm = correlation
            .iter()
            .filter(|p| p.rpm > 0)
            .map(|p| p.rpm)
            .min()
            .ok_or_else(|| format!("Fan '{}' correlation has no positive RPM points", fan.id))?;

        fans.insert(
            fan.id.clone(),
            FanRuntime {
                config: fan.clone(),
                paths,
                correlation,
                min_positive_rpm,
                pwm_limiter: ErrorRateLimiter::new(),
                health_monitor: FanHealthMonitor::new(),
                last_successful_pwm: None,
            },
        );
    }

    let mut associations_by_fan: HashMap<String, Vec<AssociationConfig>> = HashMap::new();
    for association in &config.associations {
        associations_by_fan
            .entry(association.fan_id.clone())
            .or_default()
            .push(association.clone());
    }

    for fan_id in fans.keys() {
        if !associations_by_fan.contains_key(fan_id) {
            return Err(format!("Fan '{}' has no associations after grouping", fan_id));
        }
    }

    let interval_seconds = interval_override.or(config.interval).unwrap_or(5);
    Ok(RuntimeState {
        sensors,
        fans,
        associations_by_fan,
        interval_seconds,
    })
}

fn normalize_contributions(contribs: &[(u32, f64)]) -> u32 {
    let mut weighted_sum = 0.0_f64;
    let mut total_weight = 0.0_f64;

    for (rpm, weight) in contribs {
        weighted_sum += *rpm as f64 * *weight;
        total_weight += *weight;
    }

    if total_weight <= 0.0 {
        0
    } else {
        (weighted_sum / total_weight).round() as u32
    }
}

fn enable_all_fans_manual(fans: &HashMap<String, FanRuntime>) -> Result<Vec<AutoModeGuard>, String> {
    let mut guards = Vec::new();
    for (fan_id, fan) in fans {
        let original_value = fs::read_to_string(&fan.paths.pwm_enable_path)
            .map_err(|e| format!("Failed reading pwm_enable for fan '{}': {}", fan_id, e))?
            .trim()
            .to_string();

        enable_manual_pwm(&fan.paths.pwm_enable_path)
            .map_err(|e| format!("Failed enabling manual PWM for fan '{}': {}", fan_id, e))?;

        info!(fan_id, "Manual PWM enabled");
        guards.push(AutoModeGuard::new(
            fan.paths.pwm_enable_path.clone(),
            original_value,
        ));
    }
    Ok(guards)
}

fn run_calibration(
    fan: &FanRuntime,
    calibration: &CalibrationConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting calibration for fan '{}' ---", fan.config.id);
    println!("PWM Path: {}", fan.paths.pwm_path.display());
    println!("RPM Path: {}", fan.paths.rpm_input_path.display());
    println!("Correlation file: {}", fan.config.correlation_file.display());

    let delay = Duration::from_millis(calibration.delay_ms);
    let mut points = Vec::new();
    let mut pwms = HashSet::new();

    pwms.insert(calibration.min_pwm);
    if calibration.min_pwm <= calibration.max_pwm {
        let mut current = calibration.min_pwm;
        while current < calibration.max_pwm && current < 255 {
            let step = if calibration.pwm_step == 0 {
                1
            } else {
                calibration.pwm_step
            };
            let next = current.saturating_add(step);
            if next <= current {
                break;
            }
            current = cmp::min(next, calibration.max_pwm);
            pwms.insert(current);
            if current == 255 {
                break;
            }
        }
    }
    pwms.insert(calibration.max_pwm);

    let mut sorted_pwms: Vec<u8> = pwms.into_iter().collect();
    sorted_pwms.sort_unstable();
    println!("Testing PWM values: {:?}", sorted_pwms);

    write_pwm(&fan.paths.pwm_path, calibration.min_pwm)?;
    thread::sleep(delay);

    for pwm in sorted_pwms {
        print!("Setting PWM: {} -> ", pwm);
        io::stdout().flush()?;

        write_pwm(&fan.paths.pwm_path, pwm)?;
        thread::sleep(delay);

        match read_fan_rpm(&fan.paths.rpm_input_path) {
            Ok(rpm) => {
                println!("{} RPM", rpm);
                points.push(CorrelationPoint { pwm, rpm });
            }
            Err(e) => println!("error reading RPM: {}", e),
        }
    }

    println!("Saving correlation data...");
    points.sort_unstable();
    validate_correlation_data(&points)?;
    save_correlation(&points, &fan.config.correlation_file)?;

    Ok(())
}

fn run_validate_only(state: &mut RuntimeState) -> Result<(), String> {
    for (sensor_id, sensor) in &state.sensors {
        if !sensor.temperature_path.exists() {
            return Err(format!(
                "Sensor '{}' path does not exist: {}",
                sensor_id,
                sensor.temperature_path.display()
            ));
        }
    }

    for (fan_id, fan) in &state.fans {
        if !fan.paths.pwm_path.exists() {
            return Err(format!(
                "Fan '{}' PWM path does not exist: {}",
                fan_id,
                fan.paths.pwm_path.display()
            ));
        }
        if !fan.paths.pwm_enable_path.exists() {
            return Err(format!(
                "Fan '{}' PWM enable path does not exist: {}",
                fan_id,
                fan.paths.pwm_enable_path.display()
            ));
        }
        if !fan.paths.rpm_input_path.exists() {
            return Err(format!(
                "Fan '{}' RPM path does not exist: {}",
                fan_id,
                fan.paths.rpm_input_path.display()
            ));
        }
    }

    for (fan_id, assocs) in &state.associations_by_fan {
        let fan = state
            .fans
            .get(fan_id)
            .ok_or_else(|| format!("Internal error: missing fan '{}' in runtime", fan_id))?;
        for (assoc_idx, assoc) in assocs.iter().enumerate() {
            let sensor = state.sensors.get(&assoc.sensor_id).ok_or_else(|| {
                format!(
                    "Internal error: association[{}] references missing sensor '{}'",
                    assoc_idx, assoc.sensor_id
                )
            })?;
            validate_hardware_access(&sensor.temperature_path, &fan.paths.rpm_input_path)?;
        }
    }

    let sampled_temps = sample_temperatures(state);
    let decisions = simulate_control_decisions(state, &sampled_temps)?;

    info!("Validation succeeded: configuration, paths, and basic sensor/fan reads are valid");
    info!("Simulated one control decision pass (no PWM writes):");
    for line in decisions {
        info!("{}", line);
    }

    Ok(())
}

fn print_status(state: &RuntimeState) {
    println!("rustyfan status");
    println!("  interval_seconds: {}", state.interval_seconds);
    println!("  sensors: {}", state.sensors.len());
    println!("  fans: {}", state.fans.len());

    println!("\nSensors:");
    for (sensor_id, sensor) in &state.sensors {
        println!(
            "  - {}: spec={} path={} temp_range=[{:.1}, {:.1}]",
            sensor_id,
            sensor.config.spec,
            sensor.temperature_path.display(),
            sensor.config.min_temp,
            sensor.config.max_temp
        );
    }

    println!("\nFans:");
    for (fan_id, fan) in &state.fans {
        println!(
            "  - {}: pwm={} pwm_enable={} rpm={} corr={} rpm_range=[{}, {}] fail_safe_pwm={:?}",
            fan_id,
            fan.paths.pwm_path.display(),
            fan.paths.pwm_enable_path.display(),
            fan.paths.rpm_input_path.display(),
            fan.config.correlation_file.display(),
            fan.config.min_rpm,
            fan.config.max_rpm,
            fan.config.fail_safe_pwm
        );

        if let Some(assocs) = state.associations_by_fan.get(fan_id) {
            let total_weight: f64 = assocs.iter().map(|a| a.weight).sum();
            println!("    associations (normalized weights):");
            for assoc in assocs {
                let normalized = if total_weight > 0.0 {
                    assoc.weight / total_weight
                } else {
                    0.0
                };
                println!(
                    "      - sensor={} raw_weight={:.4} normalized={:.4} min_contribution_rpm={:?}",
                    assoc.sensor_id,
                    assoc.weight,
                    normalized,
                    assoc.min_contribution_rpm
                );
            }
        }
    }
}

fn sample_temperatures(state: &mut RuntimeState) -> HashMap<String, f64> {
    let mut sampled_temps: HashMap<String, f64> = HashMap::new();
    for (sensor_id, sensor_rt) in &mut state.sensors {
        match read_temperature_with_retry(&sensor_rt.temperature_path, 3) {
            Ok(temp) => {
                sensor_rt.last_temp = Some(temp);
                sampled_temps.insert(sensor_id.clone(), temp);
            }
            Err(e) => {
                if sensor_rt.temp_limiter.should_log() {
                    warn!(sensor_id, error = %e, "Failed to read sensor temperature");
                }
                if let Some(last) = sensor_rt.last_temp {
                    sampled_temps.insert(sensor_id.clone(), last);
                }
            }
        }
    }
    sampled_temps
}

fn simulate_control_decisions(state: &RuntimeState, sampled_temps: &HashMap<String, f64>) -> Result<Vec<String>, String> {
    let mut lines = Vec::new();

    for (fan_id, fan_rt) in &state.fans {
        let Some(assocs) = state.associations_by_fan.get(fan_id) else {
            continue;
        };

        let mut contributions: Vec<(u32, f64)> = Vec::new();
        for assoc in assocs {
            if let Some(temp) = sampled_temps.get(&assoc.sensor_id) {
                let Some(sensor_rt) = state.sensors.get(&assoc.sensor_id) else {
                    continue;
                };

                let mut target_rpm = calculate_target_rpm(
                    *temp,
                    sensor_rt.config.min_temp,
                    sensor_rt.config.max_temp,
                    fan_rt.config.min_rpm,
                    fan_rt.config.max_rpm,
                );

                if let Some(min_contribution) = assoc.min_contribution_rpm {
                    target_rpm = target_rpm.max(min_contribution);
                }

                contributions.push((target_rpm, assoc.weight));
            }
        }

        if contributions.is_empty() {
            lines.push(format!(
                "fan={} action=fail-safe reason=no_healthy_sensor_data fail_safe_pwm={:?}",
                fan_id, fan_rt.config.fail_safe_pwm
            ));
            continue;
        }

        let weighted_target_rpm = normalize_contributions(&contributions)
            .clamp(fan_rt.config.min_rpm, fan_rt.config.max_rpm);
        let Some(target_pwm) = find_pwm_for_rpm(weighted_target_rpm, &fan_rt.correlation) else {
            return Err(format!("Could not find PWM for fan '{}'", fan_id));
        };

        lines.push(format!(
            "fan={} action=set-pwm target_rpm={} target_pwm={}",
            fan_id, weighted_target_rpm, target_pwm
        ));
    }

    Ok(lines)
}

fn run_control_loop(
    state: &mut RuntimeState,
    running: Arc<AtomicBool>,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting multi-fan control loop");
    info!(interval_seconds = state.interval_seconds, "Control loop interval");

    let mut last_path_check = Instant::now();

    while running.load(Ordering::SeqCst) {
        if last_path_check.elapsed() >= Duration::from_secs(60) {
            for (sensor_id, sensor) in &state.sensors {
                if !sensor.temperature_path.exists() {
                    return Err(format!(
                        "Sensor path disappeared for '{}': {}",
                        sensor_id,
                        sensor.temperature_path.display()
                    )
                    .into());
                }
            }
            for (fan_id, fan) in &state.fans {
                if !fan.paths.pwm_path.exists() || !fan.paths.rpm_input_path.exists() {
                    return Err(format!("Fan paths disappeared for '{}'", fan_id).into());
                }
            }
            last_path_check = Instant::now();
        }

        let sampled_temps = sample_temperatures(state);

        for (fan_id, fan_rt) in &mut state.fans {
            let Some(assocs) = state.associations_by_fan.get(fan_id) else {
                continue;
            };

            let mut contributions: Vec<(u32, f64)> = Vec::new();
            for assoc in assocs {
                if let Some(temp) = sampled_temps.get(&assoc.sensor_id) {
                    let Some(sensor_rt) = state.sensors.get(&assoc.sensor_id) else {
                        continue;
                    };

                    let mut target_rpm = calculate_target_rpm(
                        *temp,
                        sensor_rt.config.min_temp,
                        sensor_rt.config.max_temp,
                        fan_rt.config.min_rpm,
                        fan_rt.config.max_rpm,
                    );

                    if let Some(min_contribution) = assoc.min_contribution_rpm {
                        target_rpm = target_rpm.max(min_contribution);
                    }

                    contributions.push((target_rpm, assoc.weight));
                }
            }

            if contributions.is_empty() {
                warn!(fan_id, "No healthy sensor readings for fan; applying fail-safe behavior");
                if let Some(fail_pwm) = fan_rt.config.fail_safe_pwm {
                    if let Err(e) = write_pwm_with_retry(&fan_rt.paths.pwm_path, fail_pwm, 3) {
                        warn!(fan_id, fail_pwm, error = %e, "Failed writing fail-safe PWM");
                    }
                }
                continue;
            }

            let weighted_target_rpm = normalize_contributions(&contributions)
                .clamp(fan_rt.config.min_rpm, fan_rt.config.max_rpm);
            let Some(target_pwm) = find_pwm_for_rpm(weighted_target_rpm, &fan_rt.correlation) else {
                return Err(format!("Could not find PWM for fan '{}'", fan_id).into());
            };

            match write_pwm_with_retry(&fan_rt.paths.pwm_path, target_pwm, 3) {
                Ok(()) => {
                    fan_rt.last_successful_pwm = Some(target_pwm);
                    let rpm_result = read_fan_rpm(&fan_rt.paths.rpm_input_path);
                    let min_expected = fan_rt
                        .config
                        .min_expected_rpm
                        .unwrap_or(fan_rt.min_positive_rpm);

                    if let Err(e) = fan_rt
                        .health_monitor
                        .check_fan_response(target_pwm, rpm_result, min_expected)
                    {
                        return Err(format!("Fan '{}' health check failed: {}", fan_id, e).into());
                    }

                    if verbose {
                        debug!(
                            fan_id,
                            target_pwm,
                            weighted_target_rpm,
                            "Applied weighted sensor aggregation"
                        );
                    }
                }
                Err(e) => {
                    if fan_rt.pwm_limiter.should_log() {
                        warn!(fan_id, target_pwm, error = %e, "Failed writing PWM");
                    }
                }
            }
        }

        let sleep_duration = Duration::from_secs(state.interval_seconds);
        let step = Duration::from_millis(200);
        let started = Instant::now();
        while started.elapsed() < sleep_duration {
            if !running.load(Ordering::SeqCst) {
                info!("Exit signal received, terminating loop");
                break;
            }
            thread::sleep(step);
        }
    }

    info!("Control loop terminated");
    Ok(())
}

fn merged_calibration_config(base: Option<CalibrationConfig>, cli: &CliArgs) -> CalibrationConfig {
    let base = base.unwrap_or_default();
    CalibrationConfig {
        min_pwm: cli.min_pwm_cal.unwrap_or(base.min_pwm),
        max_pwm: cli.max_pwm_cal.unwrap_or(base.max_pwm),
        pwm_step: cli.pwm_step_cal.unwrap_or(base.pwm_step),
        delay_ms: cli.calibration_delay_ms.unwrap_or(base.delay_ms),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args = CliArgs::parse();
    init_logging(cli_args.verbose, cli_args.log_level.as_deref())?;

    let running = Arc::new(AtomicBool::new(true));
    let r = Arc::clone(&running);
    ctrlc::set_handler(move || {
        info!("Received Ctrl-C, signaling termination");
        r.store(false, Ordering::SeqCst);
    })
    .map_err(|e| format!("Error setting Ctrl-C handler: {}", e))?;

    if !Uid::effective().is_root() {
        return Err("Root privileges are required".into());
    }

    let config = load_config_file(cli_args.config.clone(), cli_args.allow_config_fallback)?;
    validate_config(&config, cli_args.interval)?;

    let mut state = build_runtime(&config, cli_args.interval)?;

    if cli_args.status {
        print_status(&state);
        return Ok(());
    }

    if cli_args.validate_only {
        run_validate_only(&mut state)?;
        return Ok(());
    }

    if let Some(calibrate_fan_id) = cli_args.calibrate_fan.as_deref() {
        let fan = state
            .fans
            .get(calibrate_fan_id)
            .ok_or_else(|| format!("Unknown fan id for calibration: '{}'", calibrate_fan_id))?;

        let original = fs::read_to_string(&fan.paths.pwm_enable_path)
            .map_err(|e| format!("Failed to read pwm_enable for fan '{}': {}", fan.config.id, e))?
            .trim()
            .to_string();
        enable_manual_pwm(&fan.paths.pwm_enable_path)?;
        let _guard = AutoModeGuard::new(fan.paths.pwm_enable_path.clone(), original);

        let calibration = merged_calibration_config(config.calibration.clone(), &cli_args);
        return run_calibration(fan, &calibration);
    }

    let _guards = enable_all_fans_manual(&state.fans)?;

    run_control_loop(&mut state, running, cli_args.verbose)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_aggregation_renormalizes() {
        let contribs = vec![(1000_u32, 0.7_f64), (2000_u32, 0.3_f64)];
        assert_eq!(normalize_contributions(&contribs), 1300);

        let contribs_missing = vec![(2000_u32, 0.3_f64)];
        assert_eq!(normalize_contributions(&contribs_missing), 2000);
    }

    #[test]
    fn validates_association_references() {
        let cfg = ConfigFile {
            interval: Some(5),
            sensors: vec![SensorConfig {
                id: "cpu".to_string(),
                spec: "k10temp/temp1".to_string(),
                min_temp: 40.0,
                max_temp: 80.0,
            }],
            fans: vec![FanConfig {
                id: "cpu_fan".to_string(),
                pwm_spec: "nct6799/pwm1".to_string(),
                rpm_spec: "nct6799/fan1".to_string(),
                correlation_file: PathBuf::from("fan.json"),
                min_rpm: 500,
                max_rpm: 2000,
                fail_safe_pwm: Some(200),
                min_expected_rpm: Some(400),
            }],
            associations: vec![AssociationConfig {
                sensor_id: "missing".to_string(),
                fan_id: "cpu_fan".to_string(),
                weight: 1.0,
                min_contribution_rpm: None,
            }],
            calibration: Some(CalibrationConfig::default()),
        };

        let err = validate_config(&cfg, None).expect_err("validation should fail");
        assert!(err.contains("unknown sensor_id"));
    }
}
