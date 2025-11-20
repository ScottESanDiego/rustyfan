// src/main.rs
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    cmp,
    collections::HashSet,
    fs::{self, File, ReadDir},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
    process,
};
use directories::ProjectDirs;
use toml;
use ctrlc;
use libc;

// --- Data Structures ---

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CorrelationPoint {
    pwm: u8,
    rpm: u32,
}

// --- Error Rate Limiting ---

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
        
        // Reset counter after 60 seconds
        if elapsed > Duration::from_secs(60) {
            if self.suppressed_count > 0 {
                eprintln!("(Suppressed {} similar errors in the last minute)", self.suppressed_count);
                self.suppressed_count = 0;
            }
            self.error_count = 0;
            self.last_error_time = now;
        }
        
        self.error_count += 1;
        
        // Log first 5 errors, then 1 per minute
        if self.error_count <= 5 || elapsed > Duration::from_secs(60) {
            true
        } else {
            self.suppressed_count += 1;
            false
        }
    }
}

// --- Fan Health Monitoring ---

struct FanHealthMonitor {
    last_pwm_written: Option<u8>,
    consecutive_failures: u32,
    last_rpm_check: Option<u32>,
}

impl FanHealthMonitor {
    fn new() -> Self {
        Self {
            last_pwm_written: None,
            consecutive_failures: 0,
            last_rpm_check: None,
        }
    }
    
    fn check_fan_response(
        &mut self,
        target_pwm: u8,
        current_rpm: Result<u32, io::Error>,
        min_rpm_expected: u32,
    ) -> Result<(), String> {
        self.last_pwm_written = Some(target_pwm);
        
        match current_rpm {
            Ok(rpm) => {
                // If we set a high PWM but fan isn't spinning, that's a problem
                if target_pwm > 50 && rpm < min_rpm_expected / 2 {
                    self.consecutive_failures += 1;
                    if self.consecutive_failures >= 5 {
                        return Err(format!(
                            "Fan health check failed: PWM={} but RPM={} (expected >= {})",
                            target_pwm, rpm, min_rpm_expected / 2
                        ));
                    }
                } else {
                    self.consecutive_failures = 0;
                }
                self.last_rpm_check = Some(rpm);
                Ok(())
            }
            Err(e) => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= 3 {
                    Err(format!("Unable to read fan RPM after 3 attempts: {}", e))
                } else {
                    Ok(())
                }
            }
        }
    }
}

// --- Config File Struct ---
#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    correlation_file: Option<PathBuf>,
    temp_sensor_spec: Option<String>,
    fan_control_spec: Option<String>,
    fan_sensor_spec: Option<String>,
    min_temp: Option<f64>,
    max_temp: Option<f64>,
    min_rpm: Option<u32>,
    max_rpm: Option<u32>,
    interval: Option<u64>,
    // verbose: Option<bool>, // Could add verbose to config file if desired
}

// --- Command Line Arguments --- (Added verbose flag)
#[derive(Parser, Debug)]
#[command(author, version, about = "Controls fan speed based on temperature using RPM correlation.", long_about = None)]
struct CliArgs {
    /// Optional path to a TOML configuration file.
    #[arg(long, short = 'c', value_name = "FILE_PATH")]
    config: Option<PathBuf>,

    /// Run calibration sequence instead of normal control loop.
    #[arg(long)]
    calibrate: bool,

    /// Enable verbose output during control loop operation.
    // ***** ADDED VERBOSE FLAG *****
    #[arg(long, short, action = clap::ArgAction::SetTrue)]
    verbose: bool,

    /// Path to the file for saving/loading PWM<->RPM correlation data (overrides config file).
    #[arg(long, value_name = "FILE_PATH")]
    correlation_file: Option<PathBuf>,

    /// Temperature sensor spec: <device_name>/<base_filename> (overrides config file). Required for control mode.
    #[arg(long = "temp-sensor-spec", value_name = "SPEC")]
    temp_sensor_spec: Option<String>,

    /// Fan PWM control spec: <device_name>/<filename> (overrides config file). Required.
    #[arg(long = "fan-control-spec", value_name = "SPEC")]
    fan_control_spec: Option<String>,

    /// Fan RPM sensor spec: <device_name>/<base_filename> (overrides config file). Required.
    #[arg(long = "fan-sensor-spec", value_name = "SPEC")]
    fan_sensor_spec: Option<String>,

    // --- Control Parameters (overrides config file) ---
    #[arg(long, value_name = "TEMP_C")] min_temp: Option<f64>,
    #[arg(long, value_name = "TEMP_C")] max_temp: Option<f64>,
    #[arg(long, value_name = "RPM")] min_rpm: Option<u32>,
    #[arg(long, value_name = "RPM")] max_rpm: Option<u32>, // If set (!=0), overrides file/calibrated max
    #[arg(long, short = 'i', value_name = "SECONDS")] interval: Option<u64>,

    // --- Calibration Parameters (overrides config file if file support added) ---
    #[arg(long, value_name = "PWM_VAL")] min_pwm_cal: Option<u8>,
    #[arg(long, value_name = "PWM_VAL")] max_pwm_cal: Option<u8>,
    #[arg(long, value_name = "PWM_STEP")] pwm_step_cal: Option<u8>,
    #[arg(long, value_name = "MS")] calibration_delay_ms: Option<u64>,
}

// --- Guard Struct for Restoring Auto Mode ---

struct AutoModeGuard {
    pwm_enable_path: PathBuf,
    original_value: String,
}

impl AutoModeGuard {
    fn new(pwm_enable_path: PathBuf, original_value: String) -> Self {
        AutoModeGuard {
            pwm_enable_path,
            original_value,
        }
    }

    fn restore(&self) {
        println!(
            "Attempting to restore PWM mode to original value ('{}') for {}...",
            self.original_value,
            self.pwm_enable_path.display()
        );

        if let Err(e) = fs::write(&self.pwm_enable_path, &self.original_value) {
            eprintln!(
                "Error: Failed to restore original PWM mode '{}' to {}: {}",
                self.original_value,
                self.pwm_enable_path.display(),
                e
            );
        } else {
            println!("Successfully restored PWM mode to '{}'.", self.original_value);
        }
    }
}

impl Drop for AutoModeGuard {
    fn drop(&mut self) {
        self.restore();
    }
}

// --- Path Finding and Parsing Logic ---

fn parse_spec(spec: &str) -> Result<(&str, &str), String> {
    spec.split_once('/')
        .ok_or_else(|| format!("Invalid spec format: '{}'. Expected '<device_name>/<filename>'.", spec))
}

fn find_hwmon_path_by_name(device_name: &str) -> Result<PathBuf, String> {
    const HWMON_BASE: &str = "/sys/class/hwmon";

    let entries: ReadDir = fs::read_dir(HWMON_BASE)
        .map_err(|e| format!("Failed to read {}: {}", HWMON_BASE, e))?;

    let mut found_path: Option<PathBuf> = None;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Error reading entry in {}: {}", HWMON_BASE, e))?;
        let path = entry.path();

        if path.is_dir() && path.file_name().map_or(false, |name| name.to_string_lossy().starts_with("hwmon")) {
            let name_path = path.join("name");
            if name_path.exists() {
                let name_content = fs::read_to_string(&name_path)
                    .map_err(|e| format!("Failed to read {}: {}", name_path.display(), e))?;

                if name_content.trim() == device_name {
                    if found_path.is_some() {
                        return Err(format!("Multiple hwmon directories found for device name '{}'", device_name));
                    }
                    found_path = Some(path);
                }
            }
        }
    }

    found_path.ok_or_else(|| format!("No hwmon directory found for device name '{}'", device_name))
}

// --- File I/O Functions ---

/// Creates a backup of the correlation file before overwriting
fn create_backup_before_save(path: &Path) -> Result<(), io::Error> {
    if path.exists() {
        let backup_path = path.with_extension("json.backup");
        fs::copy(path, backup_path)?;
    }
    Ok(())
}

/// Saves correlation data to file atomically using a temporary file
fn save_correlation(data: &[CorrelationPoint], path: &Path) -> Result<(), io::Error> {
    // Create backup of existing file if it exists
    if let Err(e) = create_backup_before_save(path) {
        eprintln!("Warning: Could not create backup: {}", e);
        // Continue anyway - backup is nice to have but not critical
    }
    
    // Write to a temporary file first
    let temp_path = path.with_extension("tmp");
    
    // Clean up any stale temp file
    let _ = fs::remove_file(&temp_path);
    
    let file = File::create(&temp_path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JSON serialization error: {}", e)))?;
    
    // Flush and sync to ensure data is written to disk
    writer.flush()?;
    writer.get_mut().sync_all()?;
    drop(writer);
    
    // Atomically rename the temp file to the target file
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

// --- Hardware Interaction Functions ---

fn read_temperature(path: impl AsRef<Path>) -> Result<f64, io::Error> {
    let raw_value_str = fs::read_to_string(path)?;
    match raw_value_str.trim().parse::<i32>() {
        Ok(raw_value) => Ok(raw_value as f64 / 1000.0),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to parse temperature value: {}", e),
        )),
    }
}

fn read_temperature_with_retry(path: impl AsRef<Path>, max_retries: u32) -> Result<f64, io::Error> {
    let path = path.as_ref();
    let mut last_error = None;
    
    for attempt in 0..=max_retries {
        match read_temperature(path) {
            Ok(temp) => {
                // Sanity check: detect obviously invalid readings
                if temp < -100.0 || temp > 200.0 {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Suspicious temperature reading: {:.1}°C", temp)
                    );
                    
                    if attempt < max_retries {
                        eprintln!("Warning: Suspicious temperature reading: {:.1}°C, retrying...", temp);
                        thread::sleep(Duration::from_millis(50));
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
    
    Err(last_error.unwrap())
}

fn write_pwm(path: impl AsRef<Path>, value: u8) -> Result<(), io::Error> {
    fs::write(path, value.to_string())
}

fn write_pwm_with_retry(path: impl AsRef<Path>, value: u8, max_retries: u32) -> Result<(), io::Error> {
    let path = path.as_ref();
    let mut last_error = None;
    
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
    
    Err(last_error.unwrap())
}

fn enable_manual_pwm(path: impl AsRef<Path>) -> Result<(), io::Error> {
    fs::write(path, "1")
}

fn read_fan_rpm(path: impl AsRef<Path>) -> Result<u32, io::Error> {
    let rpm_str = fs::read_to_string(path)?;
    match rpm_str.trim().parse::<u32>() {
        Ok(rpm) => Ok(rpm),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to parse RPM value: {}", e),
        )),
    }
}

// --- Hardware Verification ---

fn verify_hardware_paths_accessible(
    fan_pwm_path: &Path,
    fan_sensor_path: &Path,
    temp_sensor_path: &Path,
) -> Result<(), String> {
    // Check if paths still exist (might disappear on hardware changes)
    if !fan_pwm_path.exists() {
        return Err(format!("Fan PWM path disappeared: {}", fan_pwm_path.display()));
    }
    if !fan_sensor_path.exists() {
        return Err(format!("Fan sensor path disappeared: {}", fan_sensor_path.display()));
    }
    if !temp_sensor_path.exists() {
        return Err(format!("Temperature sensor path disappeared: {}", temp_sensor_path.display()));
    }
    
    // Try to read to ensure they're actually accessible
    fs::read_to_string(temp_sensor_path)
        .map_err(|e| format!("Cannot read temperature sensor: {}", e))?;
    fs::read_to_string(fan_sensor_path)
        .map_err(|e| format!("Cannot read fan sensor: {}", e))?;
    
    Ok(())
}

// --- Validation Functions ---

/// Validates configuration parameters to ensure they are sensible
fn validate_parameters(
    min_temp: f64,
    max_temp: f64,
    min_rpm: u32,
    max_rpm: u32,
    interval: u64,
) -> Result<(), String> {
    if min_temp >= max_temp {
        return Err(format!(
            "Invalid temperature range: min_temp ({:.1}°C) must be less than max_temp ({:.1}°C)",
            min_temp, max_temp
        ));
    }
    
    if min_temp < -50.0 || min_temp > 150.0 {
        return Err(format!("min_temp ({:.1}°C) is outside reasonable range (-50 to 150°C)", min_temp));
    }
    
    if max_temp < -50.0 || max_temp > 150.0 {
        return Err(format!("max_temp ({:.1}°C) is outside reasonable range (-50 to 150°C)", max_temp));
    }
    
    if min_rpm >= max_rpm {
        return Err(format!(
            "Invalid RPM range: min_rpm ({}) must be less than max_rpm ({})",
            min_rpm, max_rpm
        ));
    }
    
    if interval == 0 {
        return Err("Interval must be greater than 0 seconds".to_string());
    }
    
    if interval > 3600 {
        eprintln!("Warning: Interval of {} seconds is very long (> 1 hour)", interval);
    }
    
    Ok(())
}

/// Validates correlation data for consistency and integrity
fn validate_correlation_data(data: &[CorrelationPoint]) -> Result<(), String> {
    if data.is_empty() {
        return Err("Correlation data is empty".to_string());
    }
    
    // Check for duplicate PWM values
    let mut seen_pwms = HashSet::new();
    for point in data {
        if !seen_pwms.insert(point.pwm) {
            return Err(format!("Duplicate PWM value {} found in correlation data", point.pwm));
        }
    }
    
    // Verify data is sorted by PWM
    for i in 1..data.len() {
        if data[i].pwm <= data[i - 1].pwm {
            return Err(format!(
                "Correlation data not properly sorted: PWM {} follows {}",
                data[i].pwm, data[i - 1].pwm
            ));
        }
    }
    
    // Check that there's at least one point with RPM > 0
    if !data.iter().any(|p| p.rpm > 0) {
        return Err("No correlation points with RPM > 0 found".to_string());
    }
    
    Ok(())
}

// --- Calculation Functions ---

fn calculate_target_rpm(
    temp: f64,
    min_temp: f64,
    max_temp: f64,
    min_rpm: u32,
    max_rpm: u32,
) -> u32 {
    if max_rpm <= min_rpm {
        eprintln!(
            "Warning: Effective MAX_RPM ({}) is not greater than MIN_RPM ({}).",
            max_rpm, min_rpm
        );
    }

    if temp <= min_temp {
        return min_rpm;
    }
    if temp >= max_temp {
        return max_rpm;
    }
    if max_temp <= min_temp {
        return max_rpm;
    }

    let temp_range = max_temp - min_temp;
    let rpm_range = max_rpm as f64 - min_rpm as f64;
    let target_rpm_f64 = min_rpm as f64 + (temp - min_temp) * rpm_range / temp_range;

    target_rpm_f64.round().clamp(min_rpm as f64, max_rpm as f64) as u32
}

fn find_pwm_for_rpm(target_rpm: u32, correlation_data: &[CorrelationPoint]) -> Option<u8> {
    let min_spinning_point = correlation_data.iter().find(|p| p.rpm > 0);
    let min_spinning_pwm = match min_spinning_point {
        Some(point) => point.pwm,
        None => return None,
    };

    if target_rpm == 0 {
        return Some(min_spinning_pwm);
    }

    let target_point = correlation_data.iter().find(|point| point.rpm >= target_rpm);
    match target_point {
        Some(point) => {
            if point.rpm > 0 {
                Some(point.pwm)
            } else {
                Some(min_spinning_pwm)
            }
        }
        None => {
            correlation_data
                .iter()
                .filter(|p| p.rpm > 0)
                .max_by_key(|p| p.rpm)
                .map(|p| p.pwm)
        }
    }
}

// --- Mode Functions ---

/// Validates a calibration point for consistency
fn validate_calibration_point(pwm: u8, rpm: u32, previous_points: &[CorrelationPoint]) -> Result<(), String> {
    // Check for obviously bad readings
    if pwm > 0 && rpm == 0 {
        // This might be normal for low PWM values, just note it
        println!("  Note: PWM {} resulted in 0 RPM (fan may not spin at this speed)", pwm);
    }
    
    // Check for decreasing RPM with increasing PWM (should be monotonic or flat)
    if let Some(last_point) = previous_points.last() {
        if pwm > last_point.pwm && rpm > 0 && last_point.rpm > 0 && rpm < last_point.rpm.saturating_sub(200) {
            return Err(format!(
                "Unexpected RPM decrease: PWM {} -> {} but RPM {} -> {} (possible sensor error or unstable fan speed)",
                last_point.pwm, pwm, last_point.rpm, rpm
            ));
        }
    }
    
    Ok(())
}

/// Runs the calibration sequence.
fn run_calibration(
    correlation_file: &Path,
    fan_pwm_path: &Path,
    fan_sensor_path: &Path,
    min_pwm_cal: u8,
    max_pwm_cal: u8,
    pwm_step_cal: u8,
    calibration_delay_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting Fan Calibration ---");
    println!("Using PWM Path: {}", fan_pwm_path.display());
    println!("Using Fan Sensor Path: {}", fan_sensor_path.display());
    println!("Output file: {}", correlation_file.display());
    println!("PWM Range: {} - {} (Step: {})", min_pwm_cal, max_pwm_cal, pwm_step_cal);
    println!("Settle time: {} ms", calibration_delay_ms);

    let mut correlation_data = Vec::new();
    let delay = Duration::from_millis(calibration_delay_ms);

    // Build list of PWM values to test
    let mut pwms_to_test = HashSet::new();
    if min_pwm_cal <= max_pwm_cal {
        pwms_to_test.insert(min_pwm_cal);
        let mut current_pwm = min_pwm_cal;
        
        while current_pwm < max_pwm_cal && current_pwm < 255 {
            let step = if pwm_step_cal == 0 { 1 } else { pwm_step_cal };
            let next_pwm = current_pwm.saturating_add(step);
            
            if next_pwm <= current_pwm {
                break;
            }
            
            current_pwm = cmp::min(next_pwm, max_pwm_cal);
            pwms_to_test.insert(current_pwm);
            
            if current_pwm == 255 {
                break;
            }
        }
        
        pwms_to_test.insert(max_pwm_cal);
    }

    let mut sorted_pwms: Vec<u8> = pwms_to_test.into_iter().collect();
    sorted_pwms.sort_unstable();
    println!("Testing PWM values: {:?}", sorted_pwms);

    // Set initial PWM and wait for stabilization
    write_pwm(fan_pwm_path, min_pwm_cal)?;
    println!("Setting initial PWM to {}...", min_pwm_cal);
    thread::sleep(delay);

    // Test each PWM value
    for pwm_value in sorted_pwms {
        print!("Setting PWM: {} -> ", pwm_value);
        io::stdout().flush()?;
        
        write_pwm(fan_pwm_path, pwm_value)?;
        thread::sleep(delay);
        
        match read_fan_rpm(fan_sensor_path) {
            Ok(rpm) => {
                println!("Measured RPM: {}", rpm);
                
                // Validate calibration point before adding
                match validate_calibration_point(pwm_value, rpm, &correlation_data) {
                    Ok(()) => {
                        correlation_data.push(CorrelationPoint { pwm: pwm_value, rpm });
                    }
                    Err(e) => {
                        eprintln!("Warning: {}", e);
                        eprintln!("Adding point anyway, but calibration data may be unreliable.");
                        correlation_data.push(CorrelationPoint { pwm: pwm_value, rpm });
                    }
                }
            }
            Err(e) => {
                println!("Error reading RPM: {}. Skipping point.", e);
            }
        }
    }

    println!("Calibration finished. Setting PWM to 0 temporarily (auto mode will be restored on exit).");
    if let Err(e) = write_pwm(fan_pwm_path, 0) {
        eprintln!("Warning: Failed to set PWM to 0 after calibration: {}", e);
    }

    correlation_data.sort_unstable();
    println!("Saving correlation data to {}...", correlation_file.display());
    save_correlation(&correlation_data, correlation_file)?;
    println!("Data saved successfully.");
    
    Ok(())
}


/// Runs the main control loop.
fn run_control_loop(
    correlation_data: &[CorrelationPoint],
    temp_sensor_path: &Path,
    fan_pwm_path: &Path,
    fan_sensor_path: &Path,
    min_temp: f64,
    max_temp: f64,
    min_rpm: u32,
    effective_max_rpm: u32,
    interval: u64,
    min_positive_rpm: u32,
    running: Arc<AtomicBool>,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting Control Loop ---");
    println!("Using Temp Path: {}", temp_sensor_path.display());
    println!("Using Fan PWM Path: {}", fan_pwm_path.display());
    println!("Using Fan Sensor Path: {}", fan_sensor_path.display());
    println!(
        "Control Params: Temp Range [{:.1}C - {:.1}C], RPM Range [{} - {}], Interval {}s",
        min_temp, max_temp, min_rpm, effective_max_rpm, interval
    );
    println!("Min positive RPM from calibration: {}", min_positive_rpm);
    println!("Verbose output: {}", verbose);
    println!("Reliability features: Retry logic, error rate limiting, fan health monitoring");
    println!("Press Ctrl+C to exit.");

    // Initialize monitoring and error tracking
    let mut temp_error_limiter = ErrorRateLimiter::new();
    let mut pwm_error_limiter = ErrorRateLimiter::new();
    let mut fan_health = FanHealthMonitor::new();
    let mut last_successful_pwm: Option<u8> = None;
    let mut hardware_check_counter = 0;

    while running.load(Ordering::SeqCst) {
        // Periodically verify hardware is still accessible (every 60 iterations)
        hardware_check_counter += 1;
        if hardware_check_counter >= 60 {
            if let Err(e) = verify_hardware_paths_accessible(
                fan_pwm_path,
                fan_sensor_path,
                temp_sensor_path,
            ) {
                eprintln!("Critical hardware error: {}", e);
                eprintln!("Hardware paths are no longer accessible. Exiting to restore auto control.");
                return Err(e.into());
            }
            hardware_check_counter = 0;
        }

        // Read current temperature with retry logic
        let current_temp = match read_temperature_with_retry(temp_sensor_path, 3) {
            Ok(t) => t,
            Err(e) => {
                if temp_error_limiter.should_log() {
                    eprintln!("Error reading temperature: {}", e);
                }
                
                // If we have a last known good PWM, keep using it
                if let Some(pwm) = last_successful_pwm {
                    if verbose {
                        println!("Temperature read failed, maintaining last known PWM: {}", pwm);
                    }
                    // Re-apply last known PWM to maintain control
                    if let Err(e) = write_pwm_with_retry(fan_pwm_path, pwm, 3) {
                        if pwm_error_limiter.should_log() {
                            eprintln!("Error re-applying PWM {} during temp failure: {}", pwm, e);
                        }
                    }
                } else if verbose {
                    println!("Temperature read failed and no fallback PWM available");
                }
                
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(Duration::from_secs(interval));
                continue;
            }
        };

        // Calculate target RPM based on temperature
        let target_rpm = calculate_target_rpm(
            current_temp,
            min_temp,
            max_temp,
            min_rpm,
            effective_max_rpm,
        );

        // Find and apply appropriate PWM value
        match find_pwm_for_rpm(target_rpm, correlation_data) {
            Some(target_pwm) => {
                // Write PWM with retry logic
                match write_pwm_with_retry(fan_pwm_path, target_pwm, 3) {
                    Ok(()) => {
                        last_successful_pwm = Some(target_pwm);
                        
                        // Check fan health by reading current RPM
                        let rpm_result = read_fan_rpm(fan_sensor_path);
                        
                        // Store RPM value and error status separately for later use
                        let rpm_str = match &rpm_result {
                            Ok(rpm) => {
                                format!("{} RPM", rpm)
                            }
                            Err(_) => "Error".to_string(),
                        };
                        
                        if let Err(e) = fan_health.check_fan_response(
                            target_pwm,
                            rpm_result,
                            min_positive_rpm,
                        ) {
                            eprintln!("Fan health check failed: {}", e);
                            eprintln!("This may indicate a disconnected or failed fan.");
                            eprintln!("Exiting to restore automatic fan control.");
                            return Err(e.into());
                        }
                        
                        if verbose {
                            println!(
                                "Temp: {:.1}°C -> Target: {} RPM -> Actual: {} -> Set PWM: {}",
                                current_temp, target_rpm, rpm_str, target_pwm
                            );
                        }
                    }
                    Err(e) => {
                        if pwm_error_limiter.should_log() {
                            eprintln!(
                                "Error writing PWM value {} to {}: {}",
                                target_pwm,
                                fan_pwm_path.display(),
                                e
                            );
                        }
                        // Continue operating - better to keep trying than to crash
                        // The next iteration will attempt to write again
                    }
                }
            }
            None => {
                eprintln!("Error: Could not find suitable PWM (fan never spun?). Exiting loop.");
                return Err("Fan calibration data invalid (no spinning RPM found)".into());
            }
        }

        // Sleep with periodic checks for exit signal
        let sleep_duration = Duration::from_secs(interval);
        let step = Duration::from_millis(200);
        let start = std::time::Instant::now();
        
        while start.elapsed() < sleep_duration {
            if !running.load(Ordering::SeqCst) {
                println!("Exit signal received, terminating loop...");
                break;
            }
            thread::sleep(step);
        }
    }
    
    println!("Loop terminated.");
    Ok(())
}

// --- Initialization Helper Functions ---

/// Finds and loads the configuration file
fn load_config_file(config_path: Option<PathBuf>) -> FileConfig {
    let mut config_path_to_load = config_path;
    
    if config_path_to_load.is_none() {
        // Check user config directory
        if let Some(proj_dirs) = ProjectDirs::from("com", "YourOrg", "LinuxFanController") {
            let user_config_path = proj_dirs.config_dir().join("config.toml");
            if user_config_path.exists() {
                println!("Using default config file: {}", user_config_path.display());
                config_path_to_load = Some(user_config_path);
            }
        }
        
        // Check current directory
        if config_path_to_load.is_none() {
            let current_dir_path = PathBuf::from("./linux-fan-controller.toml");
            if current_dir_path.exists() {
                println!("Using config file in current dir: {}", current_dir_path.display());
                config_path_to_load = Some(current_dir_path);
            }
        }
    }

    match config_path_to_load {
        Some(path) => {
            println!("Loading configuration from: {}", path.display());
            match fs::read_to_string(&path) {
                Ok(contents) => match toml::from_str(&contents) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        eprintln!("Warning: Failed to parse config file '{}': {}", path.display(), e);
                        FileConfig::default()
                    }
                },
                Err(e) => {
                    eprintln!("Warning: Failed to read config file '{}': {}", path.display(), e);
                    FileConfig::default()
                }
            }
        }
        None => {
            println!("No config file specified or found in default locations.");
            FileConfig::default()
        }
    }
}

/// Resolves hardware paths from device specs
fn resolve_hardware_paths(
    fan_control_spec: &str,
    fan_sensor_spec: &str,
    temp_sensor_spec: Option<&str>,
) -> Result<(PathBuf, PathBuf, PathBuf, Option<PathBuf>), String> {
    println!("Resolving paths from device specs...");

    // Resolve fan PWM control path
    let (fan_dev, fan_pwm_file) = parse_spec(fan_control_spec)?;
    let fan_base_path = find_hwmon_path_by_name(fan_dev)?;
    let fan_pwm_path = fan_base_path.join(fan_pwm_file);
    let fan_pwm_enable_path = fan_base_path.join(format!("{}_enable", fan_pwm_file));

    println!("Using Fan Control Spec: '{}'", fan_control_spec);
    println!("  Resolved PWM Path: {}", fan_pwm_path.display());
    println!("  Derived PWM Enable Path: {}", fan_pwm_enable_path.display());

    // Resolve fan RPM sensor path
    let (fan_sensor_dev, fan_sensor_base_file) = parse_spec(fan_sensor_spec)?;
    let fan_sensor_base_path = find_hwmon_path_by_name(fan_sensor_dev)?;
    let fan_sensor_filename = format!("{}_input", fan_sensor_base_file);
    let fan_sensor_path = fan_sensor_base_path.join(&fan_sensor_filename);

    println!("Using Fan Sensor Spec: '{}' (Assuming suffix '_input')", fan_sensor_spec);
    println!("  Resolved Fan Sensor Path: {}", fan_sensor_path.display());

    // Resolve temperature sensor path (optional for calibration mode)
    let temp_sensor_path = match temp_sensor_spec {
        Some(spec) => {
            let (temp_dev, temp_base_file) = parse_spec(spec)?;
            let temp_base_path = find_hwmon_path_by_name(temp_dev)?;
            let temp_filename = format!("{}_input", temp_base_file);
            let temp_path = temp_base_path.join(&temp_filename);

            println!("Using Temp Sensor Spec: '{}' (Assuming suffix '_input')", spec);
            println!("  Resolved Temp Sensor Path: {}", temp_path.display());
            Some(temp_path)
        }
        None => None,
    };

    Ok((fan_pwm_path, fan_pwm_enable_path, fan_sensor_path, temp_sensor_path))
}


// --- Main Function ---

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup termination flag and Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl-C, signalling termination...");
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // Parse CLI arguments
    let cli_args = CliArgs::parse();

    // Load configuration file
    let file_config = load_config_file(cli_args.config.clone());

    // Merge configurations (CLI > File > Default)
    let fan_control_spec = cli_args
        .fan_control_spec
        .or(file_config.fan_control_spec)
        .ok_or("Fan control spec must be provided")?;
    let fan_sensor_spec = cli_args
        .fan_sensor_spec
        .or(file_config.fan_sensor_spec)
        .ok_or("Fan sensor spec must be provided")?;
    let temp_sensor_spec_opt = cli_args.temp_sensor_spec.or(file_config.temp_sensor_spec);
    let correlation_file = cli_args
        .correlation_file
        .or(file_config.correlation_file)
        .unwrap_or_else(|| PathBuf::from("fan_correlation.json"));
    
    let min_temp = cli_args.min_temp.or(file_config.min_temp).unwrap_or(40.0);
    let max_temp = cli_args.max_temp.or(file_config.max_temp).unwrap_or(80.0);
    let min_rpm = cli_args.min_rpm.or(file_config.min_rpm).unwrap_or(500);
    let max_rpm_arg = cli_args.max_rpm.or(file_config.max_rpm).unwrap_or(0);
    let interval = cli_args.interval.or(file_config.interval).unwrap_or(5);
    let verbose = cli_args.verbose;

    // Validate parameters early (skip max_rpm validation until we know effective value)
    if !cli_args.calibrate {
        if let Err(e) = validate_parameters(min_temp, max_temp, min_rpm, min_rpm + 1, interval) {
            eprintln!("Configuration error: {}", e);
            process::exit(1);
        }
    }

    // Calibration parameters
    let min_pwm_cal = cli_args.min_pwm_cal.unwrap_or(0);
    let max_pwm_cal = cli_args.max_pwm_cal.unwrap_or(255);
    let pwm_step_cal = cli_args.pwm_step_cal.unwrap_or(10);
    let calibration_delay_ms = cli_args.calibration_delay_ms.unwrap_or(2000);

    // Check root privileges
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("Error: Root required.");
        process::exit(1);
    }

    // Resolve hardware paths
    let temp_spec_ref = if !cli_args.calibrate {
        Some(
            temp_sensor_spec_opt
                .as_ref()
                .ok_or("Temp sensor spec required for control mode")?
                .as_str(),
        )
    } else {
        None
    };

    let (fan_pwm_path, fan_pwm_enable_path, fan_sensor_path, temp_sensor_path_opt) =
        resolve_hardware_paths(&fan_control_spec, &fan_sensor_spec, temp_spec_ref)?;

    // Validate that paths exist
    if !fan_pwm_path.exists() {
        return Err(format!("Resolved PWM path does not exist: {}", fan_pwm_path.display()).into());
    }
    if !fan_pwm_enable_path.exists() {
        return Err(format!(
            "Derived PWM enable path does not exist: {}",
            fan_pwm_enable_path.display()
        )
        .into());
    }
    if !fan_sensor_path.exists() {
        return Err(format!(
            "Resolved Fan Sensor path does not exist: {}",
            fan_sensor_path.display()
        )
        .into());
    }
    if let Some(ref temp_path) = temp_sensor_path_opt {
        if !temp_path.exists() {
            return Err(format!(
                "Resolved Temp Sensor path does not exist: {}",
                temp_path.display()
            )
            .into());
        }
    }

    // Take control of PWM and setup cleanup guard
    let original_auto_value = match fs::read_to_string(&fan_pwm_enable_path) {
        Ok(val) => val.trim().to_string(),
        Err(e) => {
            eprintln!(
                "Error reading original value from {}: {}",
                fan_pwm_enable_path.display(),
                e
            );
            process::exit(1);
        }
    };

    println!("Original PWM mode value read as: '{}'", original_auto_value);
    if let Err(e) = enable_manual_pwm(&fan_pwm_enable_path) {
        eprintln!(
            "Error enabling manual PWM control at {}: {}",
            fan_pwm_enable_path.display(),
            e
        );
        process::exit(1);
    }

    println!("Manual PWM control enabled.");
    let _guard = AutoModeGuard::new(fan_pwm_enable_path.clone(), original_auto_value.clone());

    // Execute main logic
    let execution_result = if cli_args.calibrate {
        run_calibration(
            &correlation_file,
            &fan_pwm_path,
            &fan_sensor_path,
            min_pwm_cal,
            max_pwm_cal,
            pwm_step_cal,
            calibration_delay_ms,
        )
    } else {
        let temp_sensor_path = temp_sensor_path_opt
            .ok_or("Internal error: Temp sensor path not resolved for control mode")?;
        
        println!("Loading correlation data from {}...", correlation_file.display());
        match load_correlation(&correlation_file) {
            Ok(data) => {
                // Validate correlation data integrity
                if let Err(e) = validate_correlation_data(&data) {
                    return Err(format!("Invalid correlation data: {}", e).into());
                }

                let min_positive_rpm = data.iter().filter(|p| p.rpm > 0).map(|p| p.rpm).min();
                let calibrated_max_rpm = data.iter().filter(|p| p.rpm > 0).map(|p| p.rpm).max();
                
                match (min_positive_rpm, calibrated_max_rpm) {
                    (Some(min_rpm_val), Some(max_rpm_val)) => {
                        println!(
                            "Correlation data loaded ({} points). Min positive RPM: {}, Max positive RPM: {}",
                            data.len(),
                            min_rpm_val,
                            max_rpm_val
                        );
                        
                        let effective_max_rpm = if max_rpm_arg == 0 {
                            max_rpm_val
                        } else {
                            max_rpm_arg
                        };

                        // Now validate with the effective max RPM
                        if let Err(e) = validate_parameters(
                            min_temp,
                            max_temp,
                            min_rpm,
                            effective_max_rpm,
                            interval,
                        ) {
                            eprintln!("Configuration error: {}", e);
                            process::exit(1);
                        }

                        println!("Effective maximum target RPM used: {}", effective_max_rpm);
                        
                        run_control_loop(
                            &data,
                            &temp_sensor_path,
                            &fan_pwm_path,
                            &fan_sensor_path,
                            min_temp,
                            max_temp,
                            min_rpm,
                            effective_max_rpm,
                            interval,
                            min_rpm_val,
                            running.clone(),
                            verbose,
                        )
                    }
                    _ => Err(format!(
                        "No data points with RPM > 0 found in correlation file: {}",
                        correlation_file.display()
                    )
                    .into()),
                }
            }
            Err(e) => Err(format!(
                "Error loading correlation data from {}: {}",
                correlation_file.display(),
                e
            )
            .into()),
        }
    };

    // Handle result and exit
    if let Err(e) = execution_result {
        eprintln!("An error occurred during execution: {}", e);
        return Err(e);
    }
    
    if !running.load(Ordering::SeqCst) {
        println!("Exited due to signal.");
    } else {
        println!("Program finished normally.");
    }
    
    Ok(())
}
