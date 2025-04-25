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

// --- Data Structures --- (CorrelationPoint remains the same)
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CorrelationPoint { pwm: u8, rpm: u32, }

// --- Config File Struct --- (FileConfig remains the same)
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

// --- Guard Struct for Restoring Auto Mode --- (remains the same)
// ... AutoModeGuard struct and impl ...
struct AutoModeGuard { pwm_enable_path: PathBuf, original_value: String, }
impl AutoModeGuard { fn new(pwm_enable_path: PathBuf, original_value: String) -> Self { AutoModeGuard { pwm_enable_path, original_value } } fn restore(&self) { /* ... implementation unchanged ... */ println!( "Attempting to restore PWM mode to original value ('{}') for {}...", self.original_value, self.pwm_enable_path.display()); if let Err(e) = fs::write(&self.pwm_enable_path, &self.original_value) { eprintln!( "Error: Failed to restore original PWM mode '{}' to {}: {}", self.original_value, self.pwm_enable_path.display(), e ); } else { println!("Successfully restored PWM mode to '{}'.", self.original_value); } } }
impl Drop for AutoModeGuard { fn drop(&mut self) { self.restore(); } }

// --- Path Finding and Parsing Logic --- (remain the same)
// ... parse_spec, find_hwmon_path_by_name functions ...
fn parse_spec(spec: &str) -> Result<(&str, &str), String> { /* ... implementation unchanged ... */ spec.split_once('/').ok_or_else(|| format!("Invalid spec format: '{}'. Expected '<device_name>/<filename>'.", spec)) }
fn find_hwmon_path_by_name(device_name: &str) -> Result<PathBuf, String> { /* ... implementation unchanged ... */ const HWMON_BASE: &str = "/sys/class/hwmon"; let entries: ReadDir = fs::read_dir(HWMON_BASE).map_err(|e| format!("Failed to read {}: {}", HWMON_BASE, e))?; let mut found_path: Option<PathBuf> = None; for entry in entries { let entry = entry.map_err(|e| format!("Error reading entry in {}: {}", HWMON_BASE, e))?; let path = entry.path(); if path.is_dir() && path.file_name().map_or(false, |name| name.to_string_lossy().starts_with("hwmon")) { let name_path = path.join("name"); if name_path.exists() { let name_content = fs::read_to_string(&name_path).map_err(|e| format!("Failed to read {}: {}", name_path.display(), e))?; if name_content.trim() == device_name { if found_path.is_some() { return Err(format!("Multiple hwmon directories found for device name '{}'", device_name)); } found_path = Some(path); } } } } found_path.ok_or_else(|| format!("No hwmon directory found for device name '{}'", device_name)) }

// --- File I/O Functions --- (remain the same)
// ... save_correlation, load_correlation functions ...
fn save_correlation(data: &[CorrelationPoint], path: &Path) -> Result<(), io::Error> { /* ... implementation unchanged ... */ let file = File::create(path)?; let writer = BufWriter::new(file); serde_json::to_writer_pretty(writer, data).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JSON serialization error: {}", e))) }
fn load_correlation(path: &Path) -> Result<Vec<CorrelationPoint>, io::Error> { /* ... implementation unchanged ... */ let file = File::open(path)?; let reader = BufReader::new(file); let mut data: Vec<CorrelationPoint> = serde_json::from_reader(reader).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("JSON parsing error: {}", e)))?; data.sort_unstable(); Ok(data) }

// --- Hardware Interaction Functions --- (remain the same)
// ... read_temperature, write_pwm, enable_manual_pwm, read_fan_rpm functions ...
fn read_temperature(path: impl AsRef<Path>) -> Result<f64, io::Error> { /* ... implementation unchanged ... */ let raw_value_str = fs::read_to_string(path)?; match raw_value_str.trim().parse::<i32>() { Ok(raw_value) => Ok(raw_value as f64 / 1000.0), Err(e) => Err(io::Error::new( io::ErrorKind::InvalidData, format!("Failed to parse temperature value: {}", e), )), } }
fn write_pwm(path: impl AsRef<Path>, value: u8) -> Result<(), io::Error> { fs::write(path, value.to_string()) }
fn enable_manual_pwm(path: impl AsRef<Path>) -> Result<(), io::Error> { fs::write(path, "1") }
fn read_fan_rpm(path: impl AsRef<Path>) -> Result<u32, io::Error> { /* ... implementation unchanged ... */ let rpm_str = fs::read_to_string(path)?; match rpm_str.trim().parse::<u32>() { Ok(rpm) => Ok(rpm), Err(e) => Err(io::Error::new( io::ErrorKind::InvalidData, format!("Failed to parse RPM value: {}", e), )), } }

// --- Calculation Functions --- (remain the same)
// ... calculate_target_rpm, find_pwm_for_rpm functions ...
fn calculate_target_rpm(temp: f64, min_temp: f64, max_temp: f64, min_rpm: u32, max_rpm: u32) -> u32 { /* ... implementation unchanged ... */ if max_rpm <= min_rpm { eprintln!("Warning: Effective MAX_RPM ({}) is not greater than MIN_RPM ({}).", max_rpm, min_rpm); } if temp <= min_temp { return min_rpm; } if temp >= max_temp { return max_rpm; } if max_temp <= min_temp { return max_rpm; } let temp_range = max_temp - min_temp; let rpm_range = max_rpm as f64 - min_rpm as f64; let target_rpm_f64 = min_rpm as f64 + (temp - min_temp) * rpm_range / temp_range; target_rpm_f64.round().clamp(min_rpm as f64, max_rpm as f64) as u32 }
fn find_pwm_for_rpm(target_rpm: u32, correlation_data: &[CorrelationPoint]) -> Option<u8> { /* ... implementation unchanged ... */ let min_spinning_point = correlation_data.iter().find(|p| p.rpm > 0); let min_spinning_pwm = match min_spinning_point { Some(point) => point.pwm, None => return None, }; if target_rpm == 0 { return Some(min_spinning_pwm); } let target_point = correlation_data.iter().find(|point| point.rpm >= target_rpm); match target_point { Some(point) => { if point.rpm > 0 { Some(point.pwm) } else { Some(min_spinning_pwm) } } None => { correlation_data.iter().filter(|p| p.rpm > 0).max_by_key(|p| p.rpm).map(|p| p.pwm) } } }

// --- Mode Functions ---

/// Runs the calibration sequence.
// (Signature/Body remains the same)
fn run_calibration( correlation_file: &Path, fan_pwm_path: &Path, fan_sensor_path: &Path, min_pwm_cal: u8, max_pwm_cal: u8, pwm_step_cal: u8, calibration_delay_ms: u64, ) -> Result<(), Box<dyn std::error::Error>> { /* ... implementation unchanged ... */
    println!("--- Starting Fan Calibration ---"); println!("Using PWM Path: {}", fan_pwm_path.display()); println!("Using Fan Sensor Path: {}", fan_sensor_path.display()); println!("Output file: {}", correlation_file.display()); println!("PWM Range: {} - {} (Step: {})", min_pwm_cal, max_pwm_cal, pwm_step_cal); println!("Settle time: {} ms", calibration_delay_ms);
    let mut correlation_data = Vec::new(); let delay = Duration::from_millis(calibration_delay_ms);
    let mut pwms_to_test = HashSet::new(); if min_pwm_cal <= max_pwm_cal { pwms_to_test.insert(min_pwm_cal); let mut current_pwm = min_pwm_cal; while current_pwm < max_pwm_cal && current_pwm < 255 { let step = if pwm_step_cal == 0 { 1 } else { pwm_step_cal }; let next_pwm = current_pwm.saturating_add(step); if next_pwm <= current_pwm { break; } current_pwm = cmp::min(next_pwm, max_pwm_cal); pwms_to_test.insert(current_pwm); if current_pwm == 255 { break; } } pwms_to_test.insert(max_pwm_cal); }
    let mut sorted_pwms: Vec<u8> = pwms_to_test.into_iter().collect(); sorted_pwms.sort_unstable(); println!("Testing PWM values: {:?}", sorted_pwms);
    write_pwm(fan_pwm_path, min_pwm_cal)?; println!("Setting initial PWM to {}...", min_pwm_cal); thread::sleep(delay);
    for pwm_value in sorted_pwms { print!("Setting PWM: {} -> ", pwm_value); io::stdout().flush()?; write_pwm(fan_pwm_path, pwm_value)?; thread::sleep(delay); match read_fan_rpm(fan_sensor_path) { Ok(rpm) => { println!("Measured RPM: {}", rpm); correlation_data.push(CorrelationPoint { pwm: pwm_value, rpm }); } Err(e) => { println!("Error reading RPM: {}. Skipping point.", e); } } }
    println!("Calibration finished. Setting PWM to 0 temporarily (auto mode will be restored on exit)."); if let Err(e) = write_pwm(fan_pwm_path, 0) { eprintln!("Warning: Failed to set PWM to 0 after calibration: {}", e); }
    correlation_data.sort_unstable(); println!("Saving correlation data to {}...", correlation_file.display()); save_correlation(&correlation_data, correlation_file)?; println!("Data saved successfully."); Ok(())
}


/// Runs the main control loop. (Added verbose parameter)
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
    verbose: bool, // ***** ADDED VERBOSE PARAMETER *****
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting Control Loop ---");
    println!("Using Temp Path: {}", temp_sensor_path.display());
    println!("Using Fan PWM Path: {}", fan_pwm_path.display());
    println!("Using Fan Sensor Path: {}", fan_sensor_path.display());
    println!("Control Params: Temp Range [{:.1}C - {:.1}C], RPM Range [{} - {}], Interval {}s", min_temp, max_temp, min_rpm, effective_max_rpm, interval);
    println!("Min positive RPM from calibration: {}", min_positive_rpm);
    println!("Verbose output: {}", verbose); // Indicate verbosity level
    println!("Press Ctrl+C to exit.", );

    while running.load(Ordering::SeqCst) {
        let current_temp = match read_temperature(temp_sensor_path) { Ok(t) => t, Err(e) => { eprintln!("Error reading temperature: {}", e); if !running.load(Ordering::SeqCst) { break; } thread::sleep(Duration::from_secs(interval)); continue; } };
        let target_rpm = calculate_target_rpm(current_temp, min_temp, max_temp, min_rpm, effective_max_rpm);
        match find_pwm_for_rpm(target_rpm, correlation_data) { Some(target_pwm) => { if let Err(e) = write_pwm(fan_pwm_path, target_pwm) { eprintln!("Error writing PWM value {} to {}: {}", target_pwm, fan_pwm_path.display(), e); } else {
            // ***** CONDITIONALLY PRINT STATUS *****
            if verbose {
                let rpm_str = match read_fan_rpm(fan_sensor_path) { Ok(rpm) => format!("{} RPM", rpm), Err(_) => "Error".to_string(), };
                println!( "Temp: {:.1}°C -> Target: {} RPM -> Actual: {} -> Set PWM: {}", current_temp, target_rpm, rpm_str, target_pwm );
            }
        } } None => { eprintln!("Error: Could not find suitable PWM (fan never spun?). Exiting loop."); return Err("Fan calibration data invalid (no spinning RPM found)".into()); } }
        let sleep_duration = Duration::from_secs(interval); let step = Duration::from_millis(200); let start = std::time::Instant::now(); while start.elapsed() < sleep_duration { if !running.load(Ordering::SeqCst) { println!("Exit signal received, terminating loop..."); break; } thread::sleep(step); }
    }
    println!("Loop terminated.");
    Ok(())
}


// --- Main Function --- (Pass verbose flag)

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup termination flag and Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || { println!("\nReceived Ctrl-C, signalling termination..."); r.store(false, Ordering::SeqCst); })
        .expect("Error setting Ctrl-C handler");

    // Parse CLI arguments first
    let cli_args = CliArgs::parse();

    // --- Load Configuration File ---
    // ... (config file loading logic unchanged) ...
    let mut config_path_to_load: Option<PathBuf> = cli_args.config.clone(); if config_path_to_load.is_none() { if let Some(proj_dirs) = ProjectDirs::from("com", "YourOrg", "LinuxFanController") { let user_config_path = proj_dirs.config_dir().join("config.toml"); if user_config_path.exists() { println!("Using default config file: {}", user_config_path.display()); config_path_to_load = Some(user_config_path); } } if config_path_to_load.is_none() { let current_dir_path = PathBuf::from("./linux-fan-controller.toml"); if current_dir_path.exists() { println!("Using config file in current dir: {}", current_dir_path.display()); config_path_to_load = Some(current_dir_path); } } }
    let file_config: FileConfig = match config_path_to_load { Some(path) => { println!("Loading configuration from: {}", path.display()); match fs::read_to_string(&path) { Ok(contents) => match toml::from_str(&contents) { Ok(cfg) => cfg, Err(e) => { eprintln!("Warning: Failed to parse config file '{}': {}", path.display(), e); FileConfig::default() } }, Err(e) => { eprintln!("Warning: Failed to read config file '{}': {}", path.display(), e); FileConfig::default() } } } None => { println!("No config file specified or found in default locations."); FileConfig::default() } };


    // --- Merge Configurations (CLI > File > Default Literals) ---
    // ... (merging logic for specs and numeric values unchanged) ...
    let fan_control_spec = cli_args.fan_control_spec.or(file_config.fan_control_spec).ok_or("Fan control spec must be provided")?;
    let fan_sensor_spec = cli_args.fan_sensor_spec.or(file_config.fan_sensor_spec).ok_or("Fan sensor spec must be provided")?;
    let temp_sensor_spec_opt = cli_args.temp_sensor_spec.or(file_config.temp_sensor_spec);
    let correlation_file = cli_args.correlation_file.or(file_config.correlation_file).unwrap_or_else(|| PathBuf::from("fan_correlation.json"));
    let min_temp = cli_args.min_temp.or(file_config.min_temp).unwrap_or(40.0);
    let max_temp = cli_args.max_temp.or(file_config.max_temp).unwrap_or(80.0);
    let min_rpm = cli_args.min_rpm.or(file_config.min_rpm).unwrap_or(500);
    let max_rpm_arg = cli_args.max_rpm.or(file_config.max_rpm).unwrap_or(0); // Use 0 sentinel
    let interval = cli_args.interval.or(file_config.interval).unwrap_or(5);
    let min_pwm_cal = cli_args.min_pwm_cal.unwrap_or(0);
    let max_pwm_cal = cli_args.max_pwm_cal.unwrap_or(255);
    let pwm_step_cal = cli_args.pwm_step_cal.unwrap_or(10);
    let calibration_delay_ms = cli_args.calibration_delay_ms.unwrap_or(2000);
    // Merge verbose flag - CLI takes precedence, defaults to false
    // Could add file_config.verbose if desired: cli_args.verbose || file_config.verbose.unwrap_or(false)
    let verbose = cli_args.verbose;


    // --- Initial checks (root, paths) ---
    if unsafe { libc::geteuid() } != 0 { eprintln!("Error: Root required."); process::exit(1); }

    // --- Resolve Paths using MERGED specs ---
    // ... (path resolving logic unchanged) ...
    println!("Resolving paths from final specs...");
    let (fan_dev, fan_pwm_file) = parse_spec(&fan_control_spec)?; let fan_base_path = find_hwmon_path_by_name(fan_dev)?; let fan_pwm_path = fan_base_path.join(fan_pwm_file); let fan_pwm_enable_path = fan_base_path.join(format!("{}_enable", fan_pwm_file));
    println!("Using Fan Control Spec: '{}'", fan_control_spec); println!("  Resolved PWM Path: {}", fan_pwm_path.display()); println!("  Derived PWM Enable Path: {}", fan_pwm_enable_path.display());
    let (fan_sensor_dev, fan_sensor_base_file) = parse_spec(&fan_sensor_spec)?; let fan_sensor_base_path = find_hwmon_path_by_name(fan_sensor_dev)?; let fan_sensor_filename = format!("{}_input", fan_sensor_base_file); let fan_sensor_path = fan_sensor_base_path.join(&fan_sensor_filename);
    println!("Using Fan Sensor Spec: '{}' (Assuming suffix '_input')", fan_sensor_spec); println!("  Resolved Fan Sensor Path: {}", fan_sensor_path.display());
    let temp_sensor_path_opt = if !cli_args.calibrate { let spec = temp_sensor_spec_opt.as_ref().ok_or("Temp sensor spec required for control mode")?; let (temp_dev, temp_base_file) = parse_spec(spec)?; let temp_base_path = find_hwmon_path_by_name(temp_dev)?; let temp_filename = format!("{}_input", temp_base_file); let temp_path = temp_base_path.join(&temp_filename); println!("Using Temp Sensor Spec: '{}' (Assuming suffix '_input')", spec); println!("  Resolved Temp Sensor Path: {}", temp_path.display()); Some(temp_path) } else { None };
    if !fan_pwm_path.exists() { return Err(format!("Resolved PWM path does not exist: {}", fan_pwm_path.display()).into()); } if !fan_pwm_enable_path.exists() { return Err(format!("Derived PWM enable path does not exist: {}", fan_pwm_enable_path.display()).into()); } if !fan_sensor_path.exists() { return Err(format!("Resolved Fan Sensor path does not exist: {}", fan_sensor_path.display()).into()); } if let Some(ref temp_path) = temp_sensor_path_opt { if !temp_path.exists() { return Err(format!("Resolved Temp Sensor path does not exist: {}", temp_path.display()).into()); } }


    // --- Take Control and Setup Cleanup Guard ---
    // ... (logic unchanged) ...
    let original_auto_value = match fs::read_to_string(&fan_pwm_enable_path) { Ok(val) => val.trim().to_string(), Err(e) => { eprintln!("Error reading original value from {}: {}", fan_pwm_enable_path.display(), e); process::exit(1); } };
    println!("Original PWM mode value read as: '{}'", original_auto_value); if let Err(e) = enable_manual_pwm(&fan_pwm_enable_path) { eprintln!("Error enabling manual PWM control at {}: {}", fan_pwm_enable_path.display(), e); process::exit(1); }
    println!("Manual PWM control enabled."); let _guard = AutoModeGuard::new(fan_pwm_enable_path.clone(), original_auto_value.clone());


    // --- Execute Main Logic ---
    let execution_result = {
        if cli_args.calibrate { // Use cli_args.calibrate flag
            run_calibration(&correlation_file, &fan_pwm_path, &fan_sensor_path, min_pwm_cal, max_pwm_cal, pwm_step_cal, calibration_delay_ms)
        } else {
            let temp_sensor_path = temp_sensor_path_opt.ok_or("Internal error: Temp sensor path not resolved for control mode")?;
            println!("Loading correlation data from {}...", correlation_file.display());
            match load_correlation(&correlation_file) {
                Ok(data) => {
                    if data.is_empty() { Err(format!("Correlation data file is empty: {}", correlation_file.display()).into()) }
                    else {
                        let min_positive_rpm = data.iter().filter(|p| p.rpm > 0).map(|p| p.rpm).min();
                        let calibrated_max_rpm = data.iter().filter(|p| p.rpm > 0).map(|p| p.rpm).max();
                        match (min_positive_rpm, calibrated_max_rpm) {
                            (Some(min_rpm_val), Some(max_rpm_val)) => {
                                println!("Correlation data loaded ({} points). Min positive RPM: {}, Max positive RPM: {}", data.len(), min_rpm_val, max_rpm_val);
                                let effective_max_rpm = if max_rpm_arg == 0 { max_rpm_val } else { max_rpm_arg };
                                if effective_max_rpm < min_rpm { eprintln!("Warning: Effective maximum target RPM ({}) is less than minimum target RPM ({}).", effective_max_rpm, min_rpm); }
                                println!("Effective maximum target RPM used: {}", effective_max_rpm);
                                // ***** PASS VERBOSE FLAG *****
                                run_control_loop( &data, &temp_sensor_path, &fan_pwm_path, &fan_sensor_path, min_temp, max_temp, min_rpm, effective_max_rpm, interval, min_rpm_val, running.clone(), verbose )
                            }
                            _ => { Err(format!("No data points with RPM > 0 found in correlation file: {}", correlation_file.display()).into()) }
                        }
                    }
                }
                Err(e) => { Err(format!("Error loading correlation data from {}: {}", correlation_file.display(), e).into()) }
            }
        }
    }; // End of execution block

    // --- Handle Result and Exit ---
    if let Err(e) = execution_result { eprintln!("An error occurred during execution: {}", e); return Err(e); }
    if !running.load(Ordering::SeqCst) { println!("Exited due to signal."); } else { println!("Program finished normally."); }
    Ok(())
}
