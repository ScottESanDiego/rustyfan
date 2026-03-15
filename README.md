# rustyfan

Rust fan controller for Linux `hwmon` that supports arbitrary sensor-to-fan associations.

## Features

- Multiple sensors and multiple fans in one process.
- Weighted aggregation when a fan is linked to more than one sensor.
- One sensor can drive multiple fans.
- Per-fan correlation files (`PWM -> RPM`).
- Validate-only mode to verify config and paths and simulate one control-decision pass without PWM writes.
- Status mode to print resolved paths and normalized association weights.
- Per-fan fail-safe PWM when all linked sensors are unavailable.

## Configuration Model

The config schema is fully association-based (legacy single-field schema is not supported).

- `sensors[]`: temperature inputs and curve ranges.
- `fans[]`: PWM/RPM endpoints, correlation files, and fan limits.
- `associations[]`: links from `sensor_id` to `fan_id` with weights.

See [etc/config.toml.example](etc/config.toml.example).

## Control Policy

For each fan, every associated sensor contributes a target RPM computed from that sensor's temperature curve and the fan's RPM range. The fan's final target RPM is a weighted average of available contributions.

If one associated sensor fails, remaining sensor weights are implicitly renormalized.

If all associated sensors fail for a fan, `fail_safe_pwm` is applied (if configured).

## Common Commands

Validate config and hardware paths without taking control:

```bash
rustyfan --config /etc/rustyfan/config.toml --validate-only
```

Print resolved runtime status (paths and normalized fan weights):

```bash
rustyfan --config /etc/rustyfan/config.toml --status
```

Run control loop:

```bash
rustyfan --config /etc/rustyfan/config.toml
```

Calibrate one fan by id:

```bash
rustyfan --config /etc/rustyfan/config.toml --calibrate-fan cpu_fan
```

Override calibration sweep defaults from CLI:

```bash
rustyfan --config /etc/rustyfan/config.toml \
	--calibrate-fan cpu_fan \
	--min-pwm-cal 20 --max-pwm-cal 255 --pwm-step-cal 5 --calibration-delay-ms 1500
```
