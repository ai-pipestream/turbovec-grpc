//! Environment parsing shared by both production binaries.

use std::str::FromStr;

pub fn value<T>(name: &str, default: T) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(raw) => raw
            .parse()
            .map_err(|error| format!("invalid {name}={raw:?}: {error}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("cannot read {name}: {error}")),
    }
}

pub fn positive_usize(name: &str, default: usize) -> Result<usize, String> {
    let value = value(name, default)?;
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(value)
}

pub fn enabled(name: &str) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(raw) => match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(format!("{name} must be true or false, got {raw:?}")),
        },
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(format!("cannot read {name}: {error}")),
    }
}
