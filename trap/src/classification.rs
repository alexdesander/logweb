use color_eyre::eyre::{Context, Result, eyre};
use common::LogLevel;
use regex::Regex;
use std::{fs, path::Path};

pub struct LogClassifier {
    patterns: Vec<(LogLevel, Regex)>,
}

impl LogClassifier {
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
        }
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        Self::from_str(
            &fs::read_to_string(path)
                .wrap_err_with(|| format!("failed to read regex file {}", path.display()))?,
        )
    }

    fn from_str(input: &str) -> Result<Self> {
        let mut patterns = Vec::new();

        for (idx, line) in input.lines().enumerate() {
            let line = line.trim();

            if line.is_empty() {
                continue;
            }

            let (level, pattern) = line
                .split_once('=')
                .ok_or_else(|| eyre!("line {} is missing '='", idx + 1))?;
            let level = parse_level(level.trim())
                .ok_or_else(|| eyre!("line {} has unknown level '{}'", idx + 1, level.trim()))?;
            let pattern = pattern.trim();

            if pattern.is_empty() {
                return Err(eyre!("line {} has an empty regex", idx + 1));
            }

            patterns.push((
                level,
                Regex::new(pattern)
                    .wrap_err_with(|| format!("line {} has an invalid regex", idx + 1))?,
            ));
        }

        Ok(Self { patterns })
    }

    pub fn classify(&self, log: &str) -> LogLevel {
        self.patterns
            .iter()
            .find_map(|(level, regex)| regex.is_match(log).then_some(*level))
            .unwrap_or(LogLevel::Unknown)
    }
}

fn parse_level(level: &str) -> Option<LogLevel> {
    match level.to_lowercase().as_str() {
        "unknown" => Some(LogLevel::Unknown),
        "trace" => Some(LogLevel::Trace),
        "debug" => Some(LogLevel::Debug),
        "info" => Some(LogLevel::Info),
        "warn" => Some(LogLevel::Warn),
        "error" => Some(LogLevel::Error),
        "fatal" => Some(LogLevel::Fatal),
        _ => None,
    }
}
