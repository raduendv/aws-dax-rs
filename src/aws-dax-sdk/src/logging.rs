use std::sync::Arc;

/// Severity used by DAX client diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum LogLevel {
    /// Disable client diagnostics.
    #[default]
    Off,
    /// Emit error diagnostics.
    Error,
    /// Emit warning diagnostics.
    Warn,
    /// Emit informational diagnostics.
    Info,
    /// Emit debugging diagnostics.
    Debug,
}

/// Receives redacted DAX client diagnostics.
pub trait Logger: Send + Sync {
    /// Records a DAX diagnostic message.
    fn log(&self, level: LogLevel, message: &str);
}

pub(crate) type SharedLogger = Arc<dyn Logger>;
