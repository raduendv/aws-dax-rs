//! Rust-owned UniFFI facade for the AWS DAX SDK.
//!
//! The exported contract is intentionally introduced in reviewed vertical
//! slices. DAX behavior remains implemented by the `aws-dax` crate.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod client;
mod contract;
mod error;
mod operation;
mod paginator;
mod routing;
mod runtime;
mod value;

#[cfg(test)]
mod phase_six_tests;

pub use client::*;
pub use contract::*;
pub use error::*;
pub use paginator::*;

uniffi::setup_scaffolding!();

/// Exercises bounded FFI-to-SDK-to-FFI value conversion for fuzzing.
#[doc(hidden)]
pub fn fuzz_round_trip_attribute_value(
    value: DaxAttributeValue,
) -> Result<DaxAttributeValue, String> {
    value::into_sdk_attribute_value(value)
        .and_then(value::from_sdk_attribute_value)
        .map_err(|error| error.to_string())
}

/// Round-trips one value through the bounded AWS SDK conversion contract.
#[uniffi::export]
#[allow(
    clippy::result_large_err,
    reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
)]
pub fn ffi_round_trip_attribute_value(
    value: DaxAttributeValue,
) -> Result<DaxAttributeValue, DaxFfiError> {
    value::into_sdk_attribute_value(value)
        .and_then(value::from_sdk_attribute_value)
        .map_err(DaxFfiError::from)
}

/// Returns the native FFI contract version.
#[uniffi::export]
pub fn ffi_contract_version() -> String {
    env!("CARGO_PKG_VERSION").to_owned()
}

/// Returns the native FFI contract version asynchronously.
#[uniffi::export]
pub async fn ffi_async_contract_version() -> String {
    env!("CARGO_PKG_VERSION").to_owned()
}

#[cfg(test)]
mod tests {
    #[test]
    fn reports_package_version() {
        assert_eq!(super::ffi_contract_version(), env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn reports_package_version_asynchronously() {
        assert_eq!(
            super::ffi_async_contract_version().await,
            env!("CARGO_PKG_VERSION")
        );
    }
}
