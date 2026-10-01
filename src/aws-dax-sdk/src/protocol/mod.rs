//! Internal DAX wire-protocol components.

pub mod cbor;

#[allow(
    dead_code,
    reason = "Roster accessors are staged for the multi-node routing executor"
)]
pub(crate) mod cluster;

#[allow(
    dead_code,
    reason = "Phase 3 control resolution is validated before its transport integration"
)]
pub mod control;

#[allow(
    dead_code,
    reason = "Phase 3 request codecs are validated before their Phase 5 transport integration"
)]
pub mod request;

#[allow(
    dead_code,
    reason = "Phase 3 schema state is validated before its control-call transport integration"
)]
pub mod schema;

#[allow(
    dead_code,
    reason = "Phase 3 stream framing is validated before asynchronous socket integration"
)]
pub mod stream;

#[allow(
    dead_code,
    reason = "Phase 3 direct TCP control execution is validated before client integration"
)]
pub mod transport;

#[allow(
    dead_code,
    reason = "Phase 3 tube framing is validated before asynchronous transport integration"
)]
pub mod tube;
