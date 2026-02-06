//! WASM guest transport for calling host-provided capabilities.
//!
//! This module is only compiled for `wasm32` guests.
//!
//! Host contract (0.1): the host provides an import named `lf_cap_call` in the
//! `lattice` import module. Calls are synchronous from the guest's perspective.

use thiserror::Error;

/// Standard-ish errno indicating the output buffer was too small.
///
/// Hosts should return this when `out_cap` is insufficient.
pub const ERRNO_ENOBUFS: i32 = -12;

/// Generic "unsupported" error.
pub const ERRNO_EUNSUPPORTED: i32 = -95;

#[derive(Debug, Error)]
pub enum CapCallError {
    #[error("capability call failed (op={op}, errno={errno})")]
    Host { op: u32, errno: i32 },
    #[error("capability call output exceeded limit ({0} bytes)")]
    OutputTooLarge(usize),
}

#[link(wasm_import_module = "lattice")]
unsafe extern "C" {
    fn lf_cap_call(op: u32, in_ptr: u32, in_len: u32, out_ptr: u32, out_cap: u32) -> i32;
}

const INITIAL_OUT_CAP: usize = 1024;
const MAX_OUT_CAP: usize = 16 * 1024 * 1024;

/// Call into the host capability router.
///
/// `op` is a fixed opcode describing the capability family + operation.
/// `req` is an opaque request payload.
///
/// The host writes the response bytes into the provided output buffer.
pub fn cap_call(op: u32, req: &[u8]) -> Result<Vec<u8>, CapCallError> {
    let mut out_cap = INITIAL_OUT_CAP;
    loop {
        if out_cap > MAX_OUT_CAP {
            return Err(CapCallError::OutputTooLarge(out_cap));
        }

        let mut out = Vec::<u8>::with_capacity(out_cap);
        let rc = unsafe {
            lf_cap_call(
                op,
                req.as_ptr() as u32,
                req.len() as u32,
                out.as_mut_ptr() as u32,
                out_cap as u32,
            )
        };

        if rc == ERRNO_ENOBUFS {
            out_cap = out_cap.saturating_mul(2).max(INITIAL_OUT_CAP);
            continue;
        }

        if rc < 0 {
            return Err(CapCallError::Host { op, errno: rc });
        }

        let len = rc as usize;
        if len > out_cap {
            // Defensive: host must not report a length larger than `out_cap`.
            return Err(CapCallError::Host {
                op,
                errno: ERRNO_ENOBUFS,
            });
        }

        unsafe {
            out.set_len(len);
        }
        return Ok(out);
    }
}
