//! Cross-mirror exhaustiveness guard for `OutboundAuthKind` (spec §7, F8).
//!
//! `OutboundAuthKind` is mirrored — by hand — in FIVE places that must all gain
//! an arm whenever a variant is added, or auth silently breaks (most
//! dangerously on wasm hosts, where the failure is a runtime deserialize error
//! rather than a compile error):
//!
//!   1. apply-side: `connectors-std/src/auth.rs::apply_static_outbound_auth`
//!      (the native/dev application of the credential to the request).
//!   2. wasm transport mirror: `capabilities/src/connector.rs`
//!      (`TransportOutboundAuthKind` + `From<OutboundAuthKind>`), plus the
//!      `kind_name`/`handle_kind` accessors on `OutboundAuthKind` itself.
//!   3. host-wasmtime mirror: `host-wasmtime/src/lib.rs`
//!      (`TransportOutboundAuthKind` + `transport_auth_profile_to_descriptor`).
//!   4. CLI lock apply: `cli/src/main.rs::apply_static_auth_to_request`.
//!   5. CLI handle validation: `cli/src/main.rs::validate_connector_handle_provider_config`
//!      (`auth.static_basic` → `http.basic`).
//!
//! Every one of those sites is written as an exhaustive `match` with NO
//! wildcard arm, so adding a variant fails their crates' compiles. This test is
//! the belt-and-braces canary: the exhaustive match below fails to compile the
//! moment a variant is added without a conscious edit here, pointing the author
//! at the mirror checklist above.

use capabilities::connector::OutboundAuthKind;

/// Exhaustively name every variant. Adding a variant to `OutboundAuthKind`
/// forces an edit here (compile error), which is the prompt to update all five
/// mirror sites documented above.
fn variant_tag(kind: OutboundAuthKind) -> &'static str {
    match kind {
        OutboundAuthKind::Bearer { .. } => "bearer",
        OutboundAuthKind::ApiKeyHeader { .. } => "api_key_header",
        OutboundAuthKind::ApiKeyQuery { .. } => "api_key_query",
        OutboundAuthKind::Basic { .. } => "basic",
        OutboundAuthKind::Unsupported { .. } => "unsupported",
    }
}

#[test]
fn every_outbound_auth_kind_variant_is_accounted_for() {
    // The set of variants this build knows about. If a variant is added, the
    // `variant_tag` match above fails to compile; if one is removed, this list
    // must shrink. Either way the change is deliberate and reviewable.
    let samples = [
        OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
        OutboundAuthKind::ApiKeyHeader {
            header_name: "X-Api-Key",
            prefix: None,
            handle_kind: "http.api_key",
        },
        OutboundAuthKind::ApiKeyQuery {
            query_name: "api_key",
            handle_kind: "http.api_key",
        },
        OutboundAuthKind::Basic {
            handle_kind: "http.basic",
        },
        OutboundAuthKind::Unsupported {
            kind_name: "future_kind",
            handle_kind: "future.handle",
        },
    ];

    // The accessor mirrors on `OutboundAuthKind` itself must agree with the
    // variant tags (this exercises `kind_name`/`handle_kind`, mirror site 2).
    for kind in samples {
        let tag = variant_tag(kind);
        if tag != "unsupported" {
            assert_eq!(kind.kind_name(), tag, "kind_name drift for `{tag}`");
        }
        assert!(!kind.handle_kind().is_empty());
    }

    // Basic specifically must round-trip its canonical wire identity.
    let basic = OutboundAuthKind::Basic {
        handle_kind: "http.basic",
    };
    assert_eq!(basic.kind_name(), "basic");
    assert_eq!(basic.handle_kind(), "http.basic");
}
