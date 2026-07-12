//! The byte-plane types for the `connector.http` v1.1 binary/artifact
//! contract (`impl-docs/spec/http-request-node.md` §16).
//!
//! H5a shipped the byte-plane types: `Handle<S>`, `Artifact<S>`, `ByteSource`,
//! the guest-side macaroon attenuation (`narrow`/`file`), and host-side
//! macaroon mint/verify. H5b adds the capability-narrowed node-facing views
//! `WorkspaceRead` / `WorkspaceWrite` (handle-only surface enforcing §16.4
//! gate 2 = `Macaroon::verify_tag` and gate 3 = `Macaroon::caveats_permit`).
//! The grant/accessor split lives in `scoped.rs`; per-opcode host enforcement
//! in `host-wasmtime` (see `impl-docs/spec/http-request-node.md` §16.4/§16.8).

use hmac::{Hmac, Mac};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::workspace::{Workspace, WorkspaceError, WorkspaceReadResult, normalize_path};

type HmacSha256 = Hmac<Sha256>;

// ─────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("invalid handle path: {0}")]
    InvalidPath(#[from] WorkspaceError),
    #[error("attenuation would widen scope: {current:?} -> {requested:?}")]
    WideningRejected {
        current: HandleScope,
        requested: HandleScope,
    },
}

// ─────────────────────────────────────────────────────────────────────────
// Scope (sealed marker trait) + runtime witness
// ─────────────────────────────────────────────────────────────────────────

mod sealed {
    pub trait Sealed {}
}

/// Sealed marker trait — `Exact` and `Prefix` are the only implementations.
/// External crates cannot add a third granularity.
pub trait Scope: sealed::Sealed + Send + Sync + 'static {
    /// Name used in mismatch error messages.
    const NAME: &'static str;

    /// Does the runtime witness match this compile-time scope?
    fn matches(scope: &HandleScope) -> bool;
}

/// A single-file scope: `HandleScope::Exact(path)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Exact;

/// A subtree scope: `HandleScope::Prefix(path)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Prefix;

impl sealed::Sealed for Exact {}
impl sealed::Sealed for Prefix {}

impl Scope for Exact {
    const NAME: &'static str = "exact";

    fn matches(scope: &HandleScope) -> bool {
        matches!(scope, HandleScope::Exact(_))
    }
}

impl Scope for Prefix {
    const NAME: &'static str = "prefix";

    fn matches(scope: &HandleScope) -> bool {
        matches!(scope, HandleScope::Prefix(_))
    }
}

/// Runtime witness carried inside every `Handle`, checked against the
/// compile-time `S` on deserialize (§16.2 — "type-level scope, enforced at
/// the port").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandleScope {
    Exact(String),
    Prefix(String),
}

impl HandleScope {
    pub fn path(&self) -> &str {
        match self {
            HandleScope::Exact(p) => p,
            HandleScope::Prefix(p) => p,
        }
    }

    /// Does `self` (as a scope bound) contain `target`? Used both for
    /// narrow-only attenuation checks and macaroon caveat satisfaction.
    fn contains(&self, target: &HandleScope) -> bool {
        match self {
            HandleScope::Exact(bound) => matches!(target, HandleScope::Exact(t) if t == bound),
            HandleScope::Prefix(bound) => is_subpath(bound, target.path()),
        }
    }
}

fn is_subpath(prefix: &str, candidate: &str) -> bool {
    let trimmed = prefix.trim_end_matches('/');
    if trimmed.is_empty() {
        // Prefix("") is the whole-store root — contains everything.
        return true;
    }
    candidate == trimmed || candidate.starts_with(&format!("{trimmed}/"))
}

/// Join a subtree scope with a guest-supplied sub-path/leaf, normalizing the
/// result. Uses `workspace::normalize_path` for the leaf/sub component so
/// traversal is rejected the same way workspace paths are.
fn join_scope(base: &str, sub: &str) -> Result<String, ArtifactError> {
    let normalized_sub = crate::workspace::normalize_path(sub)?;
    if base.trim_end_matches('/').is_empty() {
        return Ok(normalized_sub);
    }
    Ok(format!("{}/{}", base.trim_end_matches('/'), normalized_sub))
}

// ─────────────────────────────────────────────────────────────────────────
// StoreRef — binding name of the backing Workspace/BlobStore
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreRef(pub String);

impl StoreRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for StoreRef {
    fn from(value: &str) -> Self {
        StoreRef::new(value)
    }
}

impl From<String> for StoreRef {
    fn from(value: String) -> Self {
        StoreRef(value)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Macaroon (§16.2 — narrow-only guest-side attenuation, host-side verify)
// ─────────────────────────────────────────────────────────────────────────

/// A single scope-bound caveat in a macaroon's caveat chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Caveat {
    pub scope: HandleScope,
}

impl Caveat {
    /// Deterministic byte encoding folded into the HMAC chain. Distinct
    /// prefixes for Exact/Prefix so `Exact("a")` and `Prefix("a")` caveats
    /// never collide.
    fn encode(&self) -> Vec<u8> {
        match &self.scope {
            HandleScope::Exact(p) => format!("exact:{p}").into_bytes(),
            HandleScope::Prefix(p) => format!("prefix:{p}").into_bytes(),
        }
    }
}

/// `mint = { root_key_id, caveats, tag }`. `tag` starts as
/// `HMAC(root_key, root_key_id)` and each narrowing appends a caveat and
/// extends the tag `tag' = HMAC(tag, caveat)` (§16.2, review F3 — the
/// macaroon composition, not a naive HMAC-of-parent-mint chain, because the
/// latter does not compose across narrowing split points).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Macaroon {
    pub root_key_id: String,
    pub caveats: Vec<Caveat>,
    pub tag: Vec<u8>,
}

fn extend_tag(tag: &[u8], caveat: &Caveat) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(tag).expect("HMAC accepts any key length");
    mac.update(&caveat.encode());
    mac.finalize().into_bytes().to_vec()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl Macaroon {
    /// Host-side: mint a fresh macaroon rooted at `root_key`, bound to an
    /// initial scope (typically the whole store, `Prefix("")`, or a single
    /// staged file, `Exact(path)`). The root key never leaves the host.
    pub fn mint(
        root_key: &[u8],
        root_key_id: impl Into<String>,
        initial_scope: HandleScope,
    ) -> Self {
        let root_key_id = root_key_id.into();
        let mut mac = HmacSha256::new_from_slice(root_key).expect("HMAC accepts any key length");
        mac.update(root_key_id.as_bytes());
        let root_tag = mac.finalize().into_bytes().to_vec();

        let caveat = Caveat {
            scope: initial_scope,
        };
        let tag = extend_tag(&root_tag, &caveat);
        Macaroon {
            root_key_id,
            caveats: vec![caveat],
            tag,
        }
    }

    /// Guest-side: append a narrowing caveat and extend the tag. No root key
    /// needed — this is the property macaroons exist for.
    fn extend(&self, new_scope: HandleScope) -> Self {
        let caveat = Caveat { scope: new_scope };
        let tag = extend_tag(&self.tag, &caveat);
        let mut caveats = self.caveats.clone();
        caveats.push(caveat);
        Macaroon {
            root_key_id: self.root_key_id.clone(),
            caveats,
            tag,
        }
    }

    /// Host-side gate 2 (§16.4): recompute the tag by folding the caveat chain
    /// from the root key and constant-time compare against the carried tag.
    /// A forged or widened mint (whose caveat chain was tampered) fails here.
    /// This is deliberately split from the scope-containment check
    /// ([`caveats_permit`]) so the two deref gates are independently
    /// enforceable and independently testable (§16.4 "three deref gates").
    pub fn verify_tag(&self, root_key: &[u8]) -> bool {
        let mut mac = HmacSha256::new_from_slice(root_key).expect("HMAC accepts any key length");
        mac.update(self.root_key_id.as_bytes());
        let mut tag = mac.finalize().into_bytes().to_vec();
        for caveat in &self.caveats {
            tag = extend_tag(&tag, caveat);
        }
        constant_time_eq(&tag, &self.tag)
    }

    /// Host-side gate 3 (§16.4): does `target` satisfy **every** caveat in the
    /// chain? A handle whose runtime `scope` witness was widened past what its
    /// (validly-tagged) caveats permit fails here even though [`verify_tag`]
    /// passes.
    pub fn caveats_permit(&self, target: &HandleScope) -> bool {
        self.caveats.iter().all(|c| c.scope.contains(target))
    }

    /// Host-side: recompute the tag by folding the caveat chain from the
    /// root key, constant-time compare against the carried tag, and check
    /// `final_scope` satisfies **every** caveat (§16.4 gates 2+3 combined).
    pub fn verify(&self, root_key: &[u8], final_scope: &HandleScope) -> bool {
        self.verify_tag(root_key) && self.caveats_permit(final_scope)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Handle<S>
// ─────────────────────────────────────────────────────────────────────────

/// A capability to reach bytes. Attenuable (narrow-only). Minted host-side.
/// `S` is a phantom scope-granularity marker; `scope` is the runtime witness
/// checked against `S` at deserialize time (§16.2).
///
/// Constructors are intentionally narrow: guest code can only reach a
/// `Handle` via `narrow`/`file` on an existing `Handle<Prefix>`, or receive
/// one host-minted (`Handle::host_mint_prefix` / `Handle::host_mint_exact`).
/// Fields are private.
#[derive(Debug, Serialize)]
pub struct Handle<S: Scope = Exact> {
    store: StoreRef,
    scope: HandleScope,
    mint: Macaroon,
    #[serde(skip)]
    _s: PhantomData<S>,
}

// Manual Clone: PhantomData<S> is Clone regardless of S, so don't force
// `S: Clone` via derive's default bound inference.
impl<S: Scope> Clone for Handle<S> {
    fn clone(&self) -> Self {
        Handle {
            store: self.store.clone(),
            scope: self.scope.clone(),
            mint: self.mint.clone(),
            _s: PhantomData,
        }
    }
}

impl<S: Scope> PartialEq for Handle<S> {
    fn eq(&self, other: &Self) -> bool {
        self.store == other.store && self.scope == other.scope && self.mint == other.mint
    }
}
impl<S: Scope> Eq for Handle<S> {}

/// Wire shape used only to drive the validating `Deserialize` below — the
/// phantom has no wire presence (transparent), matching `Serialize`.
#[derive(Deserialize)]
struct HandleWire {
    store: StoreRef,
    scope: HandleScope,
    mint: Macaroon,
}

impl<'de, S: Scope> Deserialize<'de> for Handle<S> {
    /// Fail-closed validation (review F9): a `Handle<Exact>` MUST refuse a
    /// wire value whose `scope` is `Prefix(..)`, and vice versa. This is
    /// what makes "authority granularity is a compile-time property of the
    /// signature" true rather than asserted (§16.2).
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = HandleWire::deserialize(deserializer)?;
        if !S::matches(&wire.scope) {
            return Err(serde::de::Error::custom(format!(
                "handle scope mismatch: port expects {}, wire carried {:?}",
                S::NAME,
                wire.scope
            )));
        }
        Ok(Handle {
            store: wire.store,
            scope: wire.scope,
            mint: wire.mint,
            _s: PhantomData,
        })
    }
}

impl Handle<Prefix> {
    /// Host-side: mint a fresh whole-store or subtree handle. Never called
    /// from guest code (no root key available there).
    pub fn host_mint_prefix(
        store: StoreRef,
        root_key: &[u8],
        root_key_id: impl Into<String>,
        scope_path: impl Into<String>,
    ) -> Self {
        let scope = HandleScope::Prefix(scope_path.into());
        let mint = Macaroon::mint(root_key, root_key_id, scope.clone());
        Handle {
            store,
            scope,
            mint,
            _s: PhantomData,
        }
    }

    fn current_prefix(&self) -> &str {
        match &self.scope {
            HandleScope::Prefix(p) => p,
            HandleScope::Exact(_) => {
                unreachable!("Handle<Prefix> always carries HandleScope::Prefix")
            }
        }
    }

    /// Narrow-only guest-side attenuation to a sub-tree. Rejects widening.
    pub fn narrow(&self, sub: &str) -> Result<Handle<Prefix>, ArtifactError> {
        let candidate = join_scope(self.current_prefix(), sub)?;
        let new_scope = HandleScope::Prefix(candidate);
        self.ensure_narrows(&new_scope)?;
        let mint = self.mint.extend(new_scope.clone());
        Ok(Handle {
            store: self.store.clone(),
            scope: new_scope,
            mint,
            _s: PhantomData,
        })
    }

    /// Narrow-only guest-side attenuation to a single file.
    pub fn file(&self, leaf: &str) -> Result<Handle<Exact>, ArtifactError> {
        let candidate = join_scope(self.current_prefix(), leaf)?;
        let new_scope = HandleScope::Exact(candidate);
        self.ensure_narrows(&new_scope)?;
        let mint = self.mint.extend(new_scope.clone());
        Ok(Handle {
            store: self.store.clone(),
            scope: new_scope,
            mint,
            _s: PhantomData,
        })
    }

    fn ensure_narrows(&self, new_scope: &HandleScope) -> Result<(), ArtifactError> {
        if self.scope.contains(new_scope) {
            Ok(())
        } else {
            Err(ArtifactError::WideningRejected {
                current: self.scope.clone(),
                requested: new_scope.clone(),
            })
        }
    }
}

impl Handle<Exact> {
    /// Host-side: mint a handle directly bound to one file (e.g. immediately
    /// after `stage_artifact` writes it). Never called from guest code.
    pub fn host_mint_exact(
        store: StoreRef,
        root_key: &[u8],
        root_key_id: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        let scope = HandleScope::Exact(path.into());
        let mint = Macaroon::mint(root_key, root_key_id, scope.clone());
        Handle {
            store,
            scope,
            mint,
            _s: PhantomData,
        }
    }
}

impl<S: Scope> Handle<S> {
    pub fn store(&self) -> &StoreRef {
        &self.store
    }

    pub fn scope(&self) -> &HandleScope {
        &self.scope
    }

    pub fn mint(&self) -> &Macaroon {
        &self.mint
    }

    /// Host-side: does this handle's macaroon verify against `root_key`,
    /// for the scope actually being accessed (§16.4 gate 2)?
    pub fn verify(&self, root_key: &[u8]) -> bool {
        self.mint.verify(root_key, &self.scope)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Artifact<S>
// ─────────────────────────────────────────────────────────────────────────

/// The data-plane value: a handle plus enough self-description to route it
/// without re-fetching (§16.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "S: Scope")]
pub struct Artifact<S: Scope = Exact> {
    pub handle: Handle<S>,
    pub content_type: String,
    pub len: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────
// ByteSource
// ─────────────────────────────────────────────────────────────────────────

/// The universal byte-input type (§16.1). Everything that accepts "content
/// bytes" accepts this.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ByteSource {
    Inline(#[serde(with = "base64_bytes")] Vec<u8>),
    Artifact(Artifact<Exact>),
}

impl From<&str> for ByteSource {
    fn from(value: &str) -> Self {
        ByteSource::Inline(value.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for ByteSource {
    fn from(value: Vec<u8>) -> Self {
        ByteSource::Inline(value)
    }
}

impl From<Artifact<Exact>> for ByteSource {
    fn from(value: Artifact<Exact>) -> Self {
        ByteSource::Artifact(value)
    }
}

mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// stage_artifact sugar + content-hash-at-stage (review F6)
// ─────────────────────────────────────────────────────────────────────────

/// Host-side minting seam that the free `stage_artifact` helper uses to
/// produce a handle for the freshly-written file. A real host wires this to
/// its root key store.
///
/// H5b (done): the `WorkspaceWrite` view (above) now carries the root key +
/// key id internally and exposes the §16.5 `ws.stage_artifact(name, bytes,
/// content_type)` shape with no explicit minter parameter. This trait + the
/// free function below remain the minter-injecting scaffold used by the H5a
/// unit tests and any caller that wants an explicit minter.
pub trait WorkspaceMinter: Send + Sync {
    fn mint_exact(&self, path: &str) -> Handle<Exact>;
}

/// Stage bytes into the workspace and return an `Artifact` whose
/// `content_hash` is computed here — at stage time, since the backends
/// leave `WorkspaceEntry.content_hash` `None` today (review F6, §16.7).
///
/// H5a: takes an explicit `minter` because host-side root-key wiring
/// (`WorkspaceWrite` view construction) is H5b's job, not H5a's.
pub async fn stage_artifact(
    workspace: &dyn crate::workspace::Workspace,
    minter: &dyn WorkspaceMinter,
    name: &str,
    bytes: &[u8],
    content_type: impl Into<String>,
) -> Result<Artifact<Exact>, WorkspaceError> {
    let write_result = workspace
        .write(
            name,
            bytes,
            crate::workspace::WorkspaceWriteOptions::default(),
        )
        .await?;
    let handle = minter.mint_exact(&write_result.path);
    Ok(Artifact {
        handle,
        content_type: content_type.into(),
        len: bytes.len() as u64,
        content_hash: Some(sha256_hex(bytes)),
    })
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

// ─────────────────────────────────────────────────────────────────────────
// Capability-narrowed node-facing views (§16.4 — the handle-only byte surface)
// ─────────────────────────────────────────────────────────────────────────

/// Failure surfaced by the handle-scoped workspace views when a byte crossing
/// is refused by one of the three deref gates (§16.4), or by the backing
/// workspace itself.
#[derive(Debug, thiserror::Error)]
pub enum ByteAccessError {
    /// Backend / path-normalization failure from the wrapped `Workspace`.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Gate 2: the handle's macaroon tag did not verify against the host root
    /// key — a forged or tampered mint.
    #[error("gate 2: handle macaroon failed to verify against the host root key")]
    MintVerification,
    /// Gate 3: the concrete requested path is not contained by the handle's
    /// caveat chain (a widened runtime scope witness).
    #[error("gate 3: requested path `{requested}` is not permitted by handle scope caveats")]
    OutOfScope { requested: String },
    /// The handle names a different store than the view is bound to.
    #[error("handle names store `{handle_store}`, view is bound to `{view_store}`")]
    StoreMismatch {
        handle_store: String,
        view_store: String,
    },
    /// The requested entry does not exist.
    #[error("workspace entry not found: {0}")]
    NotFound(String),
}

/// Shared read path for both views: enforce store binding + gate 2 (mint
/// verify) + gate 3 (scope caveats) before touching the raw `Workspace`.
async fn read_via_handle(
    workspace: &dyn Workspace,
    root_key: &[u8],
    store: &StoreRef,
    handle: &Handle<Exact>,
) -> Result<Vec<u8>, ByteAccessError> {
    if handle.store() != store {
        return Err(ByteAccessError::StoreMismatch {
            handle_store: handle.store().as_str().to_string(),
            view_store: store.as_str().to_string(),
        });
    }
    // Gate 2 — macaroon tag verifies against the host root key.
    if !handle.mint().verify_tag(root_key) {
        return Err(ByteAccessError::MintVerification);
    }
    // Resolve the concrete leaf; traversal is rejected here exactly as raw
    // workspace paths are.
    let normalized = normalize_path(handle.scope().path())?;
    let target = HandleScope::Exact(normalized.clone());
    // Gate 3 — the concrete path is contained by every caveat.
    if !handle.mint().caveats_permit(&target) {
        return Err(ByteAccessError::OutOfScope {
            requested: normalized,
        });
    }
    match workspace.read_normalized(&normalized).await? {
        Some(WorkspaceReadResult::Bytes(bytes)) => Ok(bytes),
        Some(WorkspaceReadResult::BlobRef(_)) => Err(ByteAccessError::Workspace(
            WorkspaceError::Unsupported("workspace returned a blob reference, not bytes".into()),
        )),
        None => Err(ByteAccessError::NotFound(normalized)),
    }
}

/// Read-only handle-scoped view (§16.4). Granted by `resource::workspace::read`.
/// Its ONLY method is `read(handle)` — no `stage`, no arbitrary-path read, no
/// `list`/`delete`. Wraps the host-internal `Workspace`; the raw trait is never
/// handed to a node.
#[derive(Clone)]
pub struct WorkspaceRead {
    workspace: Arc<dyn Workspace>,
    root_key: Arc<[u8]>,
    store: StoreRef,
}

impl WorkspaceRead {
    /// Host-side constructor. `root_key` never leaves the host; the node only
    /// ever holds this view, not the key.
    pub fn new(
        workspace: Arc<dyn Workspace>,
        root_key: impl Into<Arc<[u8]>>,
        store: StoreRef,
    ) -> Self {
        Self {
            workspace,
            root_key: root_key.into(),
            store,
        }
    }

    /// Deref a single-file handle to its bytes, enforcing all three gates.
    pub async fn read(&self, handle: &Handle<Exact>) -> Result<Vec<u8>, ByteAccessError> {
        read_via_handle(self.workspace.as_ref(), &self.root_key, &self.store, handle).await
    }
}

/// Read+write handle-scoped view (§16.4). Granted by `resource::workspace::write`.
/// Exposes `stage_artifact` (the §16.5 3-arg shape — the minter/root key ride
/// internally) and `read(handle)`. Still no arbitrary-path read or `delete`.
#[derive(Clone)]
pub struct WorkspaceWrite {
    workspace: Arc<dyn Workspace>,
    root_key: Arc<[u8]>,
    root_key_id: String,
    store: StoreRef,
}

impl WorkspaceWrite {
    /// Host-side constructor. Carries the root key + key id used to mint the
    /// `Exact` handle for each staged file.
    pub fn new(
        workspace: Arc<dyn Workspace>,
        root_key: impl Into<Arc<[u8]>>,
        root_key_id: impl Into<String>,
        store: StoreRef,
    ) -> Self {
        Self {
            workspace,
            root_key: root_key.into(),
            root_key_id: root_key_id.into(),
            store,
        }
    }

    /// Stage bytes and return an `Artifact` whose `content_hash` is computed
    /// here at stage time (§16.5, review F6). The freshly-written path is
    /// minted into an `Exact` handle bound to this view's store + root key.
    pub async fn stage_artifact(
        &self,
        name: &str,
        bytes: &[u8],
        content_type: impl Into<String>,
    ) -> Result<Artifact<Exact>, ByteAccessError> {
        let write_result = self
            .workspace
            .write(
                name,
                bytes,
                crate::workspace::WorkspaceWriteOptions::default(),
            )
            .await?;
        let handle = Handle::host_mint_exact(
            self.store.clone(),
            &self.root_key,
            self.root_key_id.clone(),
            write_result.path,
        );
        Ok(Artifact {
            handle,
            content_type: content_type.into(),
            len: bytes.len() as u64,
            content_hash: Some(sha256_hex(bytes)),
        })
    }

    /// Deref a single-file handle to its bytes, enforcing all three gates.
    pub async fn read(&self, handle: &Handle<Exact>) -> Result<Vec<u8>, ByteAccessError> {
        read_via_handle(self.workspace.as_ref(), &self.root_key, &self.store, handle).await
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Capability;
    use crate::workspace::{
        Workspace, WorkspaceDeleteResult, WorkspaceEntry, WorkspaceError, WorkspaceListOptions,
        WorkspaceReadResult, WorkspaceWriteOptions, WorkspaceWriteResult,
    };
    use std::sync::Mutex;

    const ROOT_KEY: &[u8] = b"test-root-key-do-not-use-in-prod";

    fn root_handle() -> Handle<Prefix> {
        Handle::host_mint_prefix(StoreRef::new("ws"), ROOT_KEY, "root-1", "")
    }

    // ---- F9: fail-closed validating Deserialize ----

    #[test]
    fn exact_handle_refuses_prefix_wire_value() {
        let root = root_handle();
        let prefix_handle = root.narrow("uploads").expect("narrow");
        let json = serde_json::to_string(&prefix_handle).expect("serialize");

        let err = serde_json::from_str::<Handle<Exact>>(&json).expect_err("must fail closed");
        assert!(err.to_string().contains("handle scope mismatch"));
    }

    #[test]
    fn prefix_handle_refuses_exact_wire_value() {
        let root = root_handle();
        let file_handle = root.file("uploads/report.csv").expect("file");
        let json = serde_json::to_string(&file_handle).expect("serialize");

        let err = serde_json::from_str::<Handle<Prefix>>(&json).expect_err("must fail closed");
        assert!(err.to_string().contains("handle scope mismatch"));
    }

    #[test]
    fn matching_scope_round_trips() {
        let root = root_handle();
        let file_handle = root.file("uploads/report.csv").expect("file");
        let json = serde_json::to_string(&file_handle).expect("serialize");
        let decoded: Handle<Exact> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, file_handle);
    }

    // ---- Macaroon narrow/compose/verify ----

    #[test]
    fn narrow_then_file_composes_and_verifies() {
        let root = root_handle();
        let handle = root
            .narrow("uploads")
            .expect("narrow")
            .file("report.csv")
            .expect("file");

        assert!(handle.verify(ROOT_KEY));
        assert_eq!(
            handle.scope(),
            &HandleScope::Exact("uploads/report.csv".into())
        );
    }

    #[test]
    fn split_independent_compositions_both_verify_and_both_reject_out_of_scope() {
        let root = root_handle();

        let via_narrow = root.narrow("a/").expect("narrow").file("b").expect("file");
        let via_direct = root.file("a/b").expect("file");

        // Different caveat chains -> different tags (composition is
        // path-split-dependent at the tag level, expected per §16.2 review
        // F3), but BOTH verify against the root.
        assert_ne!(via_narrow.mint().tag, via_direct.mint().tag);
        assert!(via_narrow.verify(ROOT_KEY));
        assert!(via_direct.verify(ROOT_KEY));

        assert_eq!(via_narrow.scope(), via_direct.scope());

        // Both reject any path not under a/b: a handle for c/d cannot be
        // constructed from either without going through narrow/file (which
        // itself would fail at the widening check off a disjoint scope), so
        // we exercise the verify()-level check by asserting a forged
        // "c/d" scope does not satisfy either chain's caveats.
        let out_of_scope = HandleScope::Exact("c/d".into());
        assert!(!via_narrow.mint().verify(ROOT_KEY, &out_of_scope));
        assert!(!via_direct.mint().verify(ROOT_KEY, &out_of_scope));
    }

    #[test]
    fn widening_is_rejected_at_attenuation_time() {
        let root = root_handle();
        let scoped = root.narrow("uploads").expect("narrow");

        // "uploads/.." style widening is caught by normalize_path traversal
        // rejection inside join_scope; also check a sibling/widen path that
        // normalize_path would accept syntactically but is not a sub-path.
        let err = scoped
            .narrow("../other")
            .expect_err("must reject widening/traversal");
        matches!(err, ArtifactError::InvalidPath(_));

        // A prefix that is a *sibling*, not a subtree, must also be rejected.
        // Simulate by minting a second independent root-prefix handle scoped
        // to "other" and trying to treat it as a narrowing of `scoped`
        // (uploads) — done by hand-checking `contains`.
        let sibling_scope = HandleScope::Prefix("other".into());
        assert!(!scoped.scope().contains(&sibling_scope));
    }

    #[test]
    fn forged_tag_fails_verify() {
        let root = root_handle();
        let mut handle = root.file("uploads/report.csv").expect("file");
        // Corrupt the tag to simulate a forged mint.
        if let Some(byte) = handle.mint.tag.first_mut() {
            *byte ^= 0xFF;
        } else {
            handle.mint.tag.push(0xFF);
        }
        assert!(!handle.verify(ROOT_KEY));
    }

    #[test]
    fn verify_fails_with_wrong_root_key() {
        let root = root_handle();
        let handle = root.file("uploads/report.csv").expect("file");
        assert!(!handle.verify(b"wrong-key"));
    }

    // ---- stage_artifact / content-hash-at-stage (F6) ----

    #[derive(Default)]
    struct InMemoryWorkspace {
        files: Mutex<std::collections::HashMap<String, Vec<u8>>>,
    }

    impl Capability for InMemoryWorkspace {
        fn name(&self) -> &'static str {
            "workspace.test"
        }
    }

    #[async_trait::async_trait]
    impl Workspace for InMemoryWorkspace {
        async fn read_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            let files = self.files.lock().expect("lock poisoned");
            Ok(files
                .get(normalized_path)
                .cloned()
                .map(WorkspaceReadResult::Bytes))
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, WorkspaceError> {
            let mut files = self.files.lock().expect("lock poisoned");
            files.insert(normalized_path.to_string(), data.to_vec());
            Ok(WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: 0,
            })
        }

        async fn list_normalized(
            &self,
            _options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
            Ok(Vec::new())
        }

        async fn delete_normalized(
            &self,
            _normalized_path: &str,
        ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
            Ok(WorkspaceDeleteResult { deleted: false })
        }
    }

    struct TestMinter;
    impl WorkspaceMinter for TestMinter {
        fn mint_exact(&self, path: &str) -> Handle<Exact> {
            Handle::host_mint_exact(StoreRef::new("ws"), ROOT_KEY, "root-1", path)
        }
    }

    // ---- H5b: handle-only views + gate 2/gate 3 honesty tests (§16.4) ----

    fn ws_arc() -> Arc<InMemoryWorkspace> {
        Arc::new(InMemoryWorkspace::default())
    }

    #[tokio::test]
    async fn write_view_stage_then_read_round_trips_through_gates() {
        let ws = ws_arc();
        let writer =
            WorkspaceWrite::new(ws.clone(), ROOT_KEY.to_vec(), "root-1", StoreRef::new("ws"));
        let artifact = writer
            .stage_artifact("uploads/report.csv", b"a,b\n1,2\n", "text/csv")
            .await
            .expect("stage");
        assert_eq!(artifact.content_hash, Some(sha256_hex(b"a,b\n1,2\n")));

        // Read the same handle back through the read-only view.
        let reader = WorkspaceRead::new(ws.clone(), ROOT_KEY.to_vec(), StoreRef::new("ws"));
        let bytes = reader.read(&artifact.handle).await.expect("read");
        assert_eq!(bytes, b"a,b\n1,2\n");
    }

    #[tokio::test]
    async fn gate2_forged_mint_fails_verify() {
        let ws = ws_arc();
        ws.files
            .lock()
            .unwrap()
            .insert("uploads/report.csv".into(), b"secret".to_vec());
        let reader = WorkspaceRead::new(ws, ROOT_KEY.to_vec(), StoreRef::new("ws"));

        let mut handle = root_handle().file("uploads/report.csv").expect("file");
        // Corrupt the tag → forged mint.
        handle.mint.tag[0] ^= 0xFF;

        let err = reader
            .read(&handle)
            .await
            .expect_err("forged mint must fail");
        assert!(matches!(err, ByteAccessError::MintVerification));
    }

    #[tokio::test]
    async fn gate3_out_of_scope_path_fails_after_valid_tag() {
        let ws = ws_arc();
        ws.files
            .lock()
            .unwrap()
            .insert("uploads/secret.csv".into(), b"secret".to_vec());
        let reader = WorkspaceRead::new(ws, ROOT_KEY.to_vec(), StoreRef::new("ws"));

        // Valid mint for uploads/report.csv, but the runtime scope witness is
        // widened to a sibling the caveats do NOT cover. Tag still verifies
        // (it folds only the caveat chain), so this isolates gate 3.
        let mut handle = root_handle().file("uploads/report.csv").expect("file");
        assert!(handle.mint().verify_tag(ROOT_KEY));
        handle.scope = HandleScope::Exact("uploads/secret.csv".into());

        let err = reader
            .read(&handle)
            .await
            .expect_err("out-of-scope path must fail");
        assert!(matches!(err, ByteAccessError::OutOfScope { .. }));
    }

    #[tokio::test]
    async fn view_rejects_handle_from_another_store() {
        let ws = ws_arc();
        let reader = WorkspaceRead::new(ws, ROOT_KEY.to_vec(), StoreRef::new("ws"));
        let handle = Handle::<Prefix>::host_mint_prefix(
            StoreRef::new("other-store"),
            ROOT_KEY,
            "root-1",
            "",
        )
        .file("uploads/report.csv")
        .expect("file");
        let err = reader
            .read(&handle)
            .await
            .expect_err("cross-store handle must fail");
        assert!(matches!(err, ByteAccessError::StoreMismatch { .. }));
    }

    #[tokio::test]
    async fn stage_artifact_populates_content_hash_and_len() {
        let workspace = InMemoryWorkspace::default();
        let minter = TestMinter;
        let bytes = b"a,b,c\n1,2,3\n".to_vec();

        let artifact = stage_artifact(&workspace, &minter, "report.csv", &bytes, "text/csv")
            .await
            .expect("stage succeeds");

        assert_eq!(artifact.len, bytes.len() as u64);
        assert_eq!(artifact.content_type, "text/csv");
        assert_eq!(artifact.content_hash, Some(sha256_hex(&bytes)));
        assert!(artifact.handle.verify(ROOT_KEY));
    }

    // ---- ByteSource serde round-trip ----

    #[test]
    fn byte_source_inline_round_trips_as_base64() {
        let source = ByteSource::from(b"hello world".to_vec());
        let json = serde_json::to_string(&source).expect("serialize");
        // Inline payload must be a base64 string on the wire, not a JSON
        // int-array.
        assert!(json.contains("aGVsbG8gd29ybGQ="));
        let decoded: ByteSource = serde_json::from_str(&json).expect("deserialize");
        match decoded {
            ByteSource::Inline(bytes) => assert_eq!(bytes, b"hello world"),
            ByteSource::Artifact(_) => panic!("expected inline"),
        }
    }

    #[test]
    fn byte_source_artifact_round_trips_with_content_type() {
        let root = root_handle();
        let handle = root.file("uploads/report.csv").expect("file");
        let artifact = Artifact {
            handle,
            content_type: "text/csv".to_string(),
            len: 42,
            content_hash: Some("deadbeef".to_string()),
        };
        let source = ByteSource::from(artifact.clone());
        let json = serde_json::to_string(&source).expect("serialize");
        let decoded: ByteSource = serde_json::from_str(&json).expect("deserialize");
        match decoded {
            ByteSource::Artifact(a) => {
                assert_eq!(a.content_type, "text/csv");
                assert_eq!(a.len, 42);
                assert_eq!(a.content_hash.as_deref(), Some("deadbeef"));
            }
            ByteSource::Inline(_) => panic!("expected artifact"),
        }
    }
}
