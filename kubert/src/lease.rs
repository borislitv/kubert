//! A distributed, advisory lock implementation for Kubernetes
//!
//! Applications that manage state in Kubernetes--for instance, those that
//! update resource statuses, may need to coordinate access to that state so
//! that only one replica is trying to update resources at a time.
//!
//! [`LeaseManager`] interacts with a [`coordv1::Lease`] resource to ensure that
//! only a single claimant owns the lease at a time.

use k8s_openapi::{api::coordination::v1 as coordv1, apimachinery::pkg::apis::meta::v1 as metav1};
use std::{borrow::Cow, sync::{Arc, atomic::{AtomicBool, Ordering}}};
use tokio::time::{self, Duration};

#[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
use crate::admin::LeaseDiagnostics;

/// Manages a Kubernetes `Lease`
#[cfg_attr(docsrs, doc(cfg(feature = "lease")))]
pub struct LeaseManager {
    api: Api,
    name: String,
    field_manager: Cow<'static, str>,
    state: tokio::sync::Mutex<State>,

    #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
    diagnostics: Option<LeaseDiagnostics>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(docsrs, doc(cfg(feature = "lease")))]
/// Configures a Lease.
pub struct LeaseParams {
    /// Lease name.
    pub name: String,

    /// Lease namespace.
    pub namespace: String,

    /// The identity of the claimant.
    pub claimant: String,

    /// The duration of the lease
    pub lease_duration: Duration,

    /// The amount of time before the lease expiration that the lease holder
    /// should renew the lease
    pub renew_grace_period: Duration,

    /// The field manager used when updating the Lease.
    pub field_manager: Option<Cow<'static, str>>,
}

/// Configuration used when obtaining a lease.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(docsrs, doc(cfg(feature = "lease")))]
pub struct ClaimParams {
    /// The duration of the lease
    pub lease_duration: Duration,

    /// The amount of time before the lease expiration that the lease holder
    /// should renew the lease
    pub renew_grace_period: Duration,
}

/// Describes the state of a lease
#[derive(Clone, Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "lease")))]
pub struct Claim {
    /// The identity of the claim holder.
    pub holder: String,

    /// The time that the lease expires.
    pub expiry: chrono::DateTime<chrono::Utc>,

    // A wall-clock rollback must not extend an observed claim. The timestamp is
    // retained for diagnostics, but cannot by itself establish local validity.
    valid_until: time::Instant,
    expired: Arc<AtomicBool>,
}

/// Indicates an error interacting with the Lease API
#[derive(Debug, thiserror::Error)]
#[cfg_attr(docsrs, doc(cfg(feature = "lease")))]
pub enum Error {
    /// An error was received from the Kubernetes API
    #[error("failed to get lease: {0}")]
    Api(#[from] kube_client::Error),

    /// Lease resource does not have a resourceVersion
    #[error("lease does not have a resource version")]
    MissingResourceVersion,

    /// Lease resource does not have a spec
    #[error("lease does not have a spec")]
    MissingSpec,

    /// A Kubernetes API call timed out
    #[error("timed out")]
    Timeout,

    /// Parameters cannot be represented by a positive Kubernetes lease.
    #[error("invalid lease parameters")]
    InvalidParameters,

    /// The server supplied an invalid transition count or it cannot advance.
    #[error("lease transition counter is invalid or exhausted")]
    InvalidTransitions,

    /// A reply arrived after the previous local ownership bound elapsed.
    #[error("lease expired during renewal")]
    Expired,
}

#[derive(Clone, Debug)]
struct State {
    meta: Meta,
    claim: Option<Arc<Claim>>,
}

#[derive(Clone, Debug)]
struct Meta {
    version: String,
    transitions: i32,
}

pub(crate) type Api = kube_client::Api<coordv1::Lease>;

pub(crate) type Spawned = (
    tokio::sync::watch::Receiver<Arc<Claim>>,
    tokio::task::JoinHandle<Result<(), Error>>,
);

// === impl ClaimParams ===

impl Default for ClaimParams {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(30),
            renew_grace_period: Duration::from_secs(1),
        }
    }
}

// === impl Claim ===

impl Claim {
    /// Remaining local validity, bounded by both the reported wall-clock expiry
    /// and the monotonic deadline established when this version was observed.
    pub fn remaining(&self) -> Duration {
        if self.expired.load(Ordering::Acquire) { return Duration::ZERO; }
        let remaining = self.valid_until.saturating_duration_since(time::Instant::now())
            .min((self.expiry - chrono::Utc::now()).to_std().unwrap_or_default());
        if remaining.is_zero() { self.expired.store(true, Ordering::Release); }
        remaining
    }

    /// Returns true iff neither observed expiry bound has elapsed.
    #[inline]
    pub fn is_current(&self) -> bool {
        !self.remaining().is_zero()
    }

    /// Returns true iff the claim is still valid for the provided claimant
    #[inline]
    pub fn is_current_for(&self, claimant: &str) -> bool {
        self.holder == claimant && self.is_current()
    }

    /// Waits for the claim to expire
    pub async fn expire(&self) {
        self.expire_with_grace(Duration::ZERO).await;
    }

    /// Waits until there is a grace period remaining before the claim expires
    pub async fn expire_with_grace(&self, grace: Duration) {
        let sleep = self.remaining().saturating_sub(grace);
        if !sleep.is_zero() {
            tokio::time::sleep(sleep).await;
        }
    }
}

impl PartialEq for Claim {
    fn eq(&self, other: &Self) -> bool { self.holder == other.holder && self.expiry == other.expiry }
}
impl Eq for Claim {}

impl State {
    fn observe(&mut self, mut next: Self) {
        if let (Some(previous), Some(current)) = (&self.claim, next.claim.as_ref()) {
            // Metadata-only writes also change resourceVersion. They must not
            // extend the lifetime of the same observed ownership timestamp.
            if self.meta.transitions == next.meta.transitions
                && previous.holder == current.holder && previous.expiry == current.expiry {
                    let mut current = (**current).clone();
                    current.valid_until = current.valid_until.min(previous.valid_until);
                    current.expired = previous.expired.clone();
                    next.claim = Some(Arc::new(current));
            }
        }
        *self = next;
    }
}

// === impl LeaseManager ===

impl LeaseManager {
    pub(crate) const DEFAULT_FIELD_MANAGER: &'static str = "kubert";
    const API_TIMEOUT: Duration = Duration::from_secs(10);

    /// Initialize a lease's state from the Kubernetes API.
    ///
    /// The named lease resource must already have been created, or a 404 error
    /// will be returned.
    pub async fn init(api: Api, name: impl ToString) -> Result<Self, Error> {
        let name = name.to_string();
        let state = Self::get(api.clone(), &name).await?;
        Ok(Self {
            api,
            name,
            field_manager: Self::DEFAULT_FIELD_MANAGER.into(),
            state: tokio::sync::Mutex::new(state),
            #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
            diagnostics: None,
        })
    }

    /// Overrides the field manager used when updating the Lease
    ///
    /// This is intended to be used immediately following initialization and
    /// before `ensure_claimed` is invoked.
    pub fn with_field_manager(mut self, field_manager: impl Into<Cow<'static, str>>) -> Self {
        self.field_manager = field_manager.into();
        self
    }

    #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
    pub(crate) fn with_diagnostics(mut self, diagnostics: LeaseDiagnostics) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    /// Return the state of the claim without updating it from the API.
    pub async fn claimed(&self) -> Option<Arc<Claim>> {
        self.state.lock().await.claim.clone()
    }

    /// Update the state of the claim from the API.
    pub async fn sync(&self) -> Result<Option<Arc<Claim>>, Error> {
        time::timeout(Self::API_TIMEOUT, async {
            let mut state = self.state.lock().await;
            state.observe(Self::get(self.api.clone(), &self.name).await?);
            #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
            if let Some(diagnostics) = self.diagnostics.as_ref() {
                diagnostics.inspect(state.claim.clone(), state.meta.version.clone());
            }
            Ok(state.claim.clone())
        }).await.map_err(|_| Error::Timeout)?
    }

    /// Ensures that the lease, if it exists, is claimed.
    ///
    /// If these is not currently held, it is claimed by the provided identity.
    /// If it is currently held by the provided claimant, it is renewed if it is
    /// within the renew grace period.
    pub async fn ensure_claimed(
        &self,
        claimant: &str,
        params: &ClaimParams,
    ) -> Result<Arc<Claim>, Error> {
        // Bound the complete operation, including mutex waits and repeated CAS
        // conflicts. Individual HTTP timeouts alone do not bound that loop.
        time::timeout(Self::API_TIMEOUT, self.ensure_claimed_inner(claimant, params))
            .await.map_err(|_| Error::Timeout)?
    }

    async fn ensure_claimed_inner(&self, claimant: &str, params: &ClaimParams) -> Result<Arc<Claim>, Error> {
        if claimant.is_empty() || claimant.len() > 256 || claimant.chars().any(char::is_control)
            || params.lease_duration.is_zero() || params.lease_duration.subsec_nanos() != 0
            || params.lease_duration.as_secs() > i32::MAX as u64
            || params.renew_grace_period >= params.lease_duration {
            return Err(Error::InvalidParameters);
        }
        let mut state = self.state.lock().await;
        // Every ownership decision starts with a fresh resourceVersion/holder.
        // A cached unexpired claim is not evidence of current server ownership.
        state.observe(Self::get(self.api.clone(), &self.name).await?);
        loop {
            if let Some(claim) = state.claim.as_ref() {
                // If the claim is held by the provided claimant, then consider
                // renewing the claim.
                if claim.holder == claimant && claim.is_current() {
                    if claim.remaining() > params.renew_grace_period {
                        return Ok(claim.clone());
                    }

                    let (claim, meta) = match self.renew(&state.meta, claimant, params).await {
                        Ok(renew) => renew,

                        Err(e) if Self::is_conflict(&e) => {
                            // Another process updated the claim's resource version, so
                            // re-sync the state and try again.
                            state.observe(Self::get(self.api.clone(), &self.name).await?);
                            #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
                            if let Some(diagnostics) = self.diagnostics.as_ref() {
                                diagnostics
                                    .inspect(state.claim.clone(), state.meta.version.clone());
                            }
                            continue;
                        }

                        Err(e) => return Err(e),
                    };

                    if state.claim.as_ref().is_none_or(|prior| !prior.is_current()) {
                        return Err(Error::Expired);
                    }

                    *state = State {
                        claim: Some(claim.clone()),
                        meta,
                    };
                    #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
                    if let Some(diagnostics) = self.diagnostics.as_ref() {
                        diagnostics.inspect(state.claim.clone(), state.meta.version.clone());
                    }
                    return Ok(claim);
                }

                // The claim is held by another claimant, return it.
                if claim.is_current() {
                    return Ok(claim.clone());
                }
            }

            // There's no current claim, so try to acquire it.
            let (claim, meta) = match self.acquire(&state.meta, claimant, params).await {
                Ok(acquire) => acquire,

                Err(e) if Self::is_conflict(&e) => {
                    // Another process updated the claim's resource version, so
                    // re-sync the state and try again.
                    state.observe(Self::get(self.api.clone(), &self.name).await?);
                    #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
                    if let Some(diagnostics) = self.diagnostics.as_ref() {
                        diagnostics.inspect(state.claim.clone(), state.meta.version.clone());
                    }
                    continue;
                }

                Err(e) => return Err(e),
            };

            *state = State {
                claim: Some(claim.clone()),
                meta,
            };
            #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
            if let Some(diagnostics) = self.diagnostics.as_ref() {
                diagnostics.inspect(state.claim.clone(), state.meta.version.clone());
            }

            return Ok(claim);
        }
    }

    /// Clear out the state of the lease if the claim is currently held by the
    /// provided identity.
    ///
    /// This is typically used during process shutdown so that another process
    /// can potentially claim the lease before the prior lease duration expires.
    pub async fn vacate(&self, claimant: &str) -> Result<bool, Error> {
        time::timeout(Self::API_TIMEOUT, self.vacate_inner(claimant))
            .await.map_err(|_| Error::Timeout)?
    }

    async fn vacate_inner(&self, claimant: &str) -> Result<bool, Error> {
        let mut state = self.state.lock().await;
        state.observe(Self::get(self.api.clone(), &self.name).await?);
        let Some(claim) = state.claim.clone() else {
            return Ok(false);
        };

        if !claim.is_current() {
            return Ok(false);
        }

        if claim.holder != claimant {
            return Ok(false);
        }

        let observed = time::Instant::now();
        let lease = self
            .patch(&kube_client::api::Patch::Strategic(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "resourceVersion": state.meta.version,
                },
                "spec": {
                    "acquireTime": Option::<()>::None,
                    "renewTime": Option::<()>::None,
                    "holderIdentity": Option::<()>::None,
                    "leaseDurationSeconds": Option::<()>::None,
                    // leaseTransitions is preserved by strategic patch
                },
            })))
            .await?;

        state.observe(Self::from_lease(lease, observed)?);
        #[cfg(all(feature = "runtime", feature = "runtime-diagnostics"))]
        if let Some(diagnostics) = self.diagnostics.as_ref() {
            diagnostics.inspect(
                state.claim.clone(),
                state.meta.version.clone(),
            );
        }

        Ok(true)
    }

    /// Spawn a task that ensures the lease is claimed.
    ///
    /// When the lease becomes unclaimed, the task attempts to claim the lease
    /// as _claimant_ and maintains the lease until the task completes or the
    /// lease is claimed by another process.
    ///
    /// The state of the lease is published via the returned receiver.
    ///
    /// When all receivers are dropped, the task completes and the lease is
    /// vacated so that another process can claim it.
    pub async fn spawn(
        self,
        claimant: impl ToString,
        params: ClaimParams,
    ) -> Result<Spawned, Error> {
        let claimant = claimant.to_string();
        let mut claim = self.ensure_claimed(&claimant, &params).await?;
        let (tx, rx) = tokio::sync::watch::channel(claim.clone());

        let task = tokio::spawn(async move {
            loop {
                // The claimant has the privilege of renewing the lease before
                // the claim expires.
                let grace = if claim.holder == claimant {
                    params.renew_grace_period
                } else {
                    Duration::ZERO
                };

                // Wait for the current claim to expire. If all receivers are
                // dropped while we're waiting, the task terminates.
                tokio::select! {
                    biased;
                    _ = tx.closed() => break,
                    _ = claim.expire_with_grace(grace) => {}
                }

                // An uncertain API result ends this campaign. Do not keep an
                // apparently healthy watch channel alive behind unbounded retry.
                claim = tokio::select! {
                    biased;
                    _ = tx.closed() => break,
                    result = self.ensure_claimed(&claimant, &params) => result?,
                };
                if tx.send(claim.clone()).is_err() {
                    // All receivers have been dropped.
                    break;
                }
            }

            self.vacate(&claimant).await?;
            Ok(())
        });

        Ok((rx, task))
    }

    /// Acquire the lease (i.e. assuming the claimant IS NOT the current holder
    /// of the lease).
    ///
    /// A server-side apply is used to update the resource. If another writer
    /// has updated the resource since the last read, this write fails with a
    /// conflict.
    async fn acquire(
        &self,
        meta: &Meta,
        claimant: &str,
        params: &ClaimParams,
    ) -> Result<(Arc<Claim>, Meta), Error> {
        let lease_duration = chrono::Duration::from_std(params.lease_duration)
            .map_err(|_| Error::InvalidParameters)?;
        let now = chrono::Utc::now();
        let observed = time::Instant::now();
        let transitions = meta.transitions.checked_add(1).ok_or(Error::InvalidTransitions)?;
        let lease = self
            .patch(&kube_client::api::Patch::Apply(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "resourceVersion": meta.version,
                },
                "spec": {
                    "acquireTime": metav1::MicroTime(now),
                    "renewTime": metav1::MicroTime(now),
                    "holderIdentity": claimant,
                    "leaseDurationSeconds": lease_duration.num_seconds(),
                    "leaseTransitions": transitions,
                },
            })))
            .await?;

        let state = Self::from_lease(lease, observed)?;
        let claim = state.claim.ok_or(Error::InvalidParameters)?;
        if claim.holder != claimant || state.meta.transitions != transitions || !claim.is_current() {
            return Err(Error::InvalidParameters);
        }
        Ok((claim, state.meta))
    }

    /// Renew the lease (i.e. assuming the claimant IS the current holder of the
    /// lease).
    ///
    /// A strategic merge is used so that only the `renewTime` field is updated
    /// in most cases. The `leaseDurationSeconds` fields may also be updated if
    /// the caller passed an updated value.
    async fn renew(
        &self,
        meta: &Meta,
        claimant: &str,
        params: &ClaimParams,
    ) -> Result<(Arc<Claim>, Meta), Error> {
        let lease_duration = chrono::Duration::from_std(params.lease_duration)
            .map_err(|_| Error::InvalidParameters)?;
        let now = chrono::Utc::now();
        let observed = time::Instant::now();
        let lease = self
            .patch(&kube_client::api::Patch::Strategic(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "resourceVersion": meta.version,
                },
                "spec": {
                    "holderIdentity": claimant,
                    "renewTime": metav1::MicroTime(now),
                    "leaseDurationSeconds": lease_duration.num_seconds(),
                },
            })))
            .await?;

        let state = Self::from_lease(lease, observed)?;
        let claim = state.claim.ok_or(Error::InvalidParameters)?;
        if claim.holder != claimant || state.meta.transitions != meta.transitions || !claim.is_current() {
            return Err(Error::InvalidParameters);
        }
        Ok((claim, state.meta))
    }

    async fn patch<P>(&self, patch: &kube_client::api::Patch<P>) -> Result<coordv1::Lease, Error>
    where
        P: serde::Serialize + std::fmt::Debug,
    {
        tracing::debug!(?patch);
        let params = kube_client::api::PatchParams {
            field_manager: Some(self.field_manager.to_string()),
            // Force conflict resolution when using Server-side Apply (i.e., to
            // acquire a lease). This is the recommended behavior for
            // controllers. See: https://kubernetes.io/docs/reference/using-api/server-side-apply/#conflicts
            force: matches!(patch, kube_client::api::Patch::Apply(_)),
            ..Default::default()
        };
        time::timeout(
            Self::API_TIMEOUT,
            self.api.patch(&self.name, &params, patch),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(Into::into)
    }

    async fn get(api: Api, name: &str) -> Result<State, Error> {
        let observed = time::Instant::now();
        let lease = time::timeout(Self::API_TIMEOUT, api.get(name))
            .await
            .map_err(|_| Error::Timeout)??;
        Self::from_lease(lease, observed)
    }

    fn from_lease(lease: coordv1::Lease, observed: time::Instant) -> Result<State, Error> {
        // Kubernetes may omit an empty optional spec for a pre-created Lease.
        let spec = lease.spec.unwrap_or_default();

        let version = lease
            .metadata
            .resource_version
            .ok_or(Error::MissingResourceVersion)?;
        let transitions = spec.lease_transitions.unwrap_or(0);
        if transitions < 0 { return Err(Error::InvalidTransitions); }
        let meta = Meta {
            version,
            transitions,
        };

        macro_rules! or_unclaimed {
            ($e:expr) => {
                match $e {
                    Some(e) => e,
                    None => {
                        return Ok(State { meta, claim: None });
                    }
                }
            };
        }

        let holder = or_unclaimed!(spec.holder_identity);
        if holder.is_empty() { return Ok(State { meta, claim: None }); }

        let metav1::MicroTime(renew_time) = or_unclaimed!(spec.renew_time);
        let seconds = or_unclaimed!(spec.lease_duration_seconds);
        if seconds <= 0 { return Err(Error::InvalidParameters); }
        let lease_duration = chrono::Duration::seconds(seconds.into());
        let expiry = renew_time.checked_add_signed(lease_duration).ok_or(Error::InvalidParameters)?;
        let remaining = (expiry - chrono::Utc::now()).to_std().unwrap_or_default()
            .min(Duration::from_secs(seconds as u64));
        let valid_until = observed.checked_add(remaining).ok_or(Error::InvalidParameters)?;

        Ok(State {
            meta,
            // Retain an expired observation too: a later GET of the same version
            // must not resurrect it after a wall-clock rollback.
            claim: Some(Arc::new(Claim { holder, expiry, valid_until, expired: Arc::new(AtomicBool::new(false)) })),
        })
    }

    fn is_conflict(err: &Error) -> bool {
        matches!(
            err,
            Error::Api(kube_client::Error::Api(kube_core::ErrorResponse { code, .. }))
                if hyper::StatusCode::from_u16(*code).ok() == Some(hyper::StatusCode::CONFLICT)
        )
    }
}
