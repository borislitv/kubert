# Owned Lease hardening

This fork preserves the upstream Apache-2.0 license and history from
[olix0r/kubert](https://github.com/olix0r/kubert), version `0.25.0`, commit
`a2ff424f072a0de77d98ab97592d6f66ae1d9546`.

The named-Lease implementation is changed for bounded advisory candidacy:

- Ownership decisions refresh the observed holder and `resourceVersion` before
  acquisition, renewal or release. Writes carry that resource version; SSA force
  resolves field-manager conflicts without dropping the optimistic concurrency
  precondition.
- Pre-created resources with no optional spec are unclaimed. Transition counts
  use checked, nonnegative `i32` arithmetic rather than narrowing to `u16`.
- Each complete `ensure_claimed`, `sync` or `vacate` operation has a ten-second
  timeout, including mutex acquisition and CAS retries.
- Returned claims combine the server wall-clock expiry with a monotonic deadline
  bounded from the API request's start. An observed expired claim cannot revive
  after a wall-clock rollback or a metadata-only resource-version update.
- Late renewal replies and uncertain API results end the local campaign. The
  spawned task does not hide errors behind indefinite retries.
- The `lease` feature explicitly selects its clock, Tokio timer, runtime and
  macro requirements; the obsolete retry dependency is removed.

`Claim` retains public holder/expiry diagnostics and adds private local-validity
state. External code should consume API-returned claims and use `remaining`,
`is_current` or `is_current_for`; it can no longer construct a claim from two
public fields. Diagnostics in this fork are adapted to that change.

These are advisory Kubernetes claims, not a distributed storage fence or a
cross-service atomic transaction. A consumer that publishes durable state still
needs its own confirmed monotonic authority and checks at the publication
boundary. Source changes and pinned tests do not themselves establish real-API,
failure, resource or deployment qualification.

Prepared with AI assistance.
