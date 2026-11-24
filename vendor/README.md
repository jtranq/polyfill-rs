# Temporary Alloy dependency migration

These are the crates.io sources of `alloy-primitives 1.6.0`, `alloy-signer 1.8.3`, `alloy-signer-local 1.8.3`, `alloy-consensus 1.8.3` and `alloy-eip7702 0.6.3`.

The root Cargo manifest overrides these five packages so both Polyfill and the pinned official SDK use `k256 0.14`, `elliptic-curve 0.14` and the patched `crypto-bigint 0.7.11` release. No RustCrypto source is modified. The manifest changes are in the normalized `Cargo.toml` files. `Cargo.toml.orig` preserves each published source manifest.

The source adaptations rename SEC1 point conversion APIs and bridge the existing rand 0.8 interface to rand_core 0.10. The bridge forwards all random output to the caller's generator and retains its cryptographic RNG requirement. It does not implement key generation or arithmetic.

Cargo applies `[patch.crates-io]` only at the workspace root. These overrides fix builds from this checkout, including its examples and the SDK benchmark. They do not fix applications that depend on a published Polyfill crate unless those applications also install the overrides. Do not describe a crates.io release as fixing this advisory until compatible upstream Alloy releases replace these overrides or consumers have an explicit supported migration path.

Remove the overrides and these copies once upstream Alloy supports the patched RustCrypto dependency stack. The original package versions remain unchanged to satisfy the SDK's dependency requirements. Optional upstream features outside Polyfill's feature set have not been validated.

`migration.patch` is a compact review diff against the published sources with trailing whitespace normalized. `UPSTREAM.json` records each original source commit.
