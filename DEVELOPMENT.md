# Development and release process

## Local-first integration

Review and test the development branch, merge into the original checkout's
LOCAL `main`, validate there, then synchronize that exact main to GitHub.
Do not merge remotely first, force-push, or bypass repository protection.
Back up and preserve unrelated uncommitted work and linked worktrees before
integration. Source synchronization does not publish a package or authorize
deployment; release tags and immutable artifacts are a separate step.

## Branching and CI

- `main` must remain releasable.
- Use short-lived `feature/*` and `fix/*` branches.
- Protect `main` and require `.github/workflows/ci.yml`.
- CI runs unit and compatibility tests across supported Python versions and
  verifies wheel installation.

## Versioning

The package version has one source:

```text
rtdata/_version.py
```

`pyproject.toml` reads this value dynamically. Release tags use `vX.Y.Z`; the
release workflow rejects tags that do not match the package version.

## Protocol compatibility

The SDK must:

- ignore unknown additive message types;
- tolerate unknown JSON fields;
- work when an old gateway does not send `TOKEN_STATUS`;
- treat malformed optional control notifications as non-fatal;
- preserve existing `AUTH_RESPONSE` behavior.

Client-facing compatibility and quote behavior are documented in
`docs/REALTIME-FULL-FIELDS.md` and `docs/SDK_USAGE.md`.

## Release

1. Update `_version.py` from a development version to the final version.
2. Update `CHANGELOG.md`.
3. Merge after CI passes.
4. Create the matching `vX.Y.Z` tag.
5. The release workflow rebuilds and tests wheel/sdist in clean environments,
   generates checksums, and publishes immutable GitHub Release artifacts.

Normally, published package files are immutable and fixes use a new patch version.
An explicitly approved same-version consolidation must first archive the old
artifacts outside this public repository, validate local main, regenerate all
checksums, and verify the downloaded replacement. Document force-reinstallation
for clients already using that version; do not silently change an installation.

## Public content

Keep this repository and release notes focused on client APIs and usage.
Do not publish service implementation details, storage technology, internal
resource names, deployment topology, machine identifiers or operational reports.
Keep internal account-policy implementation and rollout records out of release
notes and public change summaries. Retain them in the private integration workspace.
Use market categories and synthetic endpoints in examples and tests. Keep
protocol numeric identifiers stable when changing client-facing labels.
Use provider-neutral module, constant and test names as well. Constructors
accept only explicit supported parameters, not hidden provider-selection
keywords. Schema naming changes must preserve field order, types and wire hashes.
