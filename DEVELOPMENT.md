# Development and release process

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

The supported compatibility matrix is documented in the Cloud Gateway
`protocol/COMPATIBILITY.md`.

## Release

1. Update `_version.py` from a development version to the final version.
2. Update `CHANGELOG.md`.
3. Merge after CI passes.
4. Create the matching `vX.Y.Z` tag.
5. The release workflow rebuilds and tests wheel/sdist in clean environments,
   generates checksums, and publishes immutable GitHub Release artifacts.

Published package files are never replaced in place. Fixes use a new patch
version.
