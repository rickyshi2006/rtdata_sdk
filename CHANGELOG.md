# Changelog

## 0.2.0 - 2026-07-20

- Add additive `TOKEN_STATUS (0x42)` protocol support.
- Add `TokenStatus`, `on_token_status`, `token_status`, and token expiry accessors.
- Read gateway version and enabled protocol features from discovery responses.
- Preserve compatibility with gateways that do not support token status events.
- Prevent dead or superseded sockets from reporting a successful reconnect.
- Stop automatic reconnect after an expired, disabled, or revoked token status.
- Allow an explicit reconnect after the token is restored.
