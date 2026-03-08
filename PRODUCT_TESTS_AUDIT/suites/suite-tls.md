# Suite Audit: SuiteTls

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Tls coverage.
- Owning lane: `tls`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteTls.java`
- CI bucket: `auth-and-clients`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `TlsEnvironment`
- Include tags: `ConfiguredFeatures`, `Tls`.
- Exclude tags: none.
- Expected mapped classes covered: `TestTlsJunit`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `auth-and-clients`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite identified`

## Parity Checklist

- Legacy suite or lane source: TLS coverage extracted from the `EnvMultinodeTls` run inside legacy launcher `Suite3`.
- Current suite class: `SuiteTls`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestTlsJunit`.
- Expected migrated methods covered: `TestTlsJunit.testHttpPortIsClosed`.
- Parity status: `verified`
- Recorded differences:
  - Current suite is a dedicated TLS-only JUnit suite rather than one tagged subset inside aggregate legacy `Suite3`.
  - Current run uses `TlsEnvironment` (single-node) instead of legacy `EnvMultinodeTls` (three-node).
