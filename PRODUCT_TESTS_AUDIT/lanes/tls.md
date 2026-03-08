# Lane Audit: Tls

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add TLS environment`
- Section end commit: `Migrate TestTls to JUnit`
- Introduced JUnit suites: `SuiteTls`.
- Extended existing suites: none.
- Retired legacy suites: none.
- Environment classes introduced: `TlsEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `TlsEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeTls` and the TLS run in legacy launcher `Suite3`.
- Recorded differences:
  - Legacy TLS coverage ran on a three-node TLS cluster and discovered worker hosts through `system.runtime.nodes`; current `TlsEnvironment` is a single-node dedicated TLS environment and checks the coordinator host directly.
  - Legacy launcher environment layered TLS on top of the standard Hadoop-based multinode environment; current environment is a purpose-built single-node Trino TLS container with only the `tpch` catalog configured.
  - Current test verifies failed plain-HTTP query execution by opening a JDBC connection to the HTTP port; legacy test verified only socket-level port closure on worker and coordinator hosts.
- Reviewer note: the runtime topology is narrower than the old `Suite3` TLS run, but the lane still preserves the legacy method’s intended TLS-only connectivity check and documents the extracted dedicated suite shape explicitly.

## Suite Semantic Audit
### `SuiteTls`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against the TLS run in legacy launcher `Suite3` (`EnvMultinodeTls` with `CONFIGURED_FEATURES`, `SMOKE`, `CLI`, `GROUP_BY`, `JOIN`, `TLS`, excluding `AZURE`) and current `SuiteTls`.
- Recorded differences:
  - Current suite is a dedicated one-run TLS suite selecting only `ConfiguredFeatures` + `Tls` on `TlsEnvironment`; legacy TLS coverage was one tagged subset inside aggregate `Suite3`.
  - The dedicated current suite does not carry the unrelated `SMOKE`, `CLI`, `GROUP_BY`, and `JOIN` routing that shared the legacy aggregate environment.
- Reviewer note: this is an extraction and narrowing of the legacy TLS-specific coverage rather than a one-for-one recreation of the full aggregate `Suite3` run.

## Ported Test Classes

### `TestTlsJunit`


- Owning migration commit: `Migrate TestTls to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/tls/TestTlsJunit.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestTls.java`
- Class-level environment requirement: `TlsEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `ProfileSpecificTests`, `Tls`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testHttpPortIsClosed`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestTls.java` ->
  `testHttpPortIsClosed`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/tls/TestTlsJunit.java` ->
  `TestTlsJunit.testHttpPortIsClosed`
- Mapping type: `direct`
- Environment parity: Current class requires `TlsEnvironment`. Routed by source review into `SuiteTls` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `ProfileSpecificTests`, `Tls`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: `URI.create`. Current setup shape: none.
- Action parity: Legacy action shape: `waitForNodeRefresh`, `getActiveNodesUrls`, `activeNodesUrls.stream`, `map`, `getHost`, `collect`, `toList`. Current action shape: `waitForNodeRefresh`, `isPortOpen`, `env.getHost`, `env.getHttpsPort`, `canQueryOverHttp`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isNotNull`, `hasSize`, `assertPortIsOpen`, `assertPortIsClosed`. Current assertion helpers: `assertThat`, `isTrue`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [waitForNodeRefresh, getActiveNodesUrls, activeNodesUrls.stream, map, URI.create, getHost, collect, toList] vs current [waitForNodeRefresh, isPortOpen, env.getHost, env.getHttpsPort, canQueryOverHttp]; assertion helpers differ: legacy [assertThat, isNotNull, hasSize, assertPortIsOpen, assertPortIsClosed] vs current [assertThat, isTrue, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [waitForNodeRefresh, getActiveNodesUrls, activeNodesUrls.stream, map, URI.create, getHost, collect, toList], verbs [none]. Current flow summary -> helpers [waitForNodeRefresh, isPortOpen, env.getHost, env.getHttpsPort, canQueryOverHttp], verbs [none].
- Audit status: `verified`
