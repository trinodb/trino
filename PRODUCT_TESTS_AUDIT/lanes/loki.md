# Lane Audit: Loki

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Loki runtime support`
- Section end commit: `Remove legacy SuiteLoki`
- Introduced JUnit suites: `SuiteLoki`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteLoki`.
- Environment classes introduced: `LokiEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `LokiEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeLoki` and current `LokiEnvironment`.
- Recorded differences:
  - Legacy launcher environment used a multinode Trino topology with Loki wired through the launcher common environment stack; current environment uses a single `TrinoProductTestContainer` plus `LokiContainer`.
  - Current environment resolves the Loki URI dynamically from the container instead of hard-coding the in-network `http://loki:3100` URI in the test body.
  - Current environment narrows the runtime surface to the Loki connector only.
- Reviewer note: the topology is simplified, but the lane still covers the same Loki `query_range` read path and no unresolved Loki-specific fidelity gap was found.

## Suite Semantic Audit
### `SuiteLoki`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against legacy launcher `SuiteLoki` and current `SuiteLoki`.
- Recorded differences:
  - Legacy suite ran `configured_features` + `loki` on `EnvMultinodeLoki`; current suite runs `ConfiguredFeatures` + `Loki` on `LokiEnvironment`.
  - Current suite is a dedicated JUnit entrypoint rather than a launcher suite wrapper.
- Reviewer note: suite coverage remains semantically faithful after the environment extraction.

## Ported Test Classes

### `TestLoki`


- Owning migration commit: `Migrate TestLoki to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/loki/TestLoki.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/loki/TestLoki.java`
- Class-level environment requirement: `LokiEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `Loki`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testQueryRange`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/loki/TestLoki.java` ->
  `testQueryRange`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/loki/TestLoki.java` ->
  `TestLoki.testQueryRange`
- Mapping type: `direct`
- Environment parity: Current class requires `LokiEnvironment`. Routed by source review into `SuiteLoki` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Loki`, `ProfileSpecificTests`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: `URI.create`. Current setup shape: `URI.create`.
- Action parity: Legacy action shape: `SELECT`, `okiClient`, `okiClientConfig`, `Duration.ofSeconds`, `Instant.now`, `minus`, `Duration.ofHours`, `start.plus`, `client.pushLogLine`, `end.minus`, `Duration.ofMinutes`, `ImmutableMap.of`, `client.flush`, `onTrino`, `executeQuery`, `TABLE`, `system.query_range`, `TIMESTAMP_FORMATTER.format`. Current action shape: `SELECT`, `okiClient`, `okiClientConfig`, `env.getLokiHttpUrl`, `Duration.ofSeconds`, `Instant.now`, `minus`, `Duration.ofHours`, `start.plus`, `client.pushLogLine`, `end.minus`, `Duration.ofMinutes`, `ImmutableMap.of`, `client.flush`, `env.executeTrino`, `TABLE`, `system.query_range`, `TIMESTAMP_FORMATTER.format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [okiClient, okiClientConfig, URI.create, Duration.ofSeconds, Instant.now, minus, Duration.ofHours, start.plus, client.pushLogLine, end.minus, Duration.ofMinutes, ImmutableMap.of, client.flush, onTrino, executeQuery, TABLE, system.query_range, TIMESTAMP_FORMATTER.format] vs current [okiClient, okiClientConfig, URI.create, env.getLokiHttpUrl, Duration.ofSeconds, Instant.now, minus, Duration.ofHours, start.plus, client.pushLogLine, end.minus, Duration.ofMinutes, ImmutableMap.of, client.flush, env.executeTrino, TABLE, system.query_range, TIMESTAMP_FORMATTER.format]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [okiClient, okiClientConfig, URI.create, Duration.ofSeconds, Instant.now, minus, Duration.ofHours, start.plus, client.pushLogLine, end.minus, Duration.ofMinutes, ImmutableMap.of, client.flush, onTrino, executeQuery, TABLE, system.query_range, TIMESTAMP_FORMATTER.format], verbs [SELECT]. Current flow summary -> helpers [okiClient, okiClientConfig, URI.create, env.getLokiHttpUrl, Duration.ofSeconds, Instant.now, minus, Duration.ofHours, start.plus, client.pushLogLine, end.minus, Duration.ofMinutes, ImmutableMap.of, client.flush, env.executeTrino, TABLE, system.query_range, TIMESTAMP_FORMATTER.format], verbs [SELECT].
- Audit status: `verified`
