# Lane Audit: Fault Tolerant

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add FaultTolerant environment`
- Section end commit: `Migrate TestTaskRetriesFilesystemSmoke to JUnit`
- Introduced JUnit suites: `SuiteFaultTolerant`.
- Extended existing suites: none.
- Retired legacy suites: none.
- Environment classes introduced: `FaultTolerantEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `FaultTolerantEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the legacy product-test class assumptions in `TestTaskRetriesFilesystemSmoke` and the current dedicated `FaultTolerantEnvironment`.
- Recorded differences:
  - Legacy class relied on a preconfigured Trino environment supplying task-retry filesystem exchange settings through the surrounding launcher/runtime setup.
  - Current environment makes that runtime explicit with a dedicated `MultiNodeTrinoCluster`, one worker, MinIO-backed filesystem exchange manager, and the task-retry configuration properties set directly in code.
- Reviewer note: there was no dedicated legacy launcher suite for this lane; current environment extracts and makes explicit the runtime assumptions the legacy test depended on.

## Suite Semantic Audit
### `SuiteFaultTolerant`
- Suite semantic audit status: `complete`
- CI bucket: `hive-transactional`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against the legacy `FAULT_TOLERANT`-tagged class coverage and current `SuiteFaultTolerant`.
- Recorded differences:
  - No dedicated legacy launcher suite existed; current `SuiteFaultTolerant` is an explicit JUnit suite for the previously tag-driven coverage.
  - Current suite routes only the `FaultTolerant` tag on the dedicated environment needed by the legacy test.
- Reviewer note: this is an explicit extraction of previously implicit coverage, not a reduction in method coverage.

## Ported Test Classes

### `TestTaskRetriesFilesystemSmoke`


- Owning migration commit: `Migrate TestTaskRetriesFilesystemSmoke to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/faulttolerant/TestTaskRetriesFilesystemSmoke.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/faulttolerant/TestTaskRetriesFilesystemSmoke.java`
- Class-level environment requirement: `FaultTolerantEnvironment`.
- Class-level tags: `FaultTolerant`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testSimpleQuery`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/faulttolerant/TestTaskRetriesFilesystemSmoke.java` ->
  `testSimpleQuery`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/faulttolerant/TestTaskRetriesFilesystemSmoke.java` ->
  `TestTaskRetriesFilesystemSmoke.testSimpleQuery`
- Mapping type: `direct`
- Environment parity: Current class requires `FaultTolerantEnvironment`. Routed by source review into `SuiteFaultTolerant` run 1.
- Tag parity: Current tags: `FaultTolerant`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `count`, `getOnlyValue`. Current action shape: `SELECT`, `env.executeTrino`, `count`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, count, getOnlyValue] vs current [env.executeTrino, count]; assertion helpers differ: legacy [assertThat, isEqualTo] vs current [assertThat, containsOnly, row]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, count, getOnlyValue], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, count], verbs [SELECT].
- Audit status: `verified`
