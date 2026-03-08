# Lane Audit: Clients

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add CLI environment`
- Section end commit: `Remove legacy SuiteClients`
- Introduced JUnit suites: none.
- Extended existing suites: `SuiteClients`.
- Retired legacy suites: `SuiteClients`.
- Environment classes introduced: `CliEnvironment`.
- Method status counts: verified `24`, intentional difference `6`, needs follow-up `0`.

## Semantic Audit Status

- Baseline note: detailed method, environment, and suite records below are retained from earlier audit work and must not be treated as final semantic findings until this lane is marked `complete` in `PROGRESS.md`.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Current-only CLI helper coverage is explicitly documented method-by-method rather than implied as legacy parity.
- Current JMX coverage matches the legacy `TestJmxConnector` methods after the move into `TestJmxConnectorJunit`; no JMX-specific open fidelity issue remains.

## Environment Semantic Audit
### `CliEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the legacy `EnvMultinode` launcher runtime used by `SuiteClients` and the current `CliEnvironment` source.
- Recorded differences:
  - Current CLI environment is a dedicated Trino+PostgreSQL Testcontainers environment instead of the legacy mixed launcher environment.
  - Current environment adds a PostgreSQL catalog explicitly because `shouldPrintExplainAnalyzePlan` now uses PostgreSQL tables for row-level write coverage.
- Reviewer note: this environment split is deliberate and preserves the CLI test intent, including the restored
  transaction-oriented CLI flows.

## Suite Semantic Audit
### `SuiteClients`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `extended by this lane`.
- Legacy/current basis: compared against legacy launcher `SuiteClients` and current `SuiteClients` source, focusing on the CLI extension introduced in this lane.
- Recorded differences:
  - Current suite decomposes the old single launcher run into separate JDBC and CLI runs.
  - CLI coverage remains routed by `TestGroup.Cli`, and the transaction-abort plus rollback flows are now represented
    in the current JUnit class.
- Reviewer note: suite routing is structurally correct for the CLI portion and no remaining CLI fidelity issue is
  recorded in this lane.

## Ported Test Classes

### `TestTrinoCli`


- Owning migration commit: `Migrate TestTrinoCli to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java`
- Class-level environment requirement: `CliEnvironment`.
- Class-level tags: `Cli`.
- Method inventory complete: Yes. Legacy methods: `20`. Current methods: `28`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testSetRole` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestRoles.java` -> `testSetRole`.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`, `verified`.

#### Methods

##### `shouldDisplayVersion`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldDisplayVersion`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldDisplayVersion`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCli`, `trino.readRemainingOutputLines`, `readTrinoCliVersion`. Current action shape: `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`, `trim`, `readTrinoCliVersion`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsExactly`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCli, trino.readRemainingOutputLines, readTrinoCliVersion] vs current [env.executeCli, result.getExitCode, isZero, result.getStdout, trim, readTrinoCliVersion]; assertion helpers differ: legacy [assertThat, containsExactly] vs current [assertThat, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCli, trino.readRemainingOutputLines, readTrinoCliVersion], verbs [none]. Current flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout, trim, readTrinoCliVersion], verbs [none].
- Audit status: `verified`

##### `shouldRunBatchQuery`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldRunBatchQuery`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldRunBatchQuery`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `env.executeCli`, `result.getExitCode`, `isZero`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [env.executeCli, result.getExitCode, isZero, trimLines, result.getStdout, containsAll]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [env.executeCli, result.getExitCode, isZero, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldRunQuery`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldRunQuery`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldRunQuery`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`. Current action shape: `SELECT`, `env.executeCliWithStdin`, `result.getExitCode`, `isZero`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println] vs current [env.executeCliWithStdin, result.getExitCode, isZero, trimLines, result.getStdout, containsAll]; assertion helpers differ: legacy [none] vs current [assertThat]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println], verbs [SELECT]. Current flow summary -> helpers [env.executeCliWithStdin, result.getExitCode, isZero, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldRunBatchQueryWithStdinRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldRunBatchQueryWithStdinRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldRunBatchQueryWithStdinRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `empFile`, `writeString`, `file.path`, `launchTrinoCliWithRedirectedStdin`, `file.file`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `env.executeCliWithStdin`, `result.getExitCode`, `isZero`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [empFile, writeString, file.path, launchTrinoCliWithRedirectedStdin, file.file, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [env.executeCliWithStdin, result.getExitCode, isZero, trimLines, result.getStdout, containsAll]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [empFile, writeString, file.path, launchTrinoCliWithRedirectedStdin, file.file, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [env.executeCliWithStdin, result.getExitCode, isZero, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldRunQueryFromFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldRunQueryFromFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldRunQueryFromFile`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `createFile.getExitCode`.
- Action parity: Legacy action shape: `SELECT`, `empFile`, `writeString`, `file.path`, `launchTrinoCliWithServerArgument`, `file.file`, `getAbsolutePath`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `env.getTrinoContainer`, `execInContainer`, `isZero`, `env.executeCliWithFile`, `result.getExitCode`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [empFile, writeString, file.path, launchTrinoCliWithServerArgument, file.file, getAbsolutePath, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [env.getTrinoContainer, execInContainer, createFile.getExitCode, isZero, env.executeCliWithFile, result.getExitCode, trimLines, result.getStdout, containsAll]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [empFile, writeString, file.path, launchTrinoCliWithServerArgument, file.file, getAbsolutePath, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [env.getTrinoContainer, execInContainer, createFile.getExitCode, isZero, env.executeCliWithFile, result.getExitCode, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldExitOnErrorFromFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldExitOnErrorFromFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldExitOnErrorFromFile`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `createFile.getExitCode`.
- Action parity: Legacy action shape: `SELECT`, `empFile`, `writeString`, `file.path`, `launchTrinoCliWithServerArgument`, `file.file`, `getAbsolutePath`, `trimLines`, `trino.readRemainingOutputLines`, `trino.waitForWithTimeoutAndKill`, `hasMessage`. Current action shape: `SELECT`, `env.getTrinoContainer`, `execInContainer`, `isZero`, `env.executeCliWithFile`, `result.getExitCode`, `isNotZero`, `result.getStdout`, `trim`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEmpty`, `assertThatThrownBy`. Current assertion helpers: `assertThat`, `isEmpty`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [empFile, writeString, file.path, launchTrinoCliWithServerArgument, file.file, getAbsolutePath, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill, hasMessage] vs current [env.getTrinoContainer, execInContainer, createFile.getExitCode, isZero, env.executeCliWithFile, result.getExitCode, isNotZero, result.getStdout, trim]; assertion helpers differ: legacy [assertThat, isEmpty, assertThatThrownBy] vs current [assertThat, isEmpty]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [empFile, writeString, file.path, launchTrinoCliWithServerArgument, file.file, getAbsolutePath, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill, hasMessage], verbs [SELECT]. Current flow summary -> helpers [env.getTrinoContainer, execInContainer, createFile.getExitCode, isZero, env.executeCliWithFile, result.getExitCode, isNotZero, result.getStdout, trim], verbs [SELECT].
- Audit status: `verified`

##### `shouldNotExitOnErrorFromFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldNotExitOnErrorFromFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldNotExitOnErrorFromFile`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `createFile.getExitCode`.
- Action parity: Legacy action shape: `SELECT`, `empFile`, `writeString`, `file.path`, `launchTrinoCliWithServerArgument`, `file.file`, `getAbsolutePath`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`, `hasMessage`. Current action shape: `SELECT`, `env.getTrinoContainer`, `execInContainer`, `isZero`, `env.executeCliWithFile`, `result.getExitCode`, `isNotZero`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `assertThatThrownBy`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [empFile, writeString, file.path, launchTrinoCliWithServerArgument, file.file, getAbsolutePath, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill, hasMessage] vs current [env.getTrinoContainer, execInContainer, createFile.getExitCode, isZero, env.executeCliWithFile, result.getExitCode, isNotZero, trimLines, result.getStdout, containsAll]; assertion helpers differ: legacy [assertThat, assertThatThrownBy] vs current [assertThat]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [empFile, writeString, file.path, launchTrinoCliWithServerArgument, file.file, getAbsolutePath, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill, hasMessage], verbs [SELECT]. Current flow summary -> helpers [env.getTrinoContainer, execInContainer, createFile.getExitCode, isZero, env.executeCliWithFile, result.getExitCode, isNotZero, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldRunMultipleBatchQueriesWithStdinRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldRunMultipleBatchQueriesWithStdinRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldRunMultipleBatchQueriesWithStdinRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `empFile`, `writeString`, `file.path`, `launchTrinoCliWithRedirectedStdin`, `file.file`, `builder`, `addAll`, `add`, `build`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `env.executeCliWithStdin`, `result.getExitCode`, `isZero`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [empFile, writeString, file.path, launchTrinoCliWithRedirectedStdin, file.file, builder, addAll, add, build, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [env.executeCliWithStdin, result.getExitCode, isZero, trimLines, result.getStdout, containsAll]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [empFile, writeString, file.path, launchTrinoCliWithRedirectedStdin, file.file, builder, addAll, add, build, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [env.executeCliWithStdin, result.getExitCode, isZero, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldExecuteEmptyListOfStatements`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldExecuteEmptyListOfStatements`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldExecuteEmptyListOfStatements`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `trino.waitForWithTimeoutAndKill`. Current action shape: `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`, `trim`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEmpty`. Current assertion helpers: `assertThat`, `isEmpty`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill] vs current [env.executeCli, result.getExitCode, isZero, result.getStdout, trim]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill], verbs [none]. Current flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout, trim], verbs [none].
- Audit status: `verified`

##### `shouldPassSessionUser`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldPassSessionUser`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldPassSessionUser`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill] vs current [env.executeCli, result.getExitCode, isZero, result.getStdout]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout], verbs [SELECT].
- Audit status: `verified`

##### `shouldUseCatalogAndSchemaOptions`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldUseCatalogAndSchemaOptions`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldUseCatalogAndSchemaOptions`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `env.executeCli`, `result.getExitCode`, `isZero`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [env.executeCli, result.getExitCode, isZero, trimLines, result.getStdout, containsAll]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [env.executeCli, result.getExitCode, isZero, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldExitOnErrorFromExecute`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldExitOnErrorFromExecute`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldExitOnErrorFromExecute`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `trino.waitForWithTimeoutAndKill`, `hasMessage`. Current action shape: `SELECT`, `env.executeCli`, `result.getExitCode`, `isNotZero`, `fails`, `result.getStdout`, `trim`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEmpty`, `assertThatThrownBy`. Current assertion helpers: `assertThat`, `isEmpty`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill, hasMessage] vs current [env.executeCli, result.getExitCode, isNotZero, fails, result.getStdout, trim]; assertion helpers differ: legacy [assertThat, isEmpty, assertThatThrownBy] vs current [assertThat, isEmpty]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, trino.waitForWithTimeoutAndKill, hasMessage], verbs [SELECT]. Current flow summary -> helpers [env.executeCli, result.getExitCode, isNotZero, fails, result.getStdout, trim], verbs [SELECT].
- Audit status: `verified`

##### `shouldNotExitOnErrorFromExecute`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldNotExitOnErrorFromExecute`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldNotExitOnErrorFromExecute`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`, `hasMessage`. Current action shape: `SELECT`, `env.executeCli`, `result.getExitCode`, `isNotZero`, `trimLines`, `result.getStdout`, `containsAll`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `assertThatThrownBy`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill, hasMessage] vs current [env.executeCli, result.getExitCode, isNotZero, trimLines, result.getStdout, containsAll]; assertion helpers differ: legacy [assertThat, assertThatThrownBy] vs current [assertThat]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill, hasMessage], verbs [SELECT]. Current flow summary -> helpers [env.executeCli, result.getExitCode, isNotZero, trimLines, result.getStdout, containsAll], verbs [SELECT].
- Audit status: `verified`

##### `shouldHandleUseStatement`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldHandleSession`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldHandleUseStatement`
- Mapping type: `split`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `SET`. Current setup shape: `USE`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`, `trino.readLinesUntilPrompt`, `trimLines`, `containsAll`, `squeezeLines`. Current action shape: `env.executeCliWithStdin`, `count`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, trimLines, containsAll, squeezeLines] vs current [env.executeCliWithStdin, count, result.getExitCode, isZero, result.getStdout]; SQL verbs differ: legacy [USE, SELECT, SHOW, SET] vs current [USE]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, trimLines, containsAll, squeezeLines], verbs [USE, SELECT, SHOW, SET]. Current flow summary -> helpers [env.executeCliWithStdin, count, result.getExitCode, isZero, result.getStdout], verbs [USE].
- Audit status: `verified`

##### `shouldHandleSession`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldHandleSession`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldHandleSession`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `SET`. Current setup shape: `USE`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`, `trino.readLinesUntilPrompt`, `trimLines`, `containsAll`, `squeezeLines`. Current action shape: `SELECT`, `SHOW`, `env.executeCliWithStdin`, `count`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, trimLines, containsAll, squeezeLines] vs current [env.executeCliWithStdin, count, result.getExitCode, isZero, result.getStdout]; SQL verbs differ: legacy [USE, SELECT, SHOW, SET] vs current [USE, SELECT, SHOW]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, trimLines, containsAll, squeezeLines], verbs [USE, SELECT, SHOW, SET]. Current flow summary -> helpers [env.executeCliWithStdin, count, result.getExitCode, isZero, result.getStdout], verbs [USE, SELECT, SHOW].
- Audit status: `verified`

##### `shouldHandleSessionSettings`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldHandleSession`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldHandleSessionSettings`
- Mapping type: `split`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `SET`. Current setup shape: `SET`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`, `trino.readLinesUntilPrompt`, `trimLines`, `containsAll`, `squeezeLines`. Current action shape: `env.executeCliWithStdin`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, trimLines, containsAll, squeezeLines] vs current [env.executeCliWithStdin, result.getExitCode, isZero, result.getStdout]; SQL verbs differ: legacy [USE, SELECT, SHOW, SET] vs current [SET]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, trimLines, containsAll, squeezeLines], verbs [USE, SELECT, SHOW, SET]. Current flow summary -> helpers [env.executeCliWithStdin, result.getExitCode, isZero, result.getStdout], verbs [SET].
- Audit status: `verified`

##### `shouldHandleTransaction`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldHandleTransaction`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldHandleTransaction`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `CREATE`. Current setup shape: `USE`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`, `trino.readLinesUntilPrompt`, `txn_test`, `extracting`, `trimLines`, `containsAll`, `doesNotContain`, `txn_test1`, `txn_test2`. Current action shape: `SELECT`, `accepted`, `env.executeCliWithStdin`, `count`, `result.getExitCode`, `describedAs`, `result.getStdout`, `result.getStderr`, `isZero`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, txn_test, extracting, trimLines, containsAll, doesNotContain, txn_test1, txn_test2] vs current [env.executeCliWithStdin, result.getExitCode, describedAs, result.getStdout, result.getStderr, isZero]; assertion style changed from prompt-driven line capture to whole-process stdout/stderr assertions, but the abort, rollback, and commit behavior is now represented.
- Known intentional difference: None.
- Reviewer note: Legacy method covered transaction abort behavior, rejected commands in the aborted transaction,
  rollback recovery, and committed table creation. The current JUnit method now exercises those same abort, rollback,
  and post-commit visibility flows through `executeCliWithStdin`.
- Audit status: `verified`

##### `shouldHandleTransactionRollback`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldHandleTransaction`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldHandleTransactionRollback`
- Mapping type: `split`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `CREATE`. Current setup shape: `USE`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`, `trino.readLinesUntilPrompt`, `txn_test`, `extracting`, `trimLines`, `containsAll`, `doesNotContain`, `txn_test1`, `txn_test2`. Current action shape: `SELECT`, `env.executeCliWithStdin`, `count`, `result.getExitCode`, `describedAs`, `result.getStdout`, `result.getStderr`, `isZero`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trino.readLinesUntilPrompt, txn_test, extracting, trimLines, containsAll, doesNotContain, txn_test1, txn_test2] vs current [env.executeCliWithStdin, result.getExitCode, describedAs, result.getStdout, result.getStderr, isZero]; the split rollback method now preserves the legacy aborted-transaction failure plus rollback recovery checks while still using whole-process stdout/stderr assertions.
- Known intentional difference: None.
- Reviewer note: This split method now covers the legacy transaction-abort error handling and rollback recovery path,
  including the rejected statement inside the aborted transaction and the absence of the rolled-back table afterward.
- Audit status: `verified`

##### `shouldPrintExplainAnalyzePlan`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldPrintExplainAnalyzePlan`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldPrintExplainAnalyzePlan`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `COMMENT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `COMMENT`, `SET`, `createResult.getExitCode`, `createResult.getStdout`, `createResult.getStderr`.
- Action parity: Legacy action shape: `SELECT`, `ANALYZE`, `EXPLAIN`, `UPDATE`, `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`, `trimLines`, `trino.readLinesUntilPrompt`, `VALUES`, `onTrino`, `executeQuery`. Current action shape: `SELECT`, `ANALYZE`, `EXPLAIN`, `UPDATE`, `env.executeCli`, `describedAs`, `isZero`, `VALUES`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `assertExplainAnalyzeResult`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DROP`, `DELETE`, `dropResult.getExitCode`, `dropResult.getStdout`, `dropResult.getStderr`.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trimLines, trino.readLinesUntilPrompt, VALUES, onTrino, executeQuery] vs current [env.executeCli, dropResult.getExitCode, describedAs, dropResult.getStdout, dropResult.getStderr, isZero, createResult.getExitCode, createResult.getStdout, createResult.getStderr, VALUES]; SQL verbs differ: legacy [CREATE, SELECT, ANALYZE, EXPLAIN, INSERT, COMMENT, UPDATE, SET, DELETE, DROP] vs current [DROP, CREATE, SELECT, ANALYZE, EXPLAIN, INSERT, COMMENT, UPDATE, SET, DELETE]; assertion helpers differ: legacy [assertThat, contains] vs current [assertThat, assertExplainAnalyzeResult]
- Known intentional difference: Current test uses PostgreSQL-backed mutable tables instead of the legacy Iceberg-backed table so that CREATE TABLE AS SELECT, INSERT, UPDATE, and DELETE remain exercisable in the simplified CLI environment.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trimLines, trino.readLinesUntilPrompt, VALUES, onTrino, executeQuery], verbs [CREATE, SELECT, ANALYZE, EXPLAIN, INSERT, COMMENT, UPDATE, SET, DELETE, DROP]. Current flow summary -> helpers [env.executeCli, dropResult.getExitCode, describedAs, dropResult.getStdout, dropResult.getStderr, isZero, createResult.getExitCode, createResult.getStdout, createResult.getStderr, VALUES], verbs [DROP, CREATE, SELECT, ANALYZE, EXPLAIN, INSERT, COMMENT, UPDATE, SET, DELETE].
- Audit status: `intentional difference`

##### `shouldShowCatalogs`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldShowCatalogs`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `SHOW`, `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout], verbs [SHOW].
- Audit status: `intentional difference`

##### `shouldShowSchemas`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldShowSchemas`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `SHOW`, `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout], verbs [SHOW].
- Audit status: `intentional difference`

##### `shouldShowTables`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldShowTables`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `SHOW`, `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout], verbs [SHOW].
- Audit status: `intentional difference`

##### `shouldDescribeTable`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldDescribeTable`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: `COMMENT`.
- Action parity: No legacy counterpart. Current action shape: `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout], verbs [COMMENT].
- Audit status: `intentional difference`

##### `shouldSelectWithOutputFormat`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldSelectWithOutputFormat`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `SELECT`, `env.executeCli`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.executeCli, result.getExitCode, isZero, result.getStdout], verbs [SELECT].
- Audit status: `intentional difference`

##### `shouldHandleConfigEnvVariable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldUseCatalogAndSchemaOptionsFromConfigFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldHandleConfigEnvVariable`
- Mapping type: `renamed`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`, `createConfig.getExitCode`, `createConfig.getStdout`, `createConfig.getStderr`.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithConfigurationFile`, `ImmutableList.of`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `env.getTrinoContainer`, `execInContainer`, `describedAs`, `isZero`, `env.executeCliWithEnv`, `Map.of`, `result.getExitCode`, `result.getStdout`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [env.getTrinoContainer, execInContainer, createConfig.getExitCode, describedAs, createConfig.getStdout, createConfig.getStderr, isZero, env.executeCliWithEnv, Map.of, result.getExitCode, result.getStdout, result.getStderr]; SQL verbs differ: legacy [SELECT] vs current [CREATE, SELECT]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [env.getTrinoContainer, execInContainer, createConfig.getExitCode, describedAs, createConfig.getStdout, createConfig.getStderr, isZero, env.executeCliWithEnv, Map.of, result.getExitCode, result.getStdout, result.getStderr], verbs [CREATE, SELECT].
- Audit status: `verified`

##### `shouldUseCatalogAndSchemaOptionsFromConfigFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldUseCatalogAndSchemaOptionsFromConfigFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldUseCatalogAndSchemaOptionsFromConfigFile`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithConfigurationFile`, `ImmutableList.of`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `shouldHandleConfigEnvVariable`. Legacy delegate calls: none. Current delegate calls: `shouldHandleConfigEnvVariable`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [none] vs current [shouldHandleConfigEnvVariable]; helper calls differ: legacy [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [shouldHandleConfigEnvVariable]; SQL verbs differ: legacy [SELECT] vs current [none]; assertion helpers differ: legacy [assertThat] vs current [none]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [shouldHandleConfigEnvVariable], verbs [none].
- Audit status: `verified`

##### `shouldPreferCommandLineArgumentOverConfigDefault`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldPreferCommandLineArgumentOverConfigDefault`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldPreferCommandLineArgumentOverConfigDefault`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`, `createConfig.getExitCode`, `createConfig.getStdout`, `createConfig.getStderr`.
- Action parity: Legacy action shape: `SELECT`, `launchTrinoCliWithConfigurationFile`, `ImmutableList.of`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`, `trino.waitForWithTimeoutAndKill`. Current action shape: `SELECT`, `catalog`, `env.getTrinoContainer`, `execInContainer`, `describedAs`, `isZero`, `args`, `file`, `env.executeCliWithEnv`, `Map.of`, `count`, `result.getExitCode`, `result.getStdout`, `result.getStderr`, `succeed`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill] vs current [catalog, env.getTrinoContainer, execInContainer, createConfig.getExitCode, describedAs, createConfig.getStdout, createConfig.getStderr, isZero, args, file, env.executeCliWithEnv, Map.of, count, result.getExitCode, result.getStdout, result.getStderr, succeed]; SQL verbs differ: legacy [SELECT] vs current [CREATE, SELECT]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingOutputLines, containsAll, trino.waitForWithTimeoutAndKill], verbs [SELECT]. Current flow summary -> helpers [catalog, env.getTrinoContainer, execInContainer, createConfig.getExitCode, describedAs, createConfig.getStdout, createConfig.getStderr, isZero, args, file, env.executeCliWithEnv, Map.of, count, result.getExitCode, result.getStdout, result.getStderr, succeed], verbs [CREATE, SELECT].
- Audit status: `verified`

##### `shouldExitWithErrorOnUnknownPropertiesInConfigFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `shouldExitWithErrorOnUnknownPropertiesInConfigFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/cli/TestTrinoCli.java` ->
  `TestTrinoCli.shouldExitWithErrorOnUnknownPropertiesInConfigFile`
- Mapping type: `direct`
- Environment parity: Current class requires `CliEnvironment`. Routed by source review into `SuiteClients` run 2.
- Tag parity: Current tags: `Cli`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`, `createConfig.getExitCode`, `createConfig.getStdout`, `createConfig.getStderr`.
- Action parity: Legacy action shape: `launchTrinoCliWithConfigurationFile`, `ImmutableList.of`, `trimLines`, `trino.readRemainingErrorLines`, `format`, `trino.waitForWithTimeoutAndKill`, `hasMessage`. Current action shape: `SELECT`, `env.getTrinoContainer`, `execInContainer`, `describedAs`, `isZero`, `env.executeCliWithEnv`, `Map.of`, `result.getExitCode`, `result.getStdout`, `result.getStderr`, `isNotZero`, `containsIgnoringCase`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsExactly`, `assertThatThrownBy`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingErrorLines, format, trino.waitForWithTimeoutAndKill, hasMessage] vs current [env.getTrinoContainer, execInContainer, createConfig.getExitCode, describedAs, createConfig.getStdout, createConfig.getStderr, isZero, env.executeCliWithEnv, Map.of, result.getExitCode, result.getStdout, result.getStderr, isNotZero, containsIgnoringCase]; SQL verbs differ: legacy [none] vs current [CREATE, SELECT]; assertion helpers differ: legacy [assertThat, containsExactly, assertThatThrownBy] vs current [assertThat]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithConfigurationFile, ImmutableList.of, trimLines, trino.readRemainingErrorLines, format, trino.waitForWithTimeoutAndKill, hasMessage], verbs [none]. Current flow summary -> helpers [env.getTrinoContainer, execInContainer, createConfig.getExitCode, describedAs, createConfig.getStdout, createConfig.getStderr, isZero, env.executeCliWithEnv, Map.of, result.getExitCode, result.getStdout, result.getStderr, isNotZero, containsIgnoringCase], verbs [CREATE, SELECT].
- Audit status: `verified`

### `TestJmxConnectorJunit`


- Owning migration commit: `Migrate TestJmxConnector to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/TestJmxConnectorJunit.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestJmxConnector.java`
- Class-level environment requirement: `JdbcBasicEnvironment`.
- Class-level tags: `Jdbc`.
- Method inventory complete: Yes. Legacy methods: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `selectFromJavaRuntimeJmxMBean`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestJmxConnector.java` ->
  `selectFromJavaRuntimeJmxMBean`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/TestJmxConnectorJunit.java` ->
  `TestJmxConnectorJunit.selectFromJavaRuntimeJmxMBean`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `hasColumns`, `hasAnyRows`. Current action shape: `SELECT`, `env.executeTrino`, `hasColumns`, `List.of`, `hasAnyRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, hasColumns, hasAnyRows] vs current [env.executeTrino, hasColumns, List.of, hasAnyRows]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, hasColumns, hasAnyRows], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, hasColumns, List.of, hasAnyRows], verbs [SELECT].
- Audit status: `verified`

##### `selectFromJavaOperatingSystemJmxMBean`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestJmxConnector.java` ->
  `selectFromJavaOperatingSystemJmxMBean`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/TestJmxConnectorJunit.java` ->
  `TestJmxConnectorJunit.selectFromJavaOperatingSystemJmxMBean`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `hasColumns`, `hasAnyRows`. Current action shape: `SELECT`, `env.executeTrino`, `hasColumns`, `List.of`, `hasAnyRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, hasColumns, hasAnyRows] vs current [env.executeTrino, hasColumns, List.of, hasAnyRows]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, hasColumns, hasAnyRows], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, hasColumns, List.of, hasAnyRows], verbs [SELECT].
- Audit status: `verified`
