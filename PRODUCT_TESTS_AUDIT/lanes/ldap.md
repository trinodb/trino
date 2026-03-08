# Lane Audit: Ldap

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add LDAP environments`
- Section end commit: `Remove legacy SuiteLdap`
- Introduced JUnit suites: `SuiteLdap`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteLdap`.
- Environment classes introduced: `LdapEnvironment`.
- Method status counts: verified `30`, intentional difference `8`, needs follow-up `0`.

## Semantic Audit Status

- Baseline note: detailed method, environment, and suite records below are retained from earlier audit work and must not be treated as final semantic findings until this lane is marked `complete` in `PROGRESS.md`.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none at the per-method body level.

## Environment Semantic Audit
### `LdapBasicEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the effective legacy `EnvSinglenodeLdap` and the LDAP portions of `EnvSinglenodeLdapBindDn`, `EnvSinglenodeLdapInsecure`, and `EnvSinglenodeLdapReferrals` through current/legacy source plus current suite routing.
- Recorded differences:
  - Current JUnit lane consolidates the plain LDAP launcher coverage into one `LdapBasicEnvironment` class instead of separate launcher environments.
  - This consolidation is behavior-preserving for the basic HTTPS LDAP path and the multiple-bind credential path covered by the current `TestTrinoLdapCli` and `TestLdapTrinoJdbc` classes.
- Reviewer note: current `LdapBasicEnvironment` preserves the plain HTTPS LDAP coverage shape, while the restored
  insecure, referrals, and bind-DN environments cover the remaining legacy variants explicitly.

### `LdapAndFileEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy `EnvSinglenodeLdapAndFile` plus the legacy `LDAP_AND_FILE` method bodies embedded in `TestTrinoLdapCli` and `TestLdapTrinoJdbc`.
- Recorded differences:
  - Current JUnit environment still configures both LDAP and file password authenticators.
  - Legacy launcher suite also reran the base `LDAP` and `LDAP_CLI` groups on the combined LDAP+file environment; current suite does not rerun the base LDAP classes there.
- Reviewer note: the dedicated file-authenticator methods remain faithful, and the broader legacy cross-environment
  rerun is now restored through `LdapAndFileRerunEnvironment`.

### `LdapEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the legacy LDAP launcher environment family and the current shared base environment class.
- Recorded differences:
  - Current base environment centralizes LDAP server + Trino HTTPS setup in one Testcontainers environment superclass instead of multiple launcher environment definitions.
  - The runtime model changed from launcher-managed Docker environments to direct Testcontainers-managed services, which is part of the approved framework replacement.
- Reviewer note: base LDAP topology and credentials setup are represented, and every legacy launcher variant now has a
  corresponding current environment or rerun path.

## Suite Semantic Audit
### `SuiteLdap`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against launcher `SuiteLdap` plus current `SuiteLdap` source.
- Recorded differences:
  - Legacy launcher suite had five environment runs: `EnvSinglenodeLdap`, `EnvSinglenodeLdapAndFile`, `EnvSinglenodeLdapInsecure`, `EnvSinglenodeLdapReferrals`, and `EnvSinglenodeLdapBindDn`.
  - Current JUnit suite now has six runs: `LdapBasicEnvironment`, `LdapAndFileEnvironment`,
    `LdapAndFileRerunEnvironment`, `LdapInsecureEnvironment`, `LdapReferralsEnvironment`, and
    `LdapBindDnEnvironment`.
  - `LdapAndFileRerunEnvironment` restores the legacy rerun of the base `LDAP` and `LDAP_CLI` groups under the
    combined authenticator environment, while `LdapBindDnEnvironment` excludes `LdapMultipleBinds` to match the legacy
    launcher suite.
- Reviewer note: per-method ports are present and the legacy suite/environment breadth is now restored.

## Ported Test Classes

### `TestTrinoLdapCli`


- Owning migration commit: `Migrate TestTrinoLdapCli to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java`
- Class-level environment requirement: `LdapBasicEnvironment`.
- Class-level tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `19`. Current methods: `17`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileCli.java` ->
  `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`; `shouldRunQueryWithFileAuthenticator` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileCli.java` ->
  `shouldRunQueryWithFileAuthenticator`.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `shouldRunQueryWithLdap`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldRunQueryWithLdap`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldRunQueryWithLdap`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trino.waitForPrompt`, `trino.getProcessInput`, `println`, `trimLines`, `trino.readLinesUntilPrompt`, `containsAll`. Current action shape: `env.executeCli`, `env.getDefaultUser`, `env.getDefaultPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trimLines, trino.readLinesUntilPrompt, containsAll] vs current [env.executeCli, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isZero, result.getStdout]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trino.waitForPrompt, trino.getProcessInput, println, trimLines, trino.readLinesUntilPrompt, containsAll], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `verified`

##### `shouldRunBatchQueryWithLdap`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldRunBatchQueryWithLdap`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldRunBatchQueryWithLdap`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`. Current action shape: `env.executeCli`, `env.getDefaultUser`, `env.getDefaultPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll] vs current [env.executeCli, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isZero, result.getStdout]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `verified`

##### `shouldPassQueryForLdapUserInMultipleGroups`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldPassQueryForLdapUserInMultipleGroups`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldPassQueryForLdapUserInMultipleGroups`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `USER_IN_MULTIPLE_GROUPS.getAttributes`, `get`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`. Current action shape: `env.executeCli`, `env.getUserInMultipleGroups`, `env.getLdapPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [USER_IN_MULTIPLE_GROUPS.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll] vs current [env.executeCli, env.getUserInMultipleGroups, env.getLdapPassword, result.getExitCode, isZero, result.getStdout]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [USER_IN_MULTIPLE_GROUPS.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getUserInMultipleGroups, env.getLdapPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `verified`

##### `shouldPassQueryForAlternativeLdapBind`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldPassQueryForAlternativeLdapBind`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldPassQueryForAlternativeLdapBind`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `LdapMultipleBinds`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `USER_IN_AMERICA.getAttributes`, `get`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`. Current action shape: `env.executeCli`, `env.getUserInAmerica`, `env.getLdapPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [USER_IN_AMERICA.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll] vs current [env.executeCli, env.getUserInAmerica, env.getLdapPassword, result.getExitCode, isZero, result.getStdout]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [USER_IN_AMERICA.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getUserInAmerica, env.getLdapPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForLdapUserInChildGroup`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForLdapUserInChildGroup`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForLdapUserInChildGroup`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `CHILD_GROUP_USER.getAttributes`, `get`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`, `format`. Current action shape: `env.executeCli`, `env.getChildGroupUser`, `env.getLdapPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [CHILD_GROUP_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, format] vs current [env.executeCli, env.getChildGroupUser, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr, format]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [CHILD_GROUP_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, format], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getChildGroupUser, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr, format], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForLdapUserInParentGroup`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForLdapUserInParentGroup`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForLdapUserInParentGroup`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `PARENT_GROUP_USER.getAttributes`, `get`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`, `format`. Current action shape: `env.executeCli`, `env.getParentGroupUser`, `env.getLdapPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [PARENT_GROUP_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, format] vs current [env.executeCli, env.getParentGroupUser, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr, format]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [PARENT_GROUP_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, format], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getParentGroupUser, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr, format], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForOrphanLdapUser`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForOrphanLdapUser`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForOrphanLdapUser`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `ORPHAN_USER.getAttributes`, `get`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`, `format`. Current action shape: `env.executeCli`, `env.getOrphanUser`, `env.getLdapPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [ORPHAN_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, format] vs current [env.executeCli, env.getOrphanUser, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr, format]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [ORPHAN_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, format], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getOrphanUser, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr, format], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForFailedBind`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForFailedBind`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForFailedBind`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `LdapMultipleBinds`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `USER_IN_EUROPE.getAttributes`, `get`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`. Current action shape: `env.executeCli`, `env.getUserInEurope`, `env.getLdapPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [USER_IN_EUROPE.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy] vs current [env.executeCli, env.getUserInEurope, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [USER_IN_EUROPE.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getUserInEurope, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForWrongLdapPassword`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForWrongLdapPassword`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForWrongLdapPassword`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`. Current action shape: `env.executeCli`, `env.getDefaultUser`, `result.getExitCode`, `isNotZero`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy] vs current [env.executeCli, env.getDefaultUser, result.getExitCode, isNotZero, result.getStderr]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getDefaultUser, result.getExitCode, isNotZero, result.getStderr], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForWrongLdapUser`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForWrongLdapUser`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForWrongLdapUser`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`. Current action shape: `env.executeCli`, `env.getLdapPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy] vs current [env.executeCli, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForEmptyUser`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForEmptyUser`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForEmptyUser`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`. Current action shape: `env.executeCli`, `env.getLdapPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy] vs current [env.executeCli, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForLdapWithoutPassword`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForLdapWithoutPassword`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForLdapWithoutPassword`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCli`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`. Current action shape: `env.executeCliWithoutPassword`, `env.getDefaultUser`, `result.getExitCode`, `isNotZero`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCli, trimLines, trino.readRemainingErrorLines, anySatisfy] vs current [env.executeCliWithoutPassword, env.getDefaultUser, result.getExitCode, isNotZero, result.getStderr]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCli, trimLines, trino.readRemainingErrorLines, anySatisfy], verbs [none]. Current flow summary -> helpers [env.executeCliWithoutPassword, env.getDefaultUser, result.getExitCode, isNotZero, result.getStderr], verbs [none].
- Audit status: `verified`

##### `shouldPassForCredentialsWithSpecialCharacters`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldPassForCredentialsWithSpecialCharacters`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldPassForCredentialsWithSpecialCharacters`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SPECIAL_USER.getAttributes`, `get`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`. Current action shape: `env.executeCli`, `env.getSpecialUser`, `env.getSpecialUserPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [SPECIAL_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll] vs current [env.executeCli, env.getSpecialUser, env.getSpecialUserPassword, result.getExitCode, isZero, result.getStdout]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [SPECIAL_USER.getAttributes, get, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingOutputLines, containsAll], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getSpecialUser, env.getSpecialUserPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `verified`

##### `shouldFailForUserWithColon`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailForUserWithColon`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailForUserWithColon`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`, `skipAfterMethodWithContext`. Current action shape: `env.executeCli`, `env.getLdapPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, skipAfterMethodWithContext] vs current [env.executeCli, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, skipAfterMethodWithContext], verbs [none]. Current flow summary -> helpers [env.executeCli, env.getLdapPassword, result.getExitCode, isNotZero, result.getStderr], verbs [none].
- Audit status: `verified`

##### `shouldRunQueryFromFileWithLdap`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldRunQueryFromFileWithLdap`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldRunQueryFromFileWithLdap`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `File.createTempFile`. Current setup shape: `createFile.getExitCode`.
- Action parity: Legacy action shape: `Files.writeString`, `temporayFile.toPath`, `launchTrinoCliWithServerArgument`, `temporayFile.getAbsolutePath`, `trimLines`, `trino.readRemainingOutputLines`, `containsAll`. Current action shape: `SELECT`, `env.getTrinoContainer`, `execInContainer`, `count`, `isZero`, `env.executeCli`, `env.getDefaultUser`, `env.getDefaultPassword`, `result.getExitCode`, `result.getStdout`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `temporayFile.deleteOnExit`. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [File.createTempFile, temporayFile.deleteOnExit, Files.writeString, temporayFile.toPath, launchTrinoCliWithServerArgument, temporayFile.getAbsolutePath, trimLines, trino.readRemainingOutputLines, containsAll] vs current [env.getTrinoContainer, execInContainer, count, createFile.getExitCode, isZero, env.executeCli, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, result.getStdout]; SQL verbs differ: legacy [none] vs current [SELECT]; assertion helpers differ: legacy [assertThat] vs current [assertThat, contains]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [File.createTempFile, temporayFile.deleteOnExit, Files.writeString, temporayFile.toPath, launchTrinoCliWithServerArgument, temporayFile.getAbsolutePath, trimLines, trino.readRemainingOutputLines, containsAll], verbs [none]. Current flow summary -> helpers [env.getTrinoContainer, execInContainer, count, createFile.getExitCode, isZero, env.executeCli, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, result.getStdout], verbs [SELECT].
- Audit status: `verified`

##### `shouldFailQueryForLdapWithoutHttps`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailQueryForLdapWithoutHttps`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailQueryForLdapWithoutHttps`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `format`, `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`, `skipAfterMethodWithContext`. Current action shape: `HTTP`, `env.executeCliWithHttpUrl`, `env.getDefaultUser`, `env.getDefaultPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`, `containsAnyOf`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [format, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, skipAfterMethodWithContext] vs current [HTTP, env.executeCliWithHttpUrl, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isNotZero, result.getStderr, containsAnyOf]; assertion helpers differ: legacy [assertThat, contains] vs current [assertThat]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [format, launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, skipAfterMethodWithContext], verbs [none]. Current flow summary -> helpers [HTTP, env.executeCliWithHttpUrl, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isNotZero, result.getStderr, containsAnyOf], verbs [none].
- Audit status: `verified`

##### `shouldFailForIncorrectTrustStore`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldFailForIncorrectTrustStore`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestTrinoLdapCli.java` ->
  `TestTrinoLdapCli.shouldFailForIncorrectTrustStore`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapCli`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `launchTrinoCliWithServerArgument`, `trimLines`, `trino.readRemainingErrorLines`, `anySatisfy`, `skipAfterMethodWithContext`. Current action shape: `env.executeCliWithWrongTruststorePassword`, `env.getDefaultUser`, `env.getDefaultPassword`, `result.getExitCode`, `isNotZero`, `result.getStderr`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, skipAfterMethodWithContext] vs current [env.executeCliWithWrongTruststorePassword, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isNotZero, result.getStderr]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [launchTrinoCliWithServerArgument, trimLines, trino.readRemainingErrorLines, anySatisfy, skipAfterMethodWithContext], verbs [none]. Current flow summary -> helpers [env.executeCliWithWrongTruststorePassword, env.getDefaultUser, env.getDefaultPassword, result.getExitCode, isNotZero, result.getStderr], verbs [none].
- Audit status: `verified`

### `TestLdapAndFileCli`


- Owning migration commit: `Add TestLdapAndFileCli JUnit coverage`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileCli.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `LdapAndFileEnvironment`.
- Class-level tags: `LdapAndFile`, `LdapAndFileCli`, `ProfileSpecificTests`.
- Method inventory complete: Not applicable. No legacy class or resource source exists for this new verification
  coverage.
- Legacy helper/resource dependencies accounted for: New JUnit-side verification coverage without a removed legacy
  counterpart.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods

##### `shouldRunQueryWithLdapAuthenticator`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileCli.java` ->
  `TestLdapAndFileCli.shouldRunQueryWithLdapAuthenticator`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `LdapAndFileCli`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `env.executeCli`, `env.getDefaultUser`, `env.getLdapPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.executeCli, env.getDefaultUser, env.getLdapPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `intentional difference`

##### `shouldRunQueryWithFileAuthenticator`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldRunQueryWithFileAuthenticator`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileCli.java` ->
  `TestLdapAndFileCli.shouldRunQueryWithFileAuthenticator`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `LdapAndFileCli`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `authenticate`, `env.executeCli`, `env.getDefaultUser`, `env.getFileUserPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [authenticate, env.executeCli, env.getDefaultUser, env.getFileUserPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `intentional difference`

##### `shouldRunQueryForFileOnlyUser`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileCli.java` ->
  `TestLdapAndFileCli.shouldRunQueryForFileOnlyUser`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `LdapAndFileCli`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `user`, `env.executeCli`, `env.getOnlyFileUser`, `env.getOnlyFileUserPassword`, `result.getExitCode`, `isZero`, `result.getStdout`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [user, env.executeCli, env.getOnlyFileUser, env.getOnlyFileUserPassword, result.getExitCode, isZero, result.getStdout], verbs [none].
- Audit status: `intentional difference`

##### `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/cli/TestTrinoLdapCli.java` ->
  `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileCli.java` ->
  `TestLdapAndFileCli.shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `LdapAndFileCli`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `shouldRunQueryForFileOnlyUser`.
- Assertion parity: No legacy counterpart. Current assertion helpers: none.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [shouldRunQueryForFileOnlyUser], verbs [none].
- Audit status: `intentional difference`

### `TestLdapAndFileTrinoJdbc`


- Owning migration commit: `Add TestLdapAndFileTrinoJdbc JUnit coverage`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileTrinoJdbc.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `LdapAndFileEnvironment`.
- Class-level tags: `LdapAndFile`, `ProfileSpecificTests`, `TrinoJdbc`.
- Method inventory complete: Not applicable. No legacy class or resource source exists for this new verification
  coverage.
- Legacy helper/resource dependencies accounted for: New JUnit-side verification coverage without a removed legacy
  counterpart.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods

##### `shouldRunQueryWithLdapAuthenticator`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileTrinoJdbc.java` ->
  `TestLdapAndFileTrinoJdbc.shouldRunQueryWithLdapAuthenticator`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTrinoConnection`, `conn.createStatement`, `QueryResult.forResultSet`.
- Action parity: No legacy counterpart. Current action shape: `env.getDefaultUser`, `env.getLdapPassword`, `stmt.executeQuery`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [env.createTrinoConnection, env.getDefaultUser, env.getLdapPassword, conn.createStatement, stmt.executeQuery, QueryResult.forResultSet], verbs [none].
- Audit status: `intentional difference`

##### `shouldRunQueryWithFileAuthenticator`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldRunQueryWithFileAuthenticator`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileTrinoJdbc.java` ->
  `TestLdapAndFileTrinoJdbc.shouldRunQueryWithFileAuthenticator`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTrinoConnection`, `conn.createStatement`, `QueryResult.forResultSet`.
- Action parity: No legacy counterpart. Current action shape: `authenticate`, `env.getDefaultUser`, `env.getFileUserPassword`, `stmt.executeQuery`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [authenticate, env.createTrinoConnection, env.getDefaultUser, env.getFileUserPassword, conn.createStatement, stmt.executeQuery, QueryResult.forResultSet], verbs [none].
- Audit status: `intentional difference`

##### `shouldRunQueryForFileOnlyUser`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileTrinoJdbc.java` ->
  `TestLdapAndFileTrinoJdbc.shouldRunQueryForFileOnlyUser`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: No legacy counterpart. Current setup shape: `env.createTrinoConnection`, `conn.createStatement`, `QueryResult.forResultSet`.
- Action parity: No legacy counterpart. Current action shape: `user`, `env.getOnlyFileUser`, `env.getOnlyFileUserPassword`, `stmt.executeQuery`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [user, env.createTrinoConnection, env.getOnlyFileUser, env.getOnlyFileUserPassword, conn.createStatement, stmt.executeQuery, QueryResult.forResultSet], verbs [none].
- Audit status: `intentional difference`

##### `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileTrinoJdbc.java` ->
  `TestLdapAndFileTrinoJdbc.shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `LdapAndFileEnvironment`. Routed by source review into `SuiteLdap` run 2.
- Tag parity: Current tags: `LdapAndFile`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: No legacy counterpart. Current setup shape: none.
- Action parity: No legacy counterpart. Current action shape: `shouldRunQueryForFileOnlyUser`.
- Assertion parity: No legacy counterpart. Current assertion helpers: none.
- Cleanup parity: No legacy counterpart. Current cleanup shape: none.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [shouldRunQueryForFileOnlyUser], verbs [none].
- Audit status: `intentional difference`

### `TestLdapTrinoJdbc`


- Owning migration commit: `Migrate TestLdapTrinoJdbc to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java`
- Class-level environment requirement: `LdapBasicEnvironment`.
- Class-level tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`.
- Method inventory complete: Yes. Legacy methods: `15`. Current methods: `13`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileTrinoJdbc.java` ->
  `shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`; `shouldRunQueryWithFileAuthenticator` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapAndFileTrinoJdbc.java` ->
  `shouldRunQueryWithFileAuthenticator`.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `shouldRunQueryWithLdap`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldRunQueryWithLdap`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldRunQueryWithLdap`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTrinoConnection`, `conn.createStatement`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `executeLdapQuery`, `matches`. Current action shape: `stmt.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [executeLdapQuery, matches] vs current [env.createTrinoConnection, conn.createStatement, stmt.executeQuery, QueryResult.forResultSet]; assertion helpers differ: legacy [assertThat] vs current [assertThat, containsOnly]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [executeLdapQuery, matches], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, conn.createStatement, stmt.executeQuery, QueryResult.forResultSet], verbs [none].
- Audit status: `verified`

##### `shouldRunQueryWithAlternativeBind`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldRunQueryWithAlternativeBind`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldRunQueryWithAlternativeBind`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapMultipleBinds`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTrinoConnection`, `conn.createStatement`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `USER_IN_AMERICA.getAttributes`, `get`, `executeLdapQuery`, `matches`. Current action shape: `env.getUserInAmerica`, `env.getLdapPassword`, `stmt.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [USER_IN_AMERICA.getAttributes, get, executeLdapQuery, matches] vs current [env.createTrinoConnection, env.getUserInAmerica, env.getLdapPassword, conn.createStatement, stmt.executeQuery, QueryResult.forResultSet]; assertion helpers differ: legacy [assertThat] vs current [assertThat, containsOnly]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [USER_IN_AMERICA.getAttributes, get, executeLdapQuery, matches], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, env.getUserInAmerica, env.getLdapPassword, conn.createStatement, stmt.executeQuery, QueryResult.forResultSet], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForLdapUserInChildGroup`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForLdapUserInChildGroup`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForLdapUserInChildGroup`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `CHILD_GROUP_USER.getAttributes`, `get`, `expectQueryToFailForUserNotInGroup`. Current action shape: `expectQueryToFailForUserNotInGroup`, `env.getChildGroupUser`. Legacy delegate calls: `CHILD_GROUP_USER.getAttributes`, `get`, `expectQueryToFailForUserNotInGroup`. Current delegate calls: `expectQueryToFailForUserNotInGroup`, `env.getChildGroupUser`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [CHILD_GROUP_USER.getAttributes, get, expectQueryToFailForUserNotInGroup] vs current [expectQueryToFailForUserNotInGroup, env.getChildGroupUser]; helper calls differ: legacy [CHILD_GROUP_USER.getAttributes, get, expectQueryToFailForUserNotInGroup] vs current [expectQueryToFailForUserNotInGroup, env.getChildGroupUser]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [CHILD_GROUP_USER.getAttributes, get, expectQueryToFailForUserNotInGroup], verbs [none]. Current flow summary -> helpers [expectQueryToFailForUserNotInGroup, env.getChildGroupUser], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForLdapUserInParentGroup`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForLdapUserInParentGroup`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForLdapUserInParentGroup`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `PARENT_GROUP_USER.getAttributes`, `get`, `expectQueryToFailForUserNotInGroup`. Current action shape: `expectQueryToFailForUserNotInGroup`, `env.getParentGroupUser`. Legacy delegate calls: `PARENT_GROUP_USER.getAttributes`, `get`, `expectQueryToFailForUserNotInGroup`. Current delegate calls: `expectQueryToFailForUserNotInGroup`, `env.getParentGroupUser`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [PARENT_GROUP_USER.getAttributes, get, expectQueryToFailForUserNotInGroup] vs current [expectQueryToFailForUserNotInGroup, env.getParentGroupUser]; helper calls differ: legacy [PARENT_GROUP_USER.getAttributes, get, expectQueryToFailForUserNotInGroup] vs current [expectQueryToFailForUserNotInGroup, env.getParentGroupUser]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [PARENT_GROUP_USER.getAttributes, get, expectQueryToFailForUserNotInGroup], verbs [none]. Current flow summary -> helpers [expectQueryToFailForUserNotInGroup, env.getParentGroupUser], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForOrphanLdapUser`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForOrphanLdapUser`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForOrphanLdapUser`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `ORPHAN_USER.getAttributes`, `get`, `expectQueryToFailForUserNotInGroup`. Current action shape: `expectQueryToFailForUserNotInGroup`, `env.getOrphanUser`. Legacy delegate calls: `ORPHAN_USER.getAttributes`, `get`, `expectQueryToFailForUserNotInGroup`. Current delegate calls: `expectQueryToFailForUserNotInGroup`, `env.getOrphanUser`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [ORPHAN_USER.getAttributes, get, expectQueryToFailForUserNotInGroup] vs current [expectQueryToFailForUserNotInGroup, env.getOrphanUser]; helper calls differ: legacy [ORPHAN_USER.getAttributes, get, expectQueryToFailForUserNotInGroup] vs current [expectQueryToFailForUserNotInGroup, env.getOrphanUser]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [ORPHAN_USER.getAttributes, get, expectQueryToFailForUserNotInGroup], verbs [none]. Current flow summary -> helpers [expectQueryToFailForUserNotInGroup, env.getOrphanUser], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForFailedBind`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForFailedBind`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForFailedBind`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `LdapMultipleBinds`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `USER_IN_EUROPE.getAttributes`, `get`, `expectQueryToFail`. Current action shape: `expectQueryToFail`, `env.getUserInEurope`, `env.getLdapPassword`. Legacy delegate calls: `USER_IN_EUROPE.getAttributes`, `get`, `expectQueryToFail`. Current delegate calls: `expectQueryToFail`, `env.getUserInEurope`, `env.getLdapPassword`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [USER_IN_EUROPE.getAttributes, get, expectQueryToFail] vs current [expectQueryToFail, env.getUserInEurope, env.getLdapPassword]; helper calls differ: legacy [USER_IN_EUROPE.getAttributes, get, expectQueryToFail] vs current [expectQueryToFail, env.getUserInEurope, env.getLdapPassword]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [USER_IN_EUROPE.getAttributes, get, expectQueryToFail], verbs [none]. Current flow summary -> helpers [expectQueryToFail, env.getUserInEurope, env.getLdapPassword], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForWrongLdapPassword`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForWrongLdapPassword`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForWrongLdapPassword`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `expectQueryToFail`. Current action shape: `expectQueryToFail`, `env.getDefaultUser`. Legacy delegate calls: `expectQueryToFail`. Current delegate calls: `expectQueryToFail`, `env.getDefaultUser`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getDefaultUser]; helper calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getDefaultUser]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [expectQueryToFail], verbs [none]. Current flow summary -> helpers [expectQueryToFail, env.getDefaultUser], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForWrongLdapUser`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForWrongLdapUser`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForWrongLdapUser`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `executeLdapQuery`, `hasMessageStartingWith`. Current action shape: `executeLdapQuery`, `env.getLdapPassword`, `hasMessageStartingWith`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `isInstanceOf`. Current assertion helpers: `assertThatThrownBy`, `isInstanceOf`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [executeLdapQuery, hasMessageStartingWith] vs current [executeLdapQuery, env.getLdapPassword, hasMessageStartingWith]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [executeLdapQuery, hasMessageStartingWith], verbs [none]. Current flow summary -> helpers [executeLdapQuery, env.getLdapPassword, hasMessageStartingWith], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForEmptyUser`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForEmptyUser`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForEmptyUser`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `expectQueryToFail`. Current action shape: `expectQueryToFail`, `env.getLdapPassword`. Legacy delegate calls: `expectQueryToFail`. Current delegate calls: `expectQueryToFail`, `env.getLdapPassword`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getLdapPassword]; helper calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getLdapPassword]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [expectQueryToFail], verbs [none]. Current flow summary -> helpers [expectQueryToFail, env.getLdapPassword], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForLdapWithoutPassword`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForLdapWithoutPassword`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForLdapWithoutPassword`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `expectQueryToFail`. Current action shape: `expectQueryToFail`, `env.getDefaultUser`. Legacy delegate calls: `expectQueryToFail`. Current delegate calls: `expectQueryToFail`, `env.getDefaultUser`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getDefaultUser]; helper calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getDefaultUser]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [expectQueryToFail], verbs [none]. Current flow summary -> helpers [expectQueryToFail, env.getDefaultUser], verbs [none].
- Audit status: `verified`

##### `shouldFailQueryForLdapWithoutSsl`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailQueryForLdapWithoutSsl`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailQueryForLdapWithoutSsl`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `DriverManager.getConnection`, `trinoServer`. Current action shape: `format`, `env.getTrinoHost`, `env.getTrinoPort`, `DriverManager.getConnection`, `env.getDefaultUser`, `env.getLdapPassword`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [DriverManager.getConnection, trinoServer] vs current [format, env.getTrinoHost, env.getTrinoPort, DriverManager.getConnection, env.getDefaultUser, env.getLdapPassword]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [DriverManager.getConnection, trinoServer], verbs [none]. Current flow summary -> helpers [format, env.getTrinoHost, env.getTrinoPort, DriverManager.getConnection, env.getDefaultUser, env.getLdapPassword], verbs [none].
- Audit status: `verified`

##### `shouldFailForIncorrectTrustStore`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailForIncorrectTrustStore`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailForIncorrectTrustStore`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `format`, `trinoServer`, `DriverManager.getConnection`. Current action shape: `format`, `env.getTrinoHost`, `env.getTrinoPort`, `env.getTruststorePath`, `DriverManager.getConnection`, `env.getDefaultUser`, `env.getLdapPassword`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [format, trinoServer, DriverManager.getConnection] vs current [format, env.getTrinoHost, env.getTrinoPort, env.getTruststorePath, DriverManager.getConnection, env.getDefaultUser, env.getLdapPassword]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [format, trinoServer, DriverManager.getConnection], verbs [none]. Current flow summary -> helpers [format, env.getTrinoHost, env.getTrinoPort, env.getTruststorePath, DriverManager.getConnection, env.getDefaultUser, env.getLdapPassword], verbs [none].
- Audit status: `verified`

##### `shouldFailForUserWithColon`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestLdapTrinoJdbc.java` ->
  `shouldFailForUserWithColon`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ldap/TestLdapTrinoJdbc.java` ->
  `TestLdapTrinoJdbc.shouldFailForUserWithColon`
- Mapping type: `direct`
- Environment parity: Current class requires `LdapBasicEnvironment`. Routed by source review into `SuiteLdap` run 1.
- Tag parity: Current tags: `Ldap`, `ProfileSpecificTests`, `TrinoJdbc`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `expectQueryToFail`. Current action shape: `expectQueryToFail`, `env.getLdapPassword`. Legacy delegate calls: `expectQueryToFail`. Current delegate calls: `expectQueryToFail`, `env.getLdapPassword`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getLdapPassword]; helper calls differ: legacy [expectQueryToFail] vs current [expectQueryToFail, env.getLdapPassword]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [expectQueryToFail], verbs [none]. Current flow summary -> helpers [expectQueryToFail, env.getLdapPassword], verbs [none].
- Audit status: `verified`
