# Suite Audit: SuiteAuthorization

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Authorization coverage.
- Owning lane: `hive`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteAuthorization.java`
- CI bucket: `hive-transactional`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveKerberosImpersonationEnvironment`
- Include tags: `Authorization`.
- Exclude tags: none.
- Expected mapped classes covered: `TestGrantRevoke`, `TestRoles`, `TestSqlStandardAccessControlChecks`.
- Expected mapped methods covered: `64` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-transactional`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite identified`

## Parity Checklist

- Legacy suite or lane source: the `AUTHORIZATION` run embedded in legacy `Suite3`.
- Current suite class: `SuiteAuthorization`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `3`
- Expected migrated method count: `64`
- Expected migrated classes covered: `TestGrantRevoke`, `TestRoles`, `TestSqlStandardAccessControlChecks`.
- Expected migrated methods covered: `TestGrantRevoke.testAll`, `TestGrantRevoke.testCustomRole`,
  `TestGrantRevoke.testDropRoleWithPermissionsGranted`, `TestGrantRevoke.testGrantOptionsOnGrantedPrivilege`,
  `TestGrantRevoke.testGrantRevoke`, `TestGrantRevoke.testGrantRevokeWithGrantOption`, `TestGrantRevoke.testPublic`,
  `TestGrantRevoke.testShowGrants`, `TestGrantRevoke.testTableOwnerPrivileges`,
  `TestGrantRevoke.testTablePrivilegesWithHiveOnlyViews`, `TestGrantRevoke.testTransitiveRole`,
  `TestGrantRevoke.testViewOwnerPrivileges`, `TestRoles.testAdminCanAddColumnToAnyTable`,
  `TestRoles.testAdminCanDropAnyTable`, `TestRoles.testAdminCanRenameAnyTable`,
  `TestRoles.testAdminCanRenameColumnInAnyTable`, `TestRoles.testAdminCanShowAllGrants`,
  `TestRoles.testAdminCanShowGrantsOnlyFromCurrentSchema`, `TestRoles.testAdminRoleIsGrantedToHdfs`,
  `TestRoles.testCreateDropRoleAccessControl`, `TestRoles.testCreateDuplicateRole`, `TestRoles.testCreateRole`,
  `TestRoles.testDropGrantedRole`, `TestRoles.testDropNonExistentRole`, `TestRoles.testDropRole`,
  `TestRoles.testDropTransitiveRole`, `TestRoles.testGrantRevokeRoleAccessControl`,
  `TestRoles.testGrantRoleMultipleTimes`, `TestRoles.testGrantRoleToRole`, `TestRoles.testGrantRoleToUser` ....
- Parity status: `verified`
- Recorded differences:
  - Current suite isolates the authorization-only run on `HiveKerberosImpersonationEnvironment` instead of leaving it embedded inside the larger launcher aggregate.
