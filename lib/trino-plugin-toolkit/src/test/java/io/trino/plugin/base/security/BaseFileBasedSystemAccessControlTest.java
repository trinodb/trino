/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.io.Files.copy;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.FunctionKind.TABLE;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Files.newTemporaryFile;
import static org.testng.Assert.assertEquals;

public abstract class BaseFileBasedSystemAccessControlTest
{
    private static final Identity alice = Identity.forUser("alice").withGroups(ImmutableSet.of("staff")).build();
    private static final Identity kerberosValidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidNonAsciiUser = Identity.forUser("\u0194\u0194\u0194").withPrincipal(new KerberosPrincipal("\u0194\u0194\u0194/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInvalidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("valid/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("invalid/example.com@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexWildDot = Identity.forUser(".*").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexEndQuote = Identity.forUser("\\E").withPrincipal(new KerberosPrincipal("special/\\E@EXAMPLE.COM")).build();
    private static final Identity invalidSpecialRegex = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity bob = Identity.forUser("bob").withGroups(ImmutableSet.of("staff")).build();
    private static final Identity admin = Identity.forUser("alberto").withEnabledRoles(ImmutableSet.of("admin")).withGroups(ImmutableSet.of("staff")).build();
    private static final Identity nonAsciiUser = Identity.ofUser("\u0194\u0194\u0194");
    private static final CatalogSchemaTableName aliceView = new CatalogSchemaTableName("alice-catalog", "schema", "view");
    private static final Optional<QueryId> queryId = Optional.empty();

    private static final Identity charlie = Identity.forUser("charlie").withGroups(ImmutableSet.of("guests")).build();
    private static final Identity dave = Identity.forUser("dave").withGroups(ImmutableSet.of("contractors")).build();
    private static final Identity joe = Identity.ofUser("joe");
    private static final Identity any = Identity.ofUser("any");
    private static final Identity anyone = Identity.ofUser("anyone");
    private static final SystemSecurityContext ADMIN = new SystemSecurityContext(admin, queryId);
    private static final SystemSecurityContext BOB = new SystemSecurityContext(bob, queryId);
    private static final SystemSecurityContext CHARLIE = new SystemSecurityContext(charlie, queryId);
    private static final SystemSecurityContext ALICE = new SystemSecurityContext(alice, queryId);
    private static final SystemSecurityContext JOE = new SystemSecurityContext(joe, queryId);
    private static final SystemSecurityContext UNKNOWN = new SystemSecurityContext(Identity.ofUser("some-unknown-user-id"), queryId);

    private static final String SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE = "Cannot show schemas";
    private static final String CREATE_SCHEMA_ACCESS_DENIED_MESSAGE = "Cannot create schema .*";
    private static final String DROP_SCHEMA_ACCESS_DENIED_MESSAGE = "Cannot drop schema .*";
    private static final String RENAME_SCHEMA_ACCESS_DENIED_MESSAGE = "Cannot rename schema from .* to .*";
    private static final String SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE = "Cannot show create schema for .*";
    private static final String GRANT_SCHEMA_ACCESS_DENIED_MESSAGE = "Cannot grant privilege %s on schema %s%s";
    private static final String DENY_SCHEMA_ACCESS_DENIED_MESSAGE = "Cannot deny privilege %s on schema %s%s";
    private static final String REVOKE_SCHEMA_ACCESS_DENIED_MESSAGE = "Cannot revoke privilege %s on schema %s%s";

    private static final String SHOWN_TABLES_ACCESS_DENIED_MESSAGE = "Cannot show tables of .*";
    private static final String SELECT_TABLE_ACCESS_DENIED_MESSAGE = "Cannot select from table .*";
    private static final String SHOW_COLUMNS_ACCESS_DENIED_MESSAGE = "Cannot show columns of table .*";
    private static final String ADD_COLUMNS_ACCESS_DENIED_MESSAGE = "Cannot add a column to table .*";
    private static final String DROP_COLUMNS_ACCESS_DENIED_MESSAGE = "Cannot drop a column from table .*";
    private static final String RENAME_COLUMNS_ACCESS_DENIED_MESSAGE = "Cannot rename a column in table .*";
    private static final String TABLE_COMMENT_ACCESS_DENIED_MESSAGE = "Cannot comment table to .*";
    private static final String INSERT_TABLE_ACCESS_DENIED_MESSAGE = "Cannot insert into table .*";
    private static final String DELETE_TABLE_ACCESS_DENIED_MESSAGE = "Cannot delete from table .*";
    private static final String TRUNCATE_TABLE_ACCESS_DENIED_MESSAGE = "Cannot truncate table .*";
    private static final String DROP_TABLE_ACCESS_DENIED_MESSAGE = "Cannot drop table .*";
    private static final String CREATE_TABLE_ACCESS_DENIED_MESSAGE = "Cannot show create table for .*";
    private static final String RENAME_TABLE_ACCESS_DENIED_MESSAGE = "Cannot rename table .*";
    private static final String SET_TABLE_PROPERTIES_ACCESS_DENIED_MESSAGE = "Cannot set table properties to .*";
    private static final String CREATE_VIEW_ACCESS_DENIED_MESSAGE = "View owner '.*' cannot create view that selects from .*";
    private static final String CREATE_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE = "Cannot create materialized view .*";
    private static final String DROP_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE = "Cannot drop materialized view .*";
    private static final String REFRESH_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE = "Cannot refresh materialized view .*";
    private static final String SET_MATERIALIZED_VIEW_PROPERTIES_ACCESS_DENIED_MESSAGE = "Cannot set properties of materialized view .*";
    private static final String GRANT_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE = "Cannot grant privilege DELETE on table .*";
    private static final String DENY_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE = "Cannot deny privilege DELETE on table .*";
    private static final String REVOKE_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE = "Cannot revoke privilege DELETE on table .*";

    private static final String SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE = "Cannot set system session property .*";
    private static final String SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE = "Cannot set catalog session property .*";
    private static final String EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE = "Cannot execute function .*";
    private static final String GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE = ".* cannot grant .*";

    protected abstract SystemAccessControl newFileBasedSystemAccessControl(File configFile, Map<String, String> properties);

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(SystemAccessControl.class, FileBasedSystemAccessControl.class);
    }

    @Test
    public void testRefreshing()
            throws Exception
    {
        File configFile = newTemporaryFile();
        configFile.deleteOnExit();
        copy(new File(getResourcePath("file-based-system-catalog.json")), configFile);

        SystemAccessControl accessControl = newFileBasedSystemAccessControl(configFile, ImmutableMap.of(
                "security.refresh-period", "1ms"));

        SystemSecurityContext alice = new SystemSecurityContext(BaseFileBasedSystemAccessControlTest.alice, queryId);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);

        copy(new File(getResourcePath("file-based-system-security-config-file-with-unknown-rules.json")), configFile);
        sleep(2);

        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .hasMessageContaining("Failed to convert JSON tree node");

        // test if file based cached control was not cached somewhere
        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .hasMessageContaining("Failed to convert JSON tree node");

        copy(new File(getResourcePath("file-based-system-catalog.json")), configFile);
        sleep(2);

        accessControl.checkCanCreateView(alice, aliceView);
    }

    @Test
    public void testEmptyFile()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("empty.json");

        accessControl.checkCanCreateSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"), ImmutableMap.of());
        accessControl.checkCanDropSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"));
        accessControl.checkCanRenameSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"), "new_unknown");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for schema some-catalog.unknown to ROLE some_role");
        accessControl.checkCanShowCreateSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"));

        accessControl.checkCanSelectFromColumns(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), ImmutableSet.of());
        accessControl.checkCanShowColumns(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanInsertIntoTable(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanDeleteFromTable(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanTruncateTable(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));

        accessControl.checkCanCreateTable(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), Map.of());
        accessControl.checkCanDropTable(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanTruncateTable(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanRenameTable(UNKNOWN,
                new CatalogSchemaTableName("some-catalog", "unknown", "unknown"),
                new CatalogSchemaTableName("some-catalog", "unknown", "new_unknown"));
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for table some-catalog.unknown.unknown to ROLE some_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for view some-catalog.unknown.unknown to ROLE some_role");

        accessControl.checkCanCreateMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), Map.of());
        accessControl.checkCanDropMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanRefreshMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for view some-catalog.unknown.unknown to ROLE some_role");

        accessControl.checkCanSetUser(Optional.empty(), "unknown");
        accessControl.checkCanSetUser(Optional.of(new KerberosPrincipal("stuff@example.com")), "unknown");

        accessControl.checkCanSetSystemSessionProperty(UNKNOWN, "anything");
        accessControl.checkCanSetCatalogSessionProperty(UNKNOWN, "unknown", "anything");

        accessControl.checkCanExecuteQuery(UNKNOWN);
        accessControl.checkCanViewQueryOwnedBy(UNKNOWN, anyone);
        accessControl.checkCanKillQueryOwnedBy(UNKNOWN, anyone);

        // system information access is denied by default
        assertAccessDenied(
                () -> accessControl.checkCanReadSystemInformation(UNKNOWN),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControl.checkCanWriteSystemInformation(UNKNOWN),
                "Cannot write system information");
    }

    @Test
    public void testEmptyFunctionKind()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("empty-functions-kind.json"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("functionKinds cannot be empty, provide at least one function kind [SCALAR, AGGREGATE, WINDOW, TABLE]");
    }

    @Test
    public void testDisallowFunctionKindRuleCombination()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("file-based-disallow-function-rule-combination.json"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Cannot define catalog for others function kinds than TABLE");
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("file-based-disallow-function-rule-combination-without-table.json"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Cannot define catalog for others function kinds than TABLE");
    }

    @Test
    public void testSchemaRulesForCheckCanCreateSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        Map<String, Object> properties = ImmutableMap.of();
        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"), properties);
        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"), properties);
        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"), properties);
        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"), properties);

        accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "bob"), properties);
        accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "staff"), properties);
        accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"), properties);
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "test"), properties), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"), properties);
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob"), properties), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff"), properties), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test"), properties), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testSchemaRulesForCheckCanDropSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"));
        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"));

        accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "test")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testSchemaRulesForCheckCanRenameSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"), "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"), "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"), "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"), "new_schema");

        accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "bob"), "staff");
        accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "staff"), "authenticated");
        accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"), "bob");
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "test"), "bob"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "bob"), "test"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);

        assertAccessDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob"), "new_schema"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff"), "new_schema"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"), "authenticated");
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test"), "new_schema"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testSchemaRulesForCheckCanShowCreateSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"));
        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"));

        accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "test")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test(dataProvider = "privilegeGrantOption")
    public void testGrantSchemaPrivilege(Privilege privilege, boolean grantOption)
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(USER, "alice");

        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "bob"), grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "staff"), grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "authenticated"), grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "test"), grantee, grantOption);

        accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "bob"), grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "staff"), grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "authenticated"), grantee, grantOption);
        assertAccessDenied(
                () -> accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "test"), grantee, grantOption),
                format(GRANT_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.test", ""));

        assertAccessDenied(
                () -> accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "bob"), grantee, grantOption),
                format(GRANT_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.bob", ""));
        assertAccessDenied(
                () -> accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "staff"), grantee, grantOption),
                format(GRANT_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.staff", ""));
        accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "authenticated"), grantee, grantOption);
        assertAccessDenied(
                () -> accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "test"), grantee, grantOption),
                format(GRANT_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.test", ""));
    }

    @Test
    public void testDenySchemaPrivilege()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(USER, "alice");

        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, new CatalogSchemaName("some-catalog", "bob"), grantee);
        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, new CatalogSchemaName("some-catalog", "staff"), grantee);
        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, new CatalogSchemaName("some-catalog", "authenticated"), grantee);
        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, new CatalogSchemaName("some-catalog", "test"), grantee);

        accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, new CatalogSchemaName("some-catalog", "bob"), grantee);
        accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, new CatalogSchemaName("some-catalog", "staff"), grantee);
        accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, new CatalogSchemaName("some-catalog", "authenticated"), grantee);
        assertAccessDenied(
                () -> accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, new CatalogSchemaName("some-catalog", "test"), grantee),
                format(DENY_SCHEMA_ACCESS_DENIED_MESSAGE, UPDATE, "some-catalog.test", ""));

        assertAccessDenied(
                () -> accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, new CatalogSchemaName("some-catalog", "bob"), grantee),
                format(DENY_SCHEMA_ACCESS_DENIED_MESSAGE, UPDATE, "some-catalog.bob", ""));
        assertAccessDenied(
                () -> accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, new CatalogSchemaName("some-catalog", "staff"), grantee),
                format(DENY_SCHEMA_ACCESS_DENIED_MESSAGE, UPDATE, "some-catalog.staff", ""));
        accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, new CatalogSchemaName("some-catalog", "authenticated"), grantee);
        assertAccessDenied(
                () -> accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, new CatalogSchemaName("some-catalog", "test"), grantee),
                format(DENY_SCHEMA_ACCESS_DENIED_MESSAGE, UPDATE, "some-catalog.test", ""));
    }

    @Test(dataProvider = "privilegeGrantOption")
    public void testRevokeSchemaPrivilege(Privilege privilege, boolean grantOption)
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(USER, "alice");

        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "bob"), grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "staff"), grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "authenticated"), grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, new CatalogSchemaName("some-catalog", "test"), grantee, grantOption);

        accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "bob"), grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "staff"), grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "authenticated"), grantee, grantOption);
        assertAccessDenied(
                () -> accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, new CatalogSchemaName("some-catalog", "test"), grantee, grantOption),
                format(REVOKE_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.test", ""));

        assertAccessDenied(
                () -> accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "bob"), grantee, grantOption),
                format(REVOKE_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.bob", ""));
        assertAccessDenied(
                () -> accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "staff"), grantee, grantOption),
                format(REVOKE_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.staff", ""));
        accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "authenticated"), grantee, grantOption);
        assertAccessDenied(
                () -> accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, new CatalogSchemaName("some-catalog", "test"), grantee, grantOption),
                format(REVOKE_SCHEMA_ACCESS_DENIED_MESSAGE, privilege, "some-catalog.test", ""));
    }

    @DataProvider(name = "privilegeGrantOption")
    public Object[][] privilegeGrantOption()
    {
        return EnumSet.allOf(Privilege.class)
                .stream()
                .flatMap(privilege -> Stream.of(true, false).map(grantOption -> new Object[] {privilege, grantOption}))
                .toArray(Object[][]::new);
    }

    @Test
    public void testTableRulesForCheckCanSelectFromColumns()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSelectFromColumns(ALICE, new CatalogSchemaTableName("some-catalog", "test", "test"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(ALICE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(
                ALICE,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                ImmutableSet.of("bobcolumn", "private", "restricted"));

        accessControl.checkCanSelectFromColumns(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"), ImmutableSet.of("bobcolumn"));
        assertAccessDenied(
                () -> accessControl.checkCanSelectFromColumns(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        ImmutableSet.of("bobcolumn", "private")),
                SELECT_TABLE_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanSelectFromColumns(JOE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"), ImmutableSet.of());

        assertAccessDenied(
                () -> accessControl.checkCanSelectFromColumns(
                        ADMIN,
                        new CatalogSchemaTableName("secret", "secret", "secret"),
                        ImmutableSet.of()),
                SELECT_TABLE_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(
                () -> accessControl.checkCanSelectFromColumns(
                        JOE,
                        new CatalogSchemaTableName("secret", "secret", "secret"),
                        ImmutableSet.of()),
                SELECT_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanCreateViewWithSelectFromColumns()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertAccessDenied(
                () -> accessControl.checkCanCreateViewWithSelectFromColumns(
                        ALICE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns_with_grant"),
                        ImmutableSet.of()),
                CREATE_VIEW_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanCreateViewWithSelectFromColumns(
                BOB,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns_with_grant"),
                ImmutableSet.of("bobcolumn", "private"));

        accessControl.checkCanCreateViewWithSelectFromColumns(
                CHARLIE,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns_with_grant"),
                ImmutableSet.of("bobcolumn"));
        assertAccessDenied(
                () -> accessControl.checkCanCreateViewWithSelectFromColumns(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns_with_grant"),
                        ImmutableSet.of("bobcolumn", "private")),
                SELECT_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanShowColumns()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanShowColumns(ALICE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        accessControl.checkCanShowColumns(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
    }

    @Test
    public void testTableRulesForCheckCanShowColumnsWithNoAccess()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");
        assertAccessDenied(() -> accessControl.checkCanShowColumns(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), SHOW_COLUMNS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("some-catalog", "bobschema")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testFunctionRulesForCheckExecuteAndGrantExecuteFunctionWithNoAccess()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ALICE, "some_function"), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ALICE, AGGREGATE, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ALICE, SCALAR, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ALICE, TABLE, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_table_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ALICE, WINDOW, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);

        TrinoPrincipal grantee = new TrinoPrincipal(USER, "some_user");
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, "some_function", grantee, true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, AGGREGATE, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function"), grantee, true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, SCALAR, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function"), grantee, true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, TABLE, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_table_function"), grantee, true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, WINDOW, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function"), grantee, true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForFilterColumns()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertEquals(
                accessControl.filterColumns(
                        ALICE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        ImmutableSet.of("private", "a", "restricted", "b")),
                ImmutableSet.of("private", "a", "restricted", "b"));
        assertEquals(
                accessControl.filterColumns(
                        BOB,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        ImmutableSet.of("private", "a", "restricted", "b")),
                ImmutableSet.of("private", "a", "restricted", "b"));
        assertEquals(
                accessControl.filterColumns(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        ImmutableSet.of("private", "a", "restricted", "b")),
                ImmutableSet.of("a", "b"));
    }

    @Test
    public void testTableFilter()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table-filter.json");
        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("restricted", "any"))
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .add(new SchemaTableName("bobschema", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertEquals(accessControl.filterTables(ALICE, "any", tables), ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .build());
        assertEquals(accessControl.filterTables(BOB, "any", tables), ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .build());
        assertEquals(accessControl.filterTables(ADMIN, "any", tables), ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .add(new SchemaTableName("bobschema", "any"))
                .add(new SchemaTableName("any", "any"))
                .build());
    }

    @Test
    public void testTableFilterNoAccess()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");

        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("restricted", "any"))
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertEquals(accessControl.filterTables(ALICE, "any", tables), ImmutableSet.of());
        assertEquals(accessControl.filterTables(BOB, "any", tables), ImmutableSet.of());
    }

    @Test
    public void testTableRulesForFilterColumnsWithNoAccess()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");
        assertEquals(
                accessControl.filterColumns(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), ImmutableSet.of("a")),
                ImmutableSet.of());
    }

    @Test
    public void testTableRulesForCheckCanInsertIntoTable()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");
        assertTableRulesForCheckCanInsertIntoTable(accessControl);
    }

    private static void assertTableRulesForCheckCanInsertIntoTable(SystemAccessControl accessControl)
    {
        accessControl.checkCanInsertIntoTable(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        accessControl.checkCanInsertIntoTable(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanInsertIntoTable(ALICE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), INSERT_TABLE_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanInsertIntoTable(BOB, new CatalogSchemaTableName("some-catalog", "test", "test")), INSERT_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDropTable()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDropTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanDropTable(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), DROP_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDropMaterializedView()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDropMaterializedView(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"));
        assertAccessDenied(() -> accessControl.checkCanDropMaterializedView(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view")), DROP_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanCreateMaterializedView()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanCreateMaterializedView(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"), Map.of());
        assertAccessDenied(() -> accessControl.checkCanCreateMaterializedView(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"), Map.of()), CREATE_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRefreshMaterializedView()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRefreshMaterializedView(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"));
        assertAccessDenied(() -> accessControl.checkCanRefreshMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view")), REFRESH_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanSetMaterializedViewProperties()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetMaterializedViewProperties(
                ADMIN,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"),
                ImmutableMap.of());
        accessControl.checkCanSetMaterializedViewProperties(
                ALICE,
                new CatalogSchemaTableName("some-catalog", "aliceschema", "alice-materialized-view"),
                ImmutableMap.of());
        assertAccessDenied(
                () -> accessControl.checkCanSetMaterializedViewProperties(
                        ALICE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"),
                        ImmutableMap.of()),
                SET_MATERIALIZED_VIEW_PROPERTIES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(
                () -> accessControl.checkCanSetMaterializedViewProperties(
                        BOB,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"),
                        ImmutableMap.of()),
                SET_MATERIALIZED_VIEW_PROPERTIES_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDeleteFromTable()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDeleteFromTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanDeleteFromTable(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), DELETE_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanTruncateTable()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanTruncateTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanTruncateTable(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), TRUNCATE_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanGrantTablePrivilege()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanGrantTablePrivilege(ADMIN, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false);
        assertAccessDenied(() -> accessControl.checkCanGrantTablePrivilege(BOB, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false), GRANT_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDenyTablePrivilege()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDenyTablePrivilege(ADMIN, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null);
        assertAccessDenied(() -> accessControl.checkCanDenyTablePrivilege(BOB, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null), DENY_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRevokeTablePrivilege()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRevokeTablePrivilege(ADMIN, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false);
        assertAccessDenied(() -> accessControl.checkCanRevokeTablePrivilege(BOB, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false), REVOKE_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanShowCreateTable()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanShowCreateTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanShowCreateTable(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), CREATE_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanAddColumn()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanAddColumn(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanAddColumn(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), ADD_COLUMNS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDropColumn()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDropColumn(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanDropColumn(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), DROP_COLUMNS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRenameColumn()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRenameColumn(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanRenameColumn(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), RENAME_COLUMNS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForMixedGroupUsers()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table-mixed-groups.json");

        SystemSecurityContext userGroup1Group2 = new SystemSecurityContext(Identity.forUser("user_1_2")
                .withGroups(ImmutableSet.of("group1", "group2")).build(), Optional.empty());
        SystemSecurityContext userGroup2 = new SystemSecurityContext(Identity.forUser("user_2")
                .withGroups(ImmutableSet.of("group2")).build(), Optional.empty());

        assertEquals(
                accessControl.getColumnMask(
                        userGroup1Group2,
                        new CatalogSchemaTableName("some-catalog", "my_schema", "my_table"),
                        "col_a",
                        VARCHAR),
                Optional.empty());

        assertViewExpressionEquals(
                accessControl.getColumnMask(
                        userGroup2,
                        new CatalogSchemaTableName("some-catalog", "my_schema", "my_table"),
                        "col_a",
                        VARCHAR).orElseThrow(),
                new ViewExpression(Optional.empty(), Optional.of("some-catalog"), Optional.of("my_schema"), "'mask_a'"));

        SystemSecurityContext userGroup1Group3 = new SystemSecurityContext(Identity.forUser("user_1_3")
                .withGroups(ImmutableSet.of("group1", "group3")).build(), Optional.empty());
        SystemSecurityContext userGroup3 = new SystemSecurityContext(Identity.forUser("user_3")
                .withGroups(ImmutableSet.of("group3")).build(), Optional.empty());

        assertEquals(
                accessControl.getRowFilters(
                        userGroup1Group3,
                        new CatalogSchemaTableName("some-catalog", "my_schema", "my_table")),
                ImmutableList.of());

        List<ViewExpression> rowFilters = accessControl.getRowFilters(
                userGroup3,
                new CatalogSchemaTableName("some-catalog", "my_schema", "my_table"));
        assertEquals(rowFilters.size(), 1);
        assertViewExpressionEquals(
                rowFilters.get(0),
                new ViewExpression(Optional.empty(), Optional.of("some-catalog"), Optional.of("my_schema"), "country='US'"));
    }

    @Test
    public void testTableRulesForCheckCanSetTableComment()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetTableComment(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanSetTableComment(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), TABLE_COMMENT_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRenameTable()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRenameTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), new CatalogSchemaTableName("some-catalog", "aliceschema", "newbobtable"));
        accessControl.checkCanRenameTable(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "alicetable"), new CatalogSchemaTableName("some-catalog", "aliceschema", "newalicetable"));
        assertAccessDenied(() -> accessControl.checkCanRenameTable(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), new CatalogSchemaTableName("some-catalog", "bobschema", "newbobtable")), RENAME_TABLE_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanRenameTable(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "alicetable"), new CatalogSchemaTableName("some-catalog", "bobschema", "newalicetable")), RENAME_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanSetTableProperties()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetTableProperties(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), ImmutableMap.of());
        accessControl.checkCanSetTableProperties(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "alicetable"), ImmutableMap.of());
        assertAccessDenied(() -> accessControl.checkCanSetTableProperties(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), ImmutableMap.of()), SET_TABLE_PROPERTIES_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testCanSetUserOperations()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-catalog_principal.json");

        assertAccessDenied(
                () -> accessControl.checkCanSetUser(Optional.empty(), alice.getUser()),
                "Principal null cannot become user alice");

        accessControl.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
        accessControl.checkCanSetUser(kerberosValidNonAsciiUser.getPrincipal(), kerberosValidNonAsciiUser.getUser());
        assertAccessDenied(
                () -> accessControl.checkCanSetUser(kerberosInvalidAlice.getPrincipal(), kerberosInvalidAlice.getUser()),
                "Principal mallory/example.com@EXAMPLE.COM cannot become user alice");

        accessControl.checkCanSetUser(kerberosValidShare.getPrincipal(), kerberosValidShare.getUser());
        assertAccessDenied(
                () -> accessControl.checkCanSetUser(kerberosInValidShare.getPrincipal(), kerberosInValidShare.getUser()),
                "Principal invalid/example.com@EXAMPLE.COM cannot become user alice");

        accessControl.checkCanSetUser(validSpecialRegexWildDot.getPrincipal(), validSpecialRegexWildDot.getUser());
        accessControl.checkCanSetUser(validSpecialRegexEndQuote.getPrincipal(), validSpecialRegexEndQuote.getUser());
        assertAccessDenied(
                () -> accessControl.checkCanSetUser(invalidSpecialRegex.getPrincipal(), invalidSpecialRegex.getUser()),
                "Principal special/.*@EXAMPLE.COM cannot become user alice");

        SystemAccessControl accessControlNoPatterns = newFileBasedSystemAccessControl("file-based-system-catalog.json");
        accessControlNoPatterns.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
    }

    @Test
    public void testQuery()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("query.json");

        accessControlManager.checkCanExecuteQuery(ADMIN);
        accessControlManager.checkCanViewQueryOwnedBy(ADMIN, any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(ADMIN, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(ADMIN, any);

        accessControlManager.checkCanExecuteQuery(ALICE);
        accessControlManager.checkCanViewQueryOwnedBy(ALICE, any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(ALICE, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(ALICE, any),
                "Cannot view query");

        assertAccessDenied(
                () -> accessControlManager.checkCanExecuteQuery(BOB),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(BOB, any),
                "Cannot view query");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(BOB, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(BOB, any);

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(dave, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice);
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), dave);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("bob"), Identity.ofUser("dave"), Identity.ofUser("admin"))),
                ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("dave")));
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), admin),
                "Cannot view query");

        Identity contractor = Identity.forUser("some-other-contractor").withGroups(ImmutableSet.of("contractors")).build();
        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(contractor, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave);
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave),
                "Cannot view query");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(nonAsciiUser, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), any);
    }

    @Test
    public void testInvalidQuery()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("query-invalid.json"))
                .rootCause().hasMessage("A valid query rule cannot combine an queryOwner condition with access mode 'execute'");
    }

    @Test
    public void testQueryNotSet()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("file-based-system-catalog.json");

        accessControlManager.checkCanExecuteQuery(BOB);
        accessControlManager.checkCanViewQueryOwnedBy(BOB, any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(BOB, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(BOB, any);
    }

    @Test
    public void testQueryDocsExample()
    {
        File rulesFile = new File("../../docs/src/main/sphinx/security/query-access.json");
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());

        accessControlManager.checkCanExecuteQuery(ADMIN);
        accessControlManager.checkCanViewQueryOwnedBy(ADMIN, any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(ADMIN, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(ADMIN, any);

        accessControlManager.checkCanExecuteQuery(ALICE);
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(ALICE, any),
                "Cannot view query");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(ALICE, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(ALICE, any);

        accessControlManager.checkCanExecuteQuery(BOB);
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(BOB, any),
                "Cannot view query");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(BOB, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b"))), ImmutableSet.of());
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(BOB, any),
                "Cannot view query");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(dave, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice);
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), dave);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("bob"), Identity.ofUser("dave"), Identity.ofUser("admin"))),
                ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("dave")));
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice),
                "Cannot view query");
        assertAccessDenied(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), admin),
                "Cannot view query");

        Identity contractor = Identity.forUser("some-other-contractor").withGroups(ImmutableSet.of("contractors")).build();
        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(contractor, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave);
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave),
                "Cannot view query");
    }

    @Test
    public void testSystemInformation()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("system-information.json");

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(admin, Optional.empty()));
        accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(admin, Optional.empty()));

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(alice, Optional.empty()));
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(alice, Optional.empty())),
                "Cannot write system information");

        assertAccessDenied(
                () -> accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(bob, Optional.empty())),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(bob, Optional.empty())),
                "Cannot write system information");

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(nonAsciiUser, Optional.empty()));
        accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(nonAsciiUser, Optional.empty()));
    }

    @Test
    public void testSystemInformationNotSet()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("file-based-system-catalog.json");

        assertAccessDenied(
                () -> accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(bob, Optional.empty())),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(bob, Optional.empty())),
                "Cannot write system information");
    }

    @Test
    public void testSystemInformationDocsExample()
    {
        File rulesFile = new File("../../docs/src/main/sphinx/security/system-information-access.json");
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(admin, Optional.empty()));
        accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(admin, Optional.empty()));

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(alice, Optional.empty()));
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(alice, Optional.empty())),
                "Cannot write system information");

        assertAccessDenied(
                () -> accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(bob, Optional.empty())),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(bob, Optional.empty())),
                "Cannot write system information");
    }

    @Test
    public void testSessionPropertyRules()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-session-property.json");

        accessControl.checkCanSetSystemSessionProperty(ADMIN, "dangerous");
        accessControl.checkCanSetSystemSessionProperty(ADMIN, "any");
        accessControl.checkCanSetSystemSessionProperty(ALICE, "safe");
        accessControl.checkCanSetSystemSessionProperty(ALICE, "unsafe");
        accessControl.checkCanSetSystemSessionProperty(ALICE, "staff");
        accessControl.checkCanSetSystemSessionProperty(BOB, "safe");
        accessControl.checkCanSetSystemSessionProperty(BOB, "staff");
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(BOB, "unsafe"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(ALICE, "dangerous"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(CHARLIE, "safe"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(CHARLIE, "staff"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(JOE, "staff"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "any", "dangerous");
        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "alice-catalog", "dangerous");
        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "any", "any");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "alice-catalog", "safe");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "alice-catalog", "unsafe");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "staff-catalog", "staff");
        accessControl.checkCanSetCatalogSessionProperty(BOB, "bob-catalog", "safe");
        accessControl.checkCanSetCatalogSessionProperty(BOB, "staff-catalog", "staff");
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(BOB, "bob-catalog", "any"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(BOB, "alice-catalog", "any"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(BOB, "staff-catalog", "any"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(ALICE, "alice-catalog", "dangerous"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(CHARLIE, "bob-catalog", "safe"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(CHARLIE, "staff-catalog", "staff"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(JOE, "staff-catalog", "staff"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testSessionPropertyDocsExample()
    {
        File rulesFile = new File("../../docs/src/main/sphinx/security/session-property-access.json");
        SystemAccessControl accessControl = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());
        SystemSecurityContext bannedUser = new SystemSecurityContext(Identity.ofUser("banned_user"), queryId);

        accessControl.checkCanSetSystemSessionProperty(ADMIN, "any");
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(ALICE, "any"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(bannedUser, "any"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanSetSystemSessionProperty(ADMIN, "resource_overcommit");
        accessControl.checkCanSetSystemSessionProperty(ALICE, "resource_overcommit");
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(bannedUser, "resource_overcommit"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "hive", "any");
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(ALICE, "hive", "any"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(bannedUser, "hive", "any"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "hive", "bucket_execution_enabled");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "hive", "bucket_execution_enabled");
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(bannedUser, "hive", "bucket_execution_enabled"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testFilterCatalogs()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");
        Set<String> allCatalogs = ImmutableSet.of(
                "alice-catalog",
                "bob-catalog",
                "specific-catalog",
                "secret",
                "hidden",
                "open-to-all",
                "blocked-catalog",
                "unknown",
                "ptf-catalog");

        assertEquals(accessControl.filterCatalogs(ADMIN, allCatalogs), Sets.difference(allCatalogs, ImmutableSet.of("blocked-catalog")));
        Set<String> aliceCatalogs = ImmutableSet.of("specific-catalog", "alice-catalog", "ptf-catalog");
        assertEquals(accessControl.filterCatalogs(ALICE, allCatalogs), aliceCatalogs);
        Set<String> bobCatalogs = ImmutableSet.of("specific-catalog", "alice-catalog", "bob-catalog");
        assertEquals(accessControl.filterCatalogs(BOB, allCatalogs), bobCatalogs);
        Set<String> charlieCatalogs = ImmutableSet.of("specific-catalog");
        assertEquals(accessControl.filterCatalogs(CHARLIE, allCatalogs), charlieCatalogs);
    }

    @Test
    public void testSchemaRulesForCheckCanShowSchemas()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");

        accessControl.checkCanShowSchemas(ADMIN, "specific-catalog");
        accessControl.checkCanShowSchemas(ADMIN, "session-catalog");
        accessControl.checkCanShowSchemas(ADMIN, "secret");
        accessControl.checkCanShowSchemas(ADMIN, "hidden");
        accessControl.checkCanShowSchemas(ADMIN, "open-to-all");
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ADMIN, "blocked-catalog"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanShowSchemas(ADMIN, "unknown");

        accessControl.checkCanShowSchemas(ALICE, "specific-catalog");
        accessControl.checkCanShowSchemas(ALICE, "session-catalog");
        accessControl.checkCanShowSchemas(ALICE, "alice-catalog");
        accessControl.checkCanShowSchemas(ALICE, "alice-catalog-session");
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ALICE, "bob-catalog"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ALICE, "bob-catalog-session"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ALICE, "secret"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ALICE, "hidden"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ALICE, "open-to-all"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ALICE, "blocked-catalog"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(ALICE, "unknown"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowSchemas(BOB, "specific-catalog");
        accessControl.checkCanShowSchemas(BOB, "session-catalog");
        accessControl.checkCanShowSchemas(BOB, "bob-catalog");
        accessControl.checkCanShowSchemas(BOB, "bob-catalog-session");
        accessControl.checkCanShowSchemas(BOB, "alice-catalog");
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(BOB, "alice-catalog-session"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(BOB, "secret"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(BOB, "hidden"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(BOB, "open-to-all"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(BOB, "blocked-catalog"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(BOB, "unknown"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowSchemas(CHARLIE, "session-catalog");
        accessControl.checkCanShowSchemas(CHARLIE, "specific-catalog");
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "alice-catalog-session"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "alice-catalog"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "bob-catalog-session"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "bob-catalog"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "secret"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "hidden"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "open-to-all"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "blocked-catalog"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowSchemas(CHARLIE, "unknown"), SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testFilterSchemas()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");

        assertEquals(accessControl.filterSchemas(ADMIN, "specific-catalog", ImmutableSet.of("specific-schema", "unknown")), ImmutableSet.of("specific-schema", "unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "specific-catalog", ImmutableSet.of("specific-schema", "unknown")), ImmutableSet.of("specific-schema"));
        assertEquals(accessControl.filterSchemas(BOB, "specific-catalog", ImmutableSet.of("specific-schema", "unknown")), ImmutableSet.of("specific-schema"));
        assertEquals(accessControl.filterSchemas(CHARLIE, "specific-catalog", ImmutableSet.of("specific-schema", "unknown")), ImmutableSet.of("specific-schema"));

        assertEquals(accessControl.filterSchemas(ADMIN, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown")), ImmutableSet.of("alice-schema", "bob-schema", "unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown")), ImmutableSet.of("alice-schema"));
        assertEquals(accessControl.filterSchemas(BOB, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown")), ImmutableSet.of("bob-schema"));
        assertEquals(accessControl.filterSchemas(CHARLIE, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "bob-catalog", ImmutableSet.of("bob-schema", "unknown")), ImmutableSet.of("bob-schema", "unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "bob-catalog", ImmutableSet.of("bob-schema", "unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(BOB, "bob-catalog", ImmutableSet.of("bob-schema", "unknown")), ImmutableSet.of("bob-schema"));
        assertEquals(accessControl.filterSchemas(CHARLIE, "bob-catalog", ImmutableSet.of("bob-schema", "unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "secret", ImmutableSet.of("unknown")), ImmutableSet.of("unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "secret", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(BOB, "secret", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(CHARLIE, "secret", ImmutableSet.of("unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "hidden", ImmutableSet.of("unknown")), ImmutableSet.of("unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "hidden", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(BOB, "hidden", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(CHARLIE, "hidden", ImmutableSet.of("unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "open-to-all", ImmutableSet.of("unknown")), ImmutableSet.of("unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "open-to-all", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(BOB, "open-to-all", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(CHARLIE, "open-to-all", ImmutableSet.of("unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "blocked-catalog", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(ALICE, "blocked-catalog", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(BOB, "blocked-catalog", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(CHARLIE, "blocked-catalog", ImmutableSet.of("unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "unknown", ImmutableSet.of("unknown")), ImmutableSet.of("unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "unknown", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(BOB, "unknown", ImmutableSet.of("unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(CHARLIE, "unknown", ImmutableSet.of("unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "session-catalog", ImmutableSet.of("session-schema", "unknown")), ImmutableSet.of("session-schema", "unknown"));
        assertEquals(accessControl.filterSchemas(ALICE, "session-catalog", ImmutableSet.of("session-schema", "unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(BOB, "session-catalog", ImmutableSet.of("session-schema", "unknown")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(CHARLIE, "session-catalog", ImmutableSet.of("session-schema", "unknown")), ImmutableSet.of());

        assertEquals(accessControl.filterSchemas(ADMIN, "ptf-catalog", ImmutableSet.of("ptf_schema")), ImmutableSet.of("ptf_schema"));
        assertEquals(accessControl.filterSchemas(ALICE, "ptf-catalog", ImmutableSet.of("ptf_schema")), ImmutableSet.of("ptf_schema"));
        assertEquals(accessControl.filterSchemas(BOB, "ptf-catalog", ImmutableSet.of("ptf_schema")), ImmutableSet.of());
        assertEquals(accessControl.filterSchemas(CHARLIE, "ptf-catalog", ImmutableSet.of("ptf_schema")), ImmutableSet.of());
    }

    @Test
    public void testSchemaRulesForCheckCanShowTables()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");

        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("specific-catalog", "specific-schema"));
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("bob-catalog", "bob-schema"));
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("bob-catalog", "any"));
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("alice-catalog", "alice-schema"));
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("alice-catalog", "any"));
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("secret", "secret"));
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("hidden", "any"));
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("open-to-all", "any"));
        assertAccessDenied(() -> accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanShowTables(ADMIN, new CatalogSchemaName("unknown", "any"));

        accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("specific-catalog", "specific-schema"));
        accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("alice-catalog", "alice-schema"));
        assertAccessDenied(() -> accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("bob-catalog", "bob-schema")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("secret", "secret")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("hidden", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("open-to-all", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(ALICE, new CatalogSchemaName("unknown", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowTables(BOB, new CatalogSchemaName("specific-catalog", "specific-schema"));
        accessControl.checkCanShowTables(BOB, new CatalogSchemaName("bob-catalog", "bob-schema"));
        accessControl.checkCanShowTables(BOB, new CatalogSchemaName("alice-catalog", "bob-schema"));
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("bob-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("alice-catalog", "alice-schema")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("alice-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("secret", "secret")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("hidden", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("open-to-all", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("unknown", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("specific-catalog", "specific-schema"));
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("bob-catalog", "bob-schema")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("bob-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("alice-catalog", "alice-schema")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("alice-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("secret", "secret")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("hidden", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("open-to-all", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(CHARLIE, new CatalogSchemaName("unknown", "any")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testGetColumnMask()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertEquals(
                accessControl.getColumnMask(
                        ALICE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        "masked",
                        VARCHAR),
                Optional.empty());

        assertViewExpressionEquals(
                accessControl.getColumnMask(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        "masked",
                        VARCHAR).orElseThrow(),
                new ViewExpression(Optional.empty(), Optional.of("some-catalog"), Optional.of("bobschema"), "'mask'"));

        assertViewExpressionEquals(
                accessControl.getColumnMask(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        "masked_with_user",
                        VARCHAR).orElseThrow(),
                new ViewExpression(Optional.of("mask-user"), Optional.of("some-catalog"), Optional.of("bobschema"), "'mask-with-user'"));
    }

    @Test
    public void testGetRowFilter()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertEquals(
                accessControl.getRowFilters(ALICE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns")),
                ImmutableList.of());

        List<ViewExpression> rowFilters = accessControl.getRowFilters(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"));
        assertEquals(rowFilters.size(), 1);
        assertViewExpressionEquals(
                rowFilters.get(0),
                new ViewExpression(Optional.empty(), Optional.of("some-catalog"), Optional.of("bobschema"), "starts_with(value, 'filter')"));

        rowFilters = accessControl.getRowFilters(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns_with_grant"));
        assertEquals(rowFilters.size(), 1);
        assertViewExpressionEquals(
                rowFilters.get(0),
                new ViewExpression(Optional.of("filter-user"), Optional.of("some-catalog"), Optional.of("bobschema"), "starts_with(value, 'filter-with-user')"));
    }

    private static void assertViewExpressionEquals(ViewExpression actual, ViewExpression expected)
    {
        assertEquals(actual.getSecurityIdentity(), expected.getSecurityIdentity(), "Identity");
        assertEquals(actual.getCatalog(), expected.getCatalog(), "Catalog");
        assertEquals(actual.getSchema(), expected.getSchema(), "Schema");
        assertEquals(actual.getExpression(), expected.getExpression(), "Expression");
    }

    @Test
    public void testFunctionRulesForCheckCanExecute()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ADMIN, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanExecuteFunction(ALICE, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"));
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(BOB, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(CHARLIE, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ADMIN, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_function")), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(ALICE, "some_function"), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanExecuteFunction(BOB, "some_function");
        assertAccessDenied(() -> accessControl.checkCanExecuteFunction(CHARLIE, "some_function"), EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testFunctionRulesForCheckCanGrantExecute()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");
        accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, ADMIN.getIdentity().getUser()), true);
        accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, ALICE.getIdentity().getUser()), true);
        accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, BOB.getIdentity().getUser()), true);
        accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, CHARLIE.getIdentity().getUser()), true);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_function"), new TrinoPrincipal(USER, ADMIN.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, "some_function", new TrinoPrincipal(USER, ALICE.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, "some_function", new TrinoPrincipal(USER, BOB.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(ALICE, "some_function", new TrinoPrincipal(USER, CHARLIE.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);

        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, ADMIN.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, ALICE.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, BOB.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"), new TrinoPrincipal(USER, CHARLIE.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, TABLE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_function"), new TrinoPrincipal(USER, ADMIN.getIdentity().getUser()), true), GRANT_EXECUTE_FUNCTION_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, "some_function", new TrinoPrincipal(USER, ALICE.getIdentity().getUser()), true);
        accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, "some_function", new TrinoPrincipal(USER, BOB.getIdentity().getUser()), true);
        accessControl.checkCanGrantExecuteFunctionPrivilege(BOB, "some_function", new TrinoPrincipal(USER, CHARLIE.getIdentity().getUser()), true);
    }

    @Test
    public void testSchemaAuthorization()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("authorization.json");

        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "test");
        CatalogSchemaName ownedByUser = new CatalogSchemaName("some-catalog", "owned_by_user");
        CatalogSchemaName ownedByGroup = new CatalogSchemaName("some-catalog", "owned_by_group");
        CatalogSchemaName ownedByRole = new CatalogSchemaName("some-catalog", "owned_by_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("user", "group", "role"), schema, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.test to ROLE new_role");

        // access to schema granted to user
        accessControl.checkCanSetSchemaAuthorization(user("owner_authorized", "group", "role"), ownedByUser, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetSchemaAuthorization(user("owner", "authorized", "role"), ownedByUser, new TrinoPrincipal(ROLE, "new_role"));
        accessControl.checkCanSetSchemaAuthorization(user("owner", "group", "authorized"), ownedByUser, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("owner_without_authorization_access", "group", "role"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("owner_DENY_authorized", "group", "role"), ownedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_user to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("owner", "DENY_authorized", "role"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("owner", "group", "DENY_authorized"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("owner", "group", "authorized"), ownedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_user to USER new_user");

        // access to schema granted to group
        accessControl.checkCanSetSchemaAuthorization(user("authorized", "owner", "role"), ownedByGroup, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetSchemaAuthorization(user("user", "owner", "authorized"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("user", "owner", "role"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("DENY_authorized", "owner", "role"), ownedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_group to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("user", "owner", "DENY_authorized"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("user", "owner", "authorized"), ownedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_group to USER new_user");

        // access to schema granted to role
        accessControl.checkCanSetSchemaAuthorization(user("authorized", "group", "owner"), ownedByRole, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetSchemaAuthorization(user("user", "group", "owner_authorized"), ownedByRole, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("user", "group", "owner"), ownedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("DENY_authorized", "group", "owner"), ownedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_role to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("user", "group", "owner_DENY_authorized"), ownedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetSchemaAuthorization(user("user", "group", "owner_authorized"), ownedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_role to USER new_user");
    }

    @Test
    public void testTableAuthorization()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("authorization.json");

        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "test", "table");
        CatalogSchemaTableName ownedByUser = new CatalogSchemaTableName("some-catalog", "test", "owned_by_user");
        CatalogSchemaTableName ownedByGroup = new CatalogSchemaTableName("some-catalog", "test", "owned_by_group");
        CatalogSchemaTableName ownedByRole = new CatalogSchemaTableName("some-catalog", "test", "owned_by_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("user", "group", "role"), table, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.table to ROLE new_role");

        // access to table granted to user
        accessControl.checkCanSetTableAuthorization(user("owner_authorized", "group", "role"), ownedByUser, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetTableAuthorization(user("owner", "group", "authorized"), ownedByUser, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("owner_without_authorization_access", "group", "role"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("owner_DENY_authorized", "group", "role"), ownedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("owner", "group", "DENY_authorized"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("owner", "group", "authorized"), ownedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to USER new_user");

        // access to table granted to group
        accessControl.checkCanSetTableAuthorization(user("authorized", "owner", "role"), ownedByGroup, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetTableAuthorization(user("user", "owner", "authorized"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("user", "owner", "role"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("DENY_authorized", "owner", "role"), ownedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("user", "owner", "DENY_authorized"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("user", "owner", "authorized"), ownedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to USER new_user");

        // access to table granted to role
        accessControl.checkCanSetTableAuthorization(user("authorized", "group", "owner"), ownedByRole, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetTableAuthorization(user("user", "group", "owner_authorized"), ownedByRole, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("user", "group", "owner"), ownedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("DENY_authorized", "group", "owner"), ownedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("user", "group", "owner_DENY_authorized"), ownedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetTableAuthorization(user("user", "group", "owner_authorized"), ownedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to USER new_user");
    }

    @Test
    public void testViewAuthorization()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("authorization.json");

        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "test", "table");
        CatalogSchemaTableName ownedByUser = new CatalogSchemaTableName("some-catalog", "test", "owned_by_user");
        CatalogSchemaTableName ownedByGroup = new CatalogSchemaTableName("some-catalog", "test", "owned_by_group");
        CatalogSchemaTableName ownedByRole = new CatalogSchemaTableName("some-catalog", "test", "owned_by_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("user", "group", "role"), table, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for view some-catalog.test.table to ROLE new_role");

        // access to table granted to user
        accessControl.checkCanSetViewAuthorization(user("owner_authorized", "group", "role"), ownedByUser, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetViewAuthorization(user("owner", "group", "authorized"), ownedByUser, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("owner_without_authorization_access", "group", "role"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for view some-catalog.test.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("owner_DENY_authorized", "group", "role"), ownedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for view some-catalog.test.owned_by_user to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("owner", "group", "DENY_authorized"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for view some-catalog.test.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("owner", "group", "authorized"), ownedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for view some-catalog.test.owned_by_user to USER new_user");

        // access to table granted to group
        accessControl.checkCanSetViewAuthorization(user("authorized", "owner", "role"), ownedByGroup, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetViewAuthorization(user("user", "owner", "authorized"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("user", "owner", "role"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for view some-catalog.test.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("DENY_authorized", "owner", "role"), ownedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for view some-catalog.test.owned_by_group to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("user", "owner", "DENY_authorized"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for view some-catalog.test.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("user", "owner", "authorized"), ownedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for view some-catalog.test.owned_by_group to USER new_user");

        // access to table granted to role
        accessControl.checkCanSetViewAuthorization(user("authorized", "group", "owner"), ownedByRole, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetViewAuthorization(user("user", "group", "owner_authorized"), ownedByRole, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("user", "group", "owner"), ownedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for view some-catalog.test.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("DENY_authorized", "group", "owner"), ownedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for view some-catalog.test.owned_by_role to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("user", "group", "owner_DENY_authorized"), ownedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for view some-catalog.test.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetViewAuthorization(user("user", "group", "owner_authorized"), ownedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for view some-catalog.test.owned_by_role to USER new_user");
    }

    @Test
    public void testAuthorizationDocsExample()
    {
        File rulesFile = new File("../../docs/src/main/sphinx/security/authorization.json");
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());
        CatalogSchemaName schema = new CatalogSchemaName("catalog", "schema");
        CatalogSchemaTableName tableOrView = new CatalogSchemaTableName("catalog", "schema", "table_or_view");
        accessControlManager.checkCanSetSchemaAuthorization(ADMIN, schema, new TrinoPrincipal(USER, "alice"));
        accessControlManager.checkCanSetSchemaAuthorization(ADMIN, schema, new TrinoPrincipal(ROLE, "role"));
        accessControlManager.checkCanSetTableAuthorization(ADMIN, tableOrView, new TrinoPrincipal(USER, "alice"));
        accessControlManager.checkCanSetTableAuthorization(ADMIN, tableOrView, new TrinoPrincipal(ROLE, "role"));
        accessControlManager.checkCanSetViewAuthorization(ADMIN, tableOrView, new TrinoPrincipal(USER, "alice"));
        accessControlManager.checkCanSetViewAuthorization(ADMIN, tableOrView, new TrinoPrincipal(ROLE, "role"));
        assertAccessDenied(
                () -> accessControlManager.checkCanSetSchemaAuthorization(ADMIN, schema, new TrinoPrincipal(USER, "bob")),
                "Cannot set authorization for schema catalog.schema to USER bob");
        assertAccessDenied(
                () -> accessControlManager.checkCanSetTableAuthorization(ADMIN, tableOrView, new TrinoPrincipal(USER, "bob")),
                "Cannot set authorization for table catalog.schema.table_or_view to USER bob");
        assertAccessDenied(
                () -> accessControlManager.checkCanSetViewAuthorization(ADMIN, tableOrView, new TrinoPrincipal(USER, "bob")),
                "Cannot set authorization for view catalog.schema.table_or_view to USER bob");
    }

    private static SystemSecurityContext user(String user, String group, String role)
    {
        Identity identity = Identity.forUser(user)
                .withGroups(ImmutableSet.of(group))
                .withEnabledRoles(ImmutableSet.of(role))
                .build();
        return new SystemSecurityContext(identity, queryId);
    }

    @Test
    public void parseUnknownRules()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("file-based-system-security-config-file-with-unknown-rules.json"))
                .hasMessageContaining("Failed to convert JSON tree node");
    }

    @Test
    public void testTableRulesForCheckCanInsertIntoTableWithJsonPointer()
    {
        File configFile = new File(getResourcePath("file-based-system-access-table-with-json-pointer.json"));
        SystemAccessControl accessControl = newFileBasedSystemAccessControl(configFile, ImmutableMap.of("security.json-pointer", "/data"));
        assertTableRulesForCheckCanInsertIntoTable(accessControl);
    }

    protected SystemAccessControl newFileBasedSystemAccessControl(String rulesName)
    {
        File configFile = new File(getResourcePath(rulesName));
        return newFileBasedSystemAccessControl(configFile, ImmutableMap.of());
    }

    protected SystemAccessControl newFileBasedSystemAccessControl(Map<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config);
    }

    protected String getResourcePath(String resourceName)
    {
        return requireNonNull(this.getClass().getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).getPath();
    }

    private static void assertAccessDenied(ThrowingCallable callable, String expectedMessage)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied: " + expectedMessage);
    }
}
