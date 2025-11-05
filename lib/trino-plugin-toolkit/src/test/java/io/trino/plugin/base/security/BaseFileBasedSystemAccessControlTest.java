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
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    private static final QueryId queryId = new QueryId("test_query");
    private static final Instant queryStart = Instant.now();

    private static final Identity charlie = Identity.forUser("charlie").withGroups(ImmutableSet.of("guests")).build();
    private static final Identity dave = Identity.forUser("dave").withGroups(ImmutableSet.of("contractors")).build();
    private static final Identity joe = Identity.ofUser("joe");
    private static final Identity any = Identity.ofUser("any");
    private static final Identity anyone = Identity.ofUser("anyone");
    private static final Identity unknown = Identity.ofUser("some-unknown-user-id");
    private static final SystemSecurityContext ADMIN = new SystemSecurityContext(admin, queryId, queryStart);
    private static final SystemSecurityContext BOB = new SystemSecurityContext(bob, queryId, queryStart);
    private static final SystemSecurityContext CHARLIE = new SystemSecurityContext(charlie, queryId, queryStart);
    private static final SystemSecurityContext ALICE = new SystemSecurityContext(alice, queryId, queryStart);
    private static final SystemSecurityContext JOE = new SystemSecurityContext(joe, queryId, queryStart);
    private static final SystemSecurityContext UNKNOWN = new SystemSecurityContext(unknown, queryId, queryStart);

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
    private static final String SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE = "Cannot show functions of .*";

    private static final String SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE = "Cannot set system session property .*";
    private static final String SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE = "Cannot set catalog session property .*";
    private static final String EXECUTE_PROCEDURE_ACCESS_DENIED_MESSAGE = "Cannot execute procedure .*";

    protected abstract SystemAccessControl newFileBasedSystemAccessControl(Path configFile, Map<String, String> properties);

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(SystemAccessControl.class, FileBasedSystemAccessControl.class);
    }

    @Test
    public void testRefreshing(@TempDir Path tempDir)
            throws Exception
    {
        Path configFile = tempDir.resolve("file-based-system-catalog.json");
        Files.copy(getResourcePath("file-based-system-catalog.json"), configFile, REPLACE_EXISTING);

        SystemAccessControl accessControl = newFileBasedSystemAccessControl(configFile, ImmutableMap.of(
                "security.refresh-period", "1ms"));

        SystemSecurityContext alice = new SystemSecurityContext(BaseFileBasedSystemAccessControlTest.alice, queryId, queryStart);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);

        Files.copy(getResourcePath("file-based-system-security-config-file-with-unknown-rules.json"), configFile, REPLACE_EXISTING);
        sleep(2);

        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .hasMessageContaining("Failed to convert JSON tree node");

        // test if file based cached control was not cached somewhere
        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .hasMessageContaining("Failed to convert JSON tree node");

        Files.copy(getResourcePath("file-based-system-catalog.json"), configFile, REPLACE_EXISTING);
        sleep(2);

        accessControl.checkCanCreateView(alice, aliceView);
    }

    @Test
    public void testEmptyFile()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("empty.json");

        accessControl.checkCanCreateSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"), ImmutableMap.of());
        accessControl.checkCanDropSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"));
        accessControl.checkCanRenameSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"), "new_unknown");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(UNKNOWN, new EntityKindAndName("SCHEMA", List.of("some-catalog", "unknown")), new TrinoPrincipal(ROLE, "some_role")),
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
                () -> accessControl.checkCanSetEntityAuthorization(UNKNOWN, new EntityKindAndName("TABLE", List.of("some-catalog", "unknown", "unknown")), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for table some-catalog.unknown.unknown to ROLE some_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(UNKNOWN, new EntityKindAndName("VIEW", List.of("some-catalog", "unknown", "unknown")), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for view some-catalog.unknown.unknown to ROLE some_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetMaterializedViewAuthorization(UNKNOWN, new CatalogSchemaTableName("some-catalog", new SchemaTableName("unknown", "unknown")), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for materialized view some-catalog.unknown.unknown to ROLE some_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(UNKNOWN, new EntityKindAndName("MATERIALIZED VIEW", List.of("some-catalog", "unknown", "unknown")), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for materialized view some-catalog.unknown.unknown to ROLE some_role");

        accessControl.checkCanCreateMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), Map.of());
        accessControl.checkCanDropMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanRefreshMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(UNKNOWN, new EntityKindAndName("VIEW", List.of("some-catalog", "unknown", "unknown")), new TrinoPrincipal(ROLE, "some_role")),
                "Cannot set authorization for view some-catalog.unknown.unknown to ROLE some_role");

        accessControl.checkCanSetUser(Optional.empty(), "unknown");
        accessControl.checkCanSetUser(Optional.of(new KerberosPrincipal("stuff@example.com")), "unknown");

        accessControl.checkCanSetSystemSessionProperty(unknown, queryId, "anything");
        accessControl.checkCanSetCatalogSessionProperty(UNKNOWN, "unknown", "anything");

        accessControl.checkCanExecuteQuery(unknown, queryId);
        accessControl.checkCanViewQueryOwnedBy(unknown, anyone);
        accessControl.checkCanKillQueryOwnedBy(unknown, anyone);

        // system information access is denied by default
        assertAccessDenied(
                () -> accessControl.checkCanReadSystemInformation(unknown),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControl.checkCanWriteSystemInformation(unknown),
                "Cannot write system information");
    }

    @Test
    public void testSchemaRulesForCheckCanCreateSchema()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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

    @Test
    public void testGrantSchemaPrivilege()
            throws Exception
    {
        for (Privilege privilege : Privilege.values()) {
            testGrantSchemaPrivilege(privilege, false);
            testGrantSchemaPrivilege(privilege, true);
        }
    }

    private void testGrantSchemaPrivilege(Privilege privilege, boolean grantOption)
            throws URISyntaxException
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
            throws Exception
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

    @Test
    public void testRevokeSchemaPrivilege()
            throws Exception
    {
        for (Privilege privilege : Privilege.values()) {
            testRevokeSchemaPrivilege(privilege, false);
            testRevokeSchemaPrivilege(privilege, true);
        }
    }

    private void testRevokeSchemaPrivilege(Privilege privilege, boolean grantOption)
            throws URISyntaxException
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

    @Test
    public void testTableRulesForCheckCanSelectFromColumns()
            throws Exception
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
            throws Exception
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
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanShowColumns(ALICE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        accessControl.checkCanShowColumns(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
    }

    @Test
    public void testTableRulesForCheckCanShowColumnsWithNoAccess()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");
        assertAccessDenied(() -> accessControl.checkCanShowColumns(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), SHOW_COLUMNS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowTables(BOB, new CatalogSchemaName("some-catalog", "bobschema")), SHOWN_TABLES_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testFunctionRulesForCheckExecuteAndGrantExecuteFunctionWithNoAccess()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");
        assertThat(accessControl.canExecuteFunction(ALICE, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new CatalogSchemaRoutineName("alice-catalog", "schema", "some_function"))).isFalse();
    }

    @Test
    public void testTableRulesForFilterColumns()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertThat(accessControl.filterColumns(
                ALICE,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                ImmutableSet.of("private", "a", "restricted", "b"))).isEqualTo(ImmutableSet.of("private", "a", "restricted", "b"));
        assertThat(accessControl.filterColumns(
                BOB,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                ImmutableSet.of("private", "a", "restricted", "b"))).isEqualTo(ImmutableSet.of("private", "a", "restricted", "b"));
        assertThat(accessControl.filterColumns(
                CHARLIE,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                ImmutableSet.of("private", "a", "restricted", "b"))).isEqualTo(ImmutableSet.of("a", "b"));
    }

    @Test
    public void testTableFilter()
            throws Exception
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
        assertThat(accessControl.filterTables(ALICE, "any", tables)).isEqualTo(ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .build());
        assertThat(accessControl.filterTables(BOB, "any", tables)).isEqualTo(ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .build());
        assertThat(accessControl.filterTables(ADMIN, "any", tables)).isEqualTo(ImmutableSet.<SchemaTableName>builder()
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
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");

        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("restricted", "any"))
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertThat(accessControl.filterTables(ALICE, "any", tables)).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterTables(BOB, "any", tables)).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testTableRulesForFilterColumnsWithNoAccess()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");
        assertThat(accessControl.filterColumns(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), ImmutableSet.of("a"))).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testTableRulesForCheckCanInsertIntoTable()
            throws Exception
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
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDropTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanDropTable(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), DROP_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDropMaterializedView()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDropMaterializedView(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"));
        assertAccessDenied(() -> accessControl.checkCanDropMaterializedView(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view")), DROP_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanCreateMaterializedView()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanCreateMaterializedView(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"), Map.of());
        assertAccessDenied(() -> accessControl.checkCanCreateMaterializedView(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"), Map.of()), CREATE_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRefreshMaterializedView()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRefreshMaterializedView(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view"));
        assertAccessDenied(() -> accessControl.checkCanRefreshMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "bobschema", "bob-materialized-view")), REFRESH_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanSetMaterializedViewProperties()
            throws Exception
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
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDeleteFromTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanDeleteFromTable(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), DELETE_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanTruncateTable()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanTruncateTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanTruncateTable(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), TRUNCATE_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanGrantTablePrivilege()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanGrantTablePrivilege(ADMIN, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false);
        assertAccessDenied(() -> accessControl.checkCanGrantTablePrivilege(BOB, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false), GRANT_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDenyTablePrivilege()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDenyTablePrivilege(ADMIN, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null);
        assertAccessDenied(() -> accessControl.checkCanDenyTablePrivilege(BOB, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null), DENY_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRevokeTablePrivilege()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRevokeTablePrivilege(ADMIN, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false);
        assertAccessDenied(() -> accessControl.checkCanRevokeTablePrivilege(BOB, Privilege.DELETE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), null, false), REVOKE_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanShowCreateTable()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanShowCreateTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanShowCreateTable(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), CREATE_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanAddColumn()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanAddColumn(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanAddColumn(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), ADD_COLUMNS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanDropColumn()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanDropColumn(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanDropColumn(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), DROP_COLUMNS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRenameColumn()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRenameColumn(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanRenameColumn(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), RENAME_COLUMNS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForMixedGroupUsers()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table-mixed-groups.json");

        SystemSecurityContext userGroup1Group2 = new SystemSecurityContext(Identity.forUser("user_1_2")
                .withGroups(ImmutableSet.of("group1", "group2")).build(), queryId, queryStart);
        SystemSecurityContext userGroup2 = new SystemSecurityContext(Identity.forUser("user_2")
                .withGroups(ImmutableSet.of("group2")).build(), queryId, queryStart);

        assertThat(accessControl.getColumnMask(
                userGroup1Group2,
                new CatalogSchemaTableName("some-catalog", "my_schema", "my_table"),
                "col_a",
                VARCHAR)).isEqualTo(Optional.empty());

        assertViewExpressionEquals(
                accessControl.getColumnMask(
                        userGroup2,
                        new CatalogSchemaTableName("some-catalog", "my_schema", "my_table"),
                        "col_a",
                        VARCHAR).orElseThrow(),
                ViewExpression.builder()
                        .catalog("some-catalog")
                        .schema("my_schema")
                        .expression("'mask_a'")
                        .build());

        SystemSecurityContext userGroup1Group3 = new SystemSecurityContext(Identity.forUser("user_1_3")
                .withGroups(ImmutableSet.of("group1", "group3")).build(), queryId, queryStart);
        SystemSecurityContext userGroup3 = new SystemSecurityContext(Identity.forUser("user_3")
                .withGroups(ImmutableSet.of("group3")).build(), queryId, queryStart);

        assertThat(accessControl.getRowFilters(
                userGroup1Group3,
                new CatalogSchemaTableName("some-catalog", "my_schema", "my_table"))).isEqualTo(ImmutableList.of());

        List<ViewExpression> rowFilters = accessControl.getRowFilters(
                userGroup3,
                new CatalogSchemaTableName("some-catalog", "my_schema", "my_table"));
        assertThat(rowFilters).hasSize(1);
        assertViewExpressionEquals(
                rowFilters.get(0),
                ViewExpression.builder()
                        .catalog("some-catalog")
                        .schema("my_schema")
                        .expression("country='US'")
                        .build());
    }

    @Test
    public void testTableRulesForCheckCanSetTableComment()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetTableComment(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"));
        assertAccessDenied(() -> accessControl.checkCanSetTableComment(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable")), TABLE_COMMENT_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanRenameTable()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanRenameTable(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), new CatalogSchemaTableName("some-catalog", "aliceschema", "newbobtable"));
        accessControl.checkCanRenameTable(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "alicetable"), new CatalogSchemaTableName("some-catalog", "aliceschema", "newalicetable"));
        assertAccessDenied(() -> accessControl.checkCanRenameTable(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), new CatalogSchemaTableName("some-catalog", "bobschema", "newbobtable")), RENAME_TABLE_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanRenameTable(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "alicetable"), new CatalogSchemaTableName("some-catalog", "bobschema", "newalicetable")), RENAME_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testTableRulesForCheckCanSetTableProperties()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetTableProperties(ADMIN, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), ImmutableMap.of());
        accessControl.checkCanSetTableProperties(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "alicetable"), ImmutableMap.of());
        assertAccessDenied(() -> accessControl.checkCanSetTableProperties(BOB, new CatalogSchemaTableName("some-catalog", "bobschema", "bobtable"), ImmutableMap.of()), SET_TABLE_PROPERTIES_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testCanSetUserOperations()
            throws Exception
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
            throws Exception
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("query.json");

        accessControlManager.checkCanExecuteQuery(admin, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(admin, any);
        assertThat(accessControlManager.filterViewQueryOwnedBy(admin, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(admin, any);

        accessControlManager.checkCanExecuteQuery(alice, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(alice, any);
        assertThat(accessControlManager.filterViewQueryOwnedBy(alice, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(alice, any),
                "Cannot kill query");

        assertAccessDenied(
                () -> accessControlManager.checkCanExecuteQuery(bob, queryId),
                "Cannot execute query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(bob, any),
                "Cannot view query");
        assertThat(accessControlManager.filterViewQueryOwnedBy(bob, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(bob, any);

        accessControlManager.checkCanExecuteQuery(dave, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(dave, alice);
        accessControlManager.checkCanViewQueryOwnedBy(dave, dave);
        assertThat(accessControlManager.filterViewQueryOwnedBy(dave, ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("bob"), Identity.ofUser("dave"), Identity.ofUser("admin")))).isEqualTo(ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("dave")));
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(dave, alice),
                "Cannot kill query");
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(dave, bob),
                "Cannot kill query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(dave, bob),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(dave, admin),
                "Cannot view query");

        Identity contractor = Identity.forUser("some-other-contractor").withGroups(ImmutableSet.of("contractors")).build();
        accessControlManager.checkCanExecuteQuery(contractor, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(contractor, dave);
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(contractor, dave),
                "Cannot kill query");

        accessControlManager.checkCanExecuteQuery(nonAsciiUser, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(nonAsciiUser, any);
        assertThat(accessControlManager.filterViewQueryOwnedBy(nonAsciiUser, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(nonAsciiUser, any);
    }

    @Test
    public void testInvalidQuery()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("query-invalid.json"))
                .rootCause().hasMessage("A valid query rule cannot combine an queryOwner condition with access mode 'execute'");
    }

    @Test
    public void testQueryNotSet()
            throws Exception
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("file-based-system-catalog.json");

        accessControlManager.checkCanExecuteQuery(bob, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(bob, any);
        assertThat(accessControlManager.filterViewQueryOwnedBy(bob, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(bob, any);
    }

    @Test
    public void testQueryDocsExample()
    {
        Path rulesFile = Paths.get("../../docs/src/main/sphinx/security/query-access.json");
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());

        accessControlManager.checkCanExecuteQuery(admin, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(admin, any);
        assertThat(accessControlManager.filterViewQueryOwnedBy(admin, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")));
        accessControlManager.checkCanKillQueryOwnedBy(admin, any);

        accessControlManager.checkCanExecuteQuery(alice, queryId);
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(alice, any),
                "Cannot view query");
        assertThat(accessControlManager.filterViewQueryOwnedBy(alice, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(alice, any);

        accessControlManager.checkCanExecuteQuery(alice, queryId);
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(bob, any),
                "Cannot view query");
        assertThat(accessControlManager.filterViewQueryOwnedBy(bob, ImmutableSet.of(Identity.ofUser("a"), Identity.ofUser("b")))).isEqualTo(ImmutableSet.of());
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(bob, any),
                "Cannot kill query");

        accessControlManager.checkCanExecuteQuery(dave, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(dave, alice);
        accessControlManager.checkCanViewQueryOwnedBy(dave, dave);
        assertThat(accessControlManager.filterViewQueryOwnedBy(dave, ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("bob"), Identity.ofUser("dave"), Identity.ofUser("admin")))).isEqualTo(ImmutableSet.of(Identity.ofUser("alice"), Identity.ofUser("dave")));
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(dave, alice),
                "Cannot kill query");
        assertAccessDenied(() -> accessControlManager.checkCanKillQueryOwnedBy(dave, bob),
                "Cannot kill query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(dave, bob),
                "Cannot view query");
        assertAccessDenied(
                () -> accessControlManager.checkCanViewQueryOwnedBy(dave, admin),
                "Cannot view query");

        Identity contractor = Identity.forUser("some-other-contractor").withGroups(ImmutableSet.of("contractors")).build();
        accessControlManager.checkCanExecuteQuery(contractor, queryId);
        accessControlManager.checkCanViewQueryOwnedBy(contractor, dave);
        assertAccessDenied(
                () -> accessControlManager.checkCanKillQueryOwnedBy(contractor, dave),
                "Cannot kill query");
    }

    @Test
    public void testSystemInformation()
            throws Exception
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("system-information.json");

        accessControlManager.checkCanReadSystemInformation(admin);
        accessControlManager.checkCanWriteSystemInformation(admin);

        accessControlManager.checkCanReadSystemInformation(alice);
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(alice),
                "Cannot write system information");

        assertAccessDenied(
                () -> accessControlManager.checkCanReadSystemInformation(bob),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(bob),
                "Cannot write system information");

        accessControlManager.checkCanReadSystemInformation(nonAsciiUser);
        accessControlManager.checkCanWriteSystemInformation(nonAsciiUser);
    }

    @Test
    public void testSystemInformationNotSet()
            throws Exception
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("file-based-system-catalog.json");

        assertAccessDenied(
                () -> accessControlManager.checkCanReadSystemInformation(bob),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(bob),
                "Cannot write system information");
    }

    @Test
    public void testSystemInformationDocsExample()
    {
        Path rulesFile = Paths.get("../../docs/src/main/sphinx/security/system-information-access.json");
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());

        accessControlManager.checkCanReadSystemInformation(admin);
        accessControlManager.checkCanWriteSystemInformation(admin);

        accessControlManager.checkCanReadSystemInformation(alice);
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(alice),
                "Cannot write system information");

        assertAccessDenied(
                () -> accessControlManager.checkCanReadSystemInformation(bob),
                "Cannot read system information");
        assertAccessDenied(
                () -> accessControlManager.checkCanWriteSystemInformation(bob),
                "Cannot write system information");
    }

    @Test
    public void testSessionPropertyRules()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-session-property.json");

        accessControl.checkCanSetSystemSessionProperty(admin, queryId, "dangerous");
        accessControl.checkCanSetSystemSessionProperty(admin, queryId, "any");
        accessControl.checkCanSetSystemSessionProperty(alice, queryId, "safe");
        accessControl.checkCanSetSystemSessionProperty(alice, queryId, "unsafe");
        accessControl.checkCanSetSystemSessionProperty(alice, queryId, "staff");
        accessControl.checkCanSetSystemSessionProperty(bob, queryId, "safe");
        accessControl.checkCanSetSystemSessionProperty(bob, queryId, "staff");
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(bob, queryId, "unsafe"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(alice, queryId, "dangerous"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(charlie, queryId, "safe"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(charlie, queryId, "staff"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(joe, queryId, "staff"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

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
        Path rulesFile = Paths.get("../../docs/src/main/sphinx/security/session-property-access.json");
        SystemAccessControl accessControl = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());
        Identity bannedUser = Identity.ofUser("banned_user");
        SystemSecurityContext bannedUserContext = new SystemSecurityContext(Identity.ofUser("banned_user"), queryId, queryStart);

        accessControl.checkCanSetSystemSessionProperty(admin, queryId, "any");
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(alice, queryId, "any"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(bannedUser, queryId, "any"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanSetSystemSessionProperty(admin, queryId, "resource_overcommit");
        accessControl.checkCanSetSystemSessionProperty(alice, queryId, "resource_overcommit");
        assertAccessDenied(() -> accessControl.checkCanSetSystemSessionProperty(bannedUser, queryId, "resource_overcommit"), SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "hive", "any");
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(ALICE, "hive", "any"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(bannedUserContext, "hive", "any"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "hive", "bucket_execution_enabled");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "hive", "bucket_execution_enabled");
        assertAccessDenied(() -> accessControl.checkCanSetCatalogSessionProperty(bannedUserContext, "hive", "bucket_execution_enabled"), SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testFilterCatalogs()
            throws Exception
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

        assertThat(accessControl.filterCatalogs(ADMIN, allCatalogs)).isEqualTo(Sets.difference(allCatalogs, ImmutableSet.of("blocked-catalog")));
        Set<String> aliceCatalogs = ImmutableSet.of("specific-catalog", "alice-catalog", "ptf-catalog");
        assertThat(accessControl.filterCatalogs(ALICE, allCatalogs)).isEqualTo(aliceCatalogs);
        Set<String> bobCatalogs = ImmutableSet.of("specific-catalog", "alice-catalog", "bob-catalog");
        assertThat(accessControl.filterCatalogs(BOB, allCatalogs)).isEqualTo(bobCatalogs);
        Set<String> charlieCatalogs = ImmutableSet.of("specific-catalog");
        assertThat(accessControl.filterCatalogs(CHARLIE, allCatalogs)).isEqualTo(charlieCatalogs);
    }

    @Test
    public void testSchemaRulesForCheckCanShowSchemas()
            throws Exception
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
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");

        assertThat(accessControl.filterSchemas(ADMIN, "specific-catalog", ImmutableSet.of("specific-schema", "unknown"))).isEqualTo(ImmutableSet.of("specific-schema", "unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "specific-catalog", ImmutableSet.of("specific-schema", "unknown"))).isEqualTo(ImmutableSet.of("specific-schema"));
        assertThat(accessControl.filterSchemas(BOB, "specific-catalog", ImmutableSet.of("specific-schema"))).isEqualTo(ImmutableSet.of("specific-schema"));
        assertThat(accessControl.filterSchemas(CHARLIE, "specific-catalog", ImmutableSet.of("specific-schema", "unknown"))).isEqualTo(ImmutableSet.of("specific-schema"));

        assertThat(accessControl.filterSchemas(ADMIN, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown"))).isEqualTo(ImmutableSet.of("alice-schema", "bob-schema", "unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown"))).isEqualTo(ImmutableSet.of("alice-schema"));
        assertThat(accessControl.filterSchemas(BOB, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown"))).isEqualTo(ImmutableSet.of("bob-schema"));
        assertThat(accessControl.filterSchemas(CHARLIE, "alice-catalog", ImmutableSet.of("alice-schema", "bob-schema", "unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "bob-catalog", ImmutableSet.of("bob-schema", "unknown"))).isEqualTo(ImmutableSet.of("bob-schema", "unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "bob-catalog", ImmutableSet.of("bob-schema", "unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(BOB, "bob-catalog", ImmutableSet.of("bob-schema", "unknown"))).isEqualTo(ImmutableSet.of("bob-schema"));
        assertThat(accessControl.filterSchemas(CHARLIE, "bob-catalog", ImmutableSet.of("bob-schema", "unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "secret", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of("unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "secret", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(BOB, "secret", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(CHARLIE, "secret", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "hidden", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of("unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "hidden", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(BOB, "hidden", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(CHARLIE, "hidden", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "open-to-all", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of("unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "open-to-all", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(BOB, "open-to-all", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(CHARLIE, "open-to-all", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "blocked-catalog", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(ALICE, "blocked-catalog", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(BOB, "blocked-catalog", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(CHARLIE, "blocked-catalog", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "unknown", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of("unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "unknown", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(BOB, "unknown", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(CHARLIE, "unknown", ImmutableSet.of("unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "session-catalog", ImmutableSet.of("session-schema", "unknown"))).isEqualTo(ImmutableSet.of("session-schema", "unknown"));
        assertThat(accessControl.filterSchemas(ALICE, "session-catalog", ImmutableSet.of("session-schema", "unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(BOB, "session-catalog", ImmutableSet.of("session-schema", "unknown"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(CHARLIE, "session-catalog", ImmutableSet.of("session-schema", "unknown"))).isEqualTo(ImmutableSet.of());

        assertThat(accessControl.filterSchemas(ADMIN, "ptf-catalog", ImmutableSet.of("ptf_schema"))).isEqualTo(ImmutableSet.of("ptf_schema"));
        assertThat(accessControl.filterSchemas(ALICE, "ptf-catalog", ImmutableSet.of("ptf_schema"))).isEqualTo(ImmutableSet.of("ptf_schema"));
        assertThat(accessControl.filterSchemas(BOB, "ptf-catalog", ImmutableSet.of("ptf_schema"))).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterSchemas(CHARLIE, "ptf-catalog", ImmutableSet.of("ptf_schema"))).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testSchemaRulesForCheckCanShowTables()
            throws Exception
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
    public void testSchemaRulesForCheckCanShowFunctions()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");

        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("specific-catalog", "specific-schema"));
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("bob-catalog", "bob-schema"));
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("bob-catalog", "any"));
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("alice-catalog", "alice-schema"));
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("alice-catalog", "any"));
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("secret", "secret"));
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("hidden", "any"));
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("open-to-all", "any"));
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanShowFunctions(ADMIN, new CatalogSchemaName("unknown", "any"));

        accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("specific-catalog", "specific-schema"));
        accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("alice-catalog", "alice-schema"));
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("bob-catalog", "bob-schema")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("secret", "secret")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("hidden", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("open-to-all", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(ALICE, new CatalogSchemaName("unknown", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("specific-catalog", "specific-schema"));
        accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("bob-catalog", "bob-schema"));
        accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("alice-catalog", "bob-schema"));
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("bob-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("alice-catalog", "alice-schema")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("alice-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("secret", "secret")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("hidden", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("open-to-all", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(BOB, new CatalogSchemaName("unknown", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("specific-catalog", "specific-schema"));
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("bob-catalog", "bob-schema")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("bob-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("alice-catalog", "alice-schema")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("alice-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("secret", "secret")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("hidden", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("open-to-all", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("blocked-catalog", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, new CatalogSchemaName("unknown", "any")), SHOWN_FUNCTIONS_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testGetColumnMask()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertThat(accessControl.getColumnMask(
                ALICE,
                new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                "masked",
                VARCHAR)).isEqualTo(Optional.empty());

        assertViewExpressionEquals(
                accessControl.getColumnMask(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        "masked",
                        VARCHAR).orElseThrow(),
                ViewExpression.builder()
                        .catalog("some-catalog")
                        .schema("bobschema")
                        .expression("'mask'")
                        .build());

        assertViewExpressionEquals(
                accessControl.getColumnMask(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        "masked_with_user",
                        VARCHAR).orElseThrow(),
                ViewExpression.builder()
                        .identity("mask-user")
                        .catalog("some-catalog")
                        .schema("bobschema")
                        .expression("'mask-with-user'")
                        .build());
    }

    @Test
    public void testGetColumnMasks()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");
        List<ColumnSchema> columns = Stream.of("private", "restricted", "masked", "masked_with_user")
                .map(BaseFileBasedSystemAccessControlTest::createColumnSchema)
                .collect(toImmutableList());

        assertThat(accessControl.getColumnMasks(
                 ALICE,
                 new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                 columns)).isEmpty();

        Map<ColumnSchema, ViewExpression> charlieColumnMasks = accessControl.getColumnMasks(
                 CHARLIE,
                 new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                 columns);
        assertThat(charlieColumnMasks).doesNotContainKey(createColumnSchema("private"));
        assertThat(charlieColumnMasks).doesNotContainKey(createColumnSchema("restricted"));
        assertViewExpressionEquals(
                charlieColumnMasks.get(createColumnSchema("masked")),
                ViewExpression.builder()
                        .catalog("some-catalog")
                        .schema("bobschema")
                        .expression("'mask'")
                        .build());
        assertViewExpressionEquals(
                charlieColumnMasks.get(createColumnSchema("masked_with_user")),
                 ViewExpression.builder()
                        .identity("mask-user")
                        .catalog("some-catalog")
                        .schema("bobschema")
                        .expression("'mask-with-user'")
                        .build());
    }

    public static ColumnSchema createColumnSchema(String columnName)
    {
        return ColumnSchema.builder().setName(columnName).setType(VARCHAR).build();
    }

    @Test
    public void testGetRowFilter()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertThat(accessControl.getRowFilters(ALICE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"))).isEqualTo(ImmutableList.of());

        List<ViewExpression> rowFilters = accessControl.getRowFilters(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"));
        assertThat(rowFilters).hasSize(1);
        assertViewExpressionEquals(
                rowFilters.get(0),
                ViewExpression.builder()
                        .catalog("some-catalog")
                        .schema("bobschema")
                        .expression("starts_with(value, 'filter')")
                        .build());

        rowFilters = accessControl.getRowFilters(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns_with_grant"));
        assertThat(rowFilters).hasSize(1);
        assertViewExpressionEquals(
                rowFilters.get(0),
                ViewExpression.builder()
                        .identity("filter-user")
                        .catalog("some-catalog")
                        .schema("bobschema")
                        .expression("starts_with(value, 'filter-with-user')")
                        .build());
    }

    private static void assertViewExpressionEquals(ViewExpression actual, ViewExpression expected)
    {
        assertThat(actual.getSecurityIdentity())
                .describedAs("Identity")
                .isEqualTo(expected.getSecurityIdentity());
        assertThat(actual.getCatalog())
                .describedAs("Catalog")
                .isEqualTo(expected.getCatalog());
        assertThat(actual.getSchema())
                .describedAs("Schema")
                .isEqualTo(expected.getSchema());
        assertThat(actual.getExpression())
                .describedAs("Expression")
                .isEqualTo(expected.getExpression());
        assertThat(actual.getPath())
                .describedAs("Path")
                .isEqualTo(expected.getPath());
    }

    @Test
    public void testProcedureRulesForCheckCanExecute()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");

        accessControl.checkCanExecuteProcedure(BOB, new CatalogSchemaRoutineName("alice-catalog", new SchemaRoutineName("procedure-schema", "some_procedure")));
        assertAccessDenied(
                () -> accessControl.checkCanExecuteProcedure(BOB, new CatalogSchemaRoutineName("alice-catalog", new SchemaRoutineName("some-schema", "some_procedure"))),
                EXECUTE_PROCEDURE_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(
                () -> accessControl.checkCanExecuteProcedure(BOB, new CatalogSchemaRoutineName("alice-catalog", new SchemaRoutineName("procedure-schema", "another_procedure"))),
                EXECUTE_PROCEDURE_ACCESS_DENIED_MESSAGE);

        assertAccessDenied(
                () -> accessControl.checkCanExecuteProcedure(CHARLIE, new CatalogSchemaRoutineName("open-to-all", new SchemaRoutineName("some-schema", "some_procedure"))),
                EXECUTE_PROCEDURE_ACCESS_DENIED_MESSAGE);

        assertAccessDenied(
                () -> accessControl.checkCanExecuteProcedure(ALICE, new CatalogSchemaRoutineName("alice-catalog", new SchemaRoutineName("procedure-schema", "some_procedure"))),
                EXECUTE_PROCEDURE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testFunctionRulesForCheckCanExecute()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");
        assertThat(accessControl.canExecuteFunction(BOB, new CatalogSchemaRoutineName("specific-catalog", "system", "some_function"))).isTrue();

        assertThat(accessControl.canExecuteFunction(ADMIN, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ADMIN, new CatalogSchemaRoutineName("specific-catalog", "system", "some_function"))).isFalse();

        assertThat(accessControl.canExecuteFunction(ALICE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"))).isTrue();
        assertThat(accessControl.canExecuteFunction(ALICE, new CatalogSchemaRoutineName("specific-catalog", "system", "some_function"))).isFalse();

        assertThat(accessControl.canExecuteFunction(BOB, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(BOB, new CatalogSchemaRoutineName("specific-catalog", "system", "some_function"))).isTrue();

        assertThat(accessControl.canExecuteFunction(CHARLIE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(CHARLIE, new CatalogSchemaRoutineName("specific-catalog", "system", "some_function"))).isFalse();
    }

    @Test
    public void testFunctionRulesForCheckCanCreateView()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-visibility.json");
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"))).isTrue();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new CatalogSchemaRoutineName("specific-catalog", "builtin", "some_table_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new CatalogSchemaRoutineName("specific-catalog", "builtin", "some_function"))).isFalse();

        assertThat(accessControl.canCreateViewWithExecuteFunction(BOB, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_table_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(BOB, new CatalogSchemaRoutineName("ptf-catalog", "ptf_schema", "some_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(BOB, new CatalogSchemaRoutineName("specific-catalog", "builtin", "some_table_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(BOB, new CatalogSchemaRoutineName("specific-catalog", "builtin", "some_function"))).isTrue();
    }

    @Test
    public void testSchemaAuthorization()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("authorization.json");

        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "test");
        List<String> ownedByUser = List.of("some-catalog", "owned_by_user");
        List<String> ownedByGroup = List.of("some-catalog", "owned_by_group");
        List<String> ownedByRole = List.of("some-catalog", "owned_by_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "role"), new EntityKindAndName("SCHEMA", List.of(schema.getCatalogName(), schema.getSchemaName())), new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.test to ROLE new_role");

        // access to schema granted to user
        EntityKindAndName schemaOwnedByUser = new EntityKindAndName("SCHEMA", ownedByUser);
        accessControl.checkCanSetEntityAuthorization(user("owner_authorized", "group", "role"), schemaOwnedByUser, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("owner", "authorized", "role"), schemaOwnedByUser, new TrinoPrincipal(ROLE, "new_role"));
        accessControl.checkCanSetEntityAuthorization(user("owner", "group", "authorized"), schemaOwnedByUser, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner_without_authorization_access", "group", "role"), schemaOwnedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner_DENY_authorized", "group", "role"), schemaOwnedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_user to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner", "DENY_authorized", "role"), schemaOwnedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner", "group", "DENY_authorized"), schemaOwnedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner", "group", "authorized"), schemaOwnedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_user to USER new_user");

        // access to schema granted to group
        EntityKindAndName schemaOwnedByGroup = new EntityKindAndName("SCHEMA", ownedByGroup);
        accessControl.checkCanSetEntityAuthorization(user("authorized", "owner", "role"), schemaOwnedByGroup, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("user", "owner", "authorized"), schemaOwnedByGroup, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "role"), schemaOwnedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("DENY_authorized", "owner", "role"), schemaOwnedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_group to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "DENY_authorized"), schemaOwnedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "authorized"), schemaOwnedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_group to USER new_user");

        // access to schema granted to role
        EntityKindAndName schemaOwnedByRole = new EntityKindAndName("SCHEMA", ownedByRole);
        accessControl.checkCanSetEntityAuthorization(user("authorized", "group", "owner"), schemaOwnedByRole, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_authorized"), schemaOwnedByRole, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner"), schemaOwnedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("DENY_authorized", "group", "owner"), schemaOwnedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_role to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_DENY_authorized"), schemaOwnedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for schema some-catalog.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_authorized"), schemaOwnedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for schema some-catalog.owned_by_role to USER new_user");
    }

    @Test
    public void testTableAuthorization()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("authorization.json");

        List<String> table = List.of("some-catalog", "test", "table");
        List<String> ownedByUser = List.of("some-catalog", "test", "owned_by_user");
        List<String> ownedByGroup = List.of("some-catalog", "test", "owned_by_group");
        List<String> ownedByRole = List.of("some-catalog", "test", "owned_by_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "role"), new EntityKindAndName("TABLE", table), new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.table to ROLE new_role");

        // access to table granted to user
        EntityKindAndName tableOwnedByUser = new EntityKindAndName("TABLE", ownedByUser);
        accessControl.checkCanSetEntityAuthorization(user("owner_authorized", "group", "role"), tableOwnedByUser, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("owner", "group", "authorized"), tableOwnedByUser, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner_without_authorization_access", "group", "role"), tableOwnedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner_DENY_authorized", "group", "role"), tableOwnedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner", "group", "DENY_authorized"), tableOwnedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner", "group", "authorized"), tableOwnedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_user to USER new_user");

        // access to table granted to group
        EntityKindAndName tableOwnedByGroup = new EntityKindAndName("TABLE", ownedByGroup);
        accessControl.checkCanSetEntityAuthorization(user("authorized", "owner", "role"), tableOwnedByGroup, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("user", "owner", "authorized"), tableOwnedByGroup, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "role"), tableOwnedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("DENY_authorized", "owner", "role"), tableOwnedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "DENY_authorized"), tableOwnedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "authorized"), tableOwnedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_group to USER new_user");

        // access to table granted to role
        EntityKindAndName tableOwnedByRole = new EntityKindAndName("TABLE", ownedByRole);
        accessControl.checkCanSetEntityAuthorization(user("authorized", "group", "owner"), tableOwnedByRole, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_authorized"), tableOwnedByRole, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner"), tableOwnedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("DENY_authorized", "group", "owner"), tableOwnedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to USER new_user");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_DENY_authorized"), tableOwnedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to ROLE new_role");
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_authorized"), tableOwnedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for table some-catalog.test.owned_by_role to USER new_user");
    }

    @Test
    public void testViewAuthorization()
            throws Exception
    {
        assertViewSetEntityAuthorization("VIEW");
    }

    @Test
    public void testMaterializedViewAuthorization()
            throws Exception
    {
        assertViewSetEntityAuthorization("MATERIALIZED VIEW");
    }

    private void assertViewSetEntityAuthorization(String entityKind)
            throws URISyntaxException
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("authorization.json");

        List<String> table = List.of("some-catalog", "test", "table");
        List<String> ownedByUser = List.of("some-catalog", "test", "owned_by_user");
        List<String> ownedByGroup = List.of("some-catalog", "test", "owned_by_group");
        List<String> ownedByRole = List.of("some-catalog", "test", "owned_by_role");

        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "role"), new EntityKindAndName(entityKind, table), new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for %s some-catalog.test.table to ROLE new_role".formatted(entityKind.toLowerCase(Locale.ROOT)));

        // access to table granted to user
        EntityKindAndName viewOwnedByUser = new EntityKindAndName(entityKind, ownedByUser);
        accessControl.checkCanSetEntityAuthorization(user("owner_authorized", "group", "role"), viewOwnedByUser, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("owner", "group", "authorized"), viewOwnedByUser, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner_without_authorization_access", "group", "role"), viewOwnedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for %s some-catalog.test.owned_by_user to ROLE new_role".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner_DENY_authorized", "group", "role"), viewOwnedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for %s some-catalog.test.owned_by_user to USER new_user".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner", "group", "DENY_authorized"), viewOwnedByUser, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for %s some-catalog.test.owned_by_user to ROLE new_role".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("owner", "group", "authorized"), viewOwnedByUser, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for %s some-catalog.test.owned_by_user to USER new_user".formatted(entityKind.toLowerCase(Locale.ROOT)));

        // access to table granted to group
        EntityKindAndName viewOwnedByGroup = new EntityKindAndName(entityKind, ownedByGroup);
        accessControl.checkCanSetEntityAuthorization(user("authorized", "owner", "role"), viewOwnedByGroup, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("user", "owner", "authorized"), viewOwnedByGroup, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "role"), viewOwnedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for %s some-catalog.test.owned_by_group to ROLE new_role".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("DENY_authorized", "owner", "role"), viewOwnedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for %s some-catalog.test.owned_by_group to USER new_user".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "DENY_authorized"), viewOwnedByGroup, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for %s some-catalog.test.owned_by_group to ROLE new_role".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "owner", "authorized"), viewOwnedByGroup, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for %s some-catalog.test.owned_by_group to USER new_user".formatted(entityKind.toLowerCase(Locale.ROOT)));

        // access to table granted to role
        EntityKindAndName viewOwnedByRole = new EntityKindAndName(entityKind, ownedByRole);
        accessControl.checkCanSetEntityAuthorization(user("authorized", "group", "owner"), viewOwnedByRole, new TrinoPrincipal(USER, "new_user"));
        accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_authorized"), viewOwnedByRole, new TrinoPrincipal(ROLE, "new_role"));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner"), viewOwnedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for %s some-catalog.test.owned_by_role to ROLE new_role".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("DENY_authorized", "group", "owner"), viewOwnedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for %s some-catalog.test.owned_by_role to USER new_user".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_DENY_authorized"), viewOwnedByRole, new TrinoPrincipal(ROLE, "new_role")),
                "Cannot set authorization for %s some-catalog.test.owned_by_role to ROLE new_role".formatted(entityKind.toLowerCase(Locale.ROOT)));
        assertAccessDenied(
                () -> accessControl.checkCanSetEntityAuthorization(user("user", "group", "owner_authorized"), viewOwnedByRole, new TrinoPrincipal(USER, "new_user")),
                "Cannot set authorization for %s some-catalog.test.owned_by_role to USER new_user".formatted(entityKind.toLowerCase(Locale.ROOT)));
    }

    @Test
    public void testFunctionsFilter()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-function-filter.json");
        Set<SchemaFunctionName> functions = ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("restricted", "any"))
                .add(new SchemaFunctionName("secret", "any"))
                .add(new SchemaFunctionName("aliceschema", "any"))
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .add(new SchemaFunctionName("bobschema", "bob_any"))
                .add(new SchemaFunctionName("bobschema", "any"))
                .add(new SchemaFunctionName("any", "any"))
                .build();
        assertThat(accessControl.filterFunctions(ALICE, "any", functions)).isEqualTo(ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("aliceschema", "any"))
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .build());
        assertThat(accessControl.filterFunctions(BOB, "any", functions)).isEqualTo(ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .add(new SchemaFunctionName("bobschema", "bob_any"))
                .build());
        assertThat(accessControl.filterFunctions(ADMIN, "any", functions)).isEqualTo(ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("secret", "any"))
                .add(new SchemaFunctionName("aliceschema", "any"))
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .add(new SchemaFunctionName("bobschema", "bob_any"))
                .add(new SchemaFunctionName("bobschema", "any"))
                .add(new SchemaFunctionName("any", "any"))
                .build());
    }

    @Test
    public void testFunctionsFilterNoAccess()
            throws Exception
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-no-access.json");

        Set<SchemaFunctionName> functions = ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("restricted", "any"))
                .add(new SchemaFunctionName("secret", "any"))
                .add(new SchemaFunctionName("any", "any"))
                .build();
        assertThat(accessControl.filterFunctions(ALICE, "any", functions)).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterFunctions(BOB, "any", functions)).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testAuthorizationDocsExample()
    {
        Path rulesFile = Paths.get("../../docs/src/main/sphinx/security/authorization.json");
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(rulesFile, ImmutableMap.of());
        List<String> schema = List.of("catalog", "schema");
        List<String> tableOrView = List.of("catalog", "schema", "table_or_view");
        accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("SCHEMA", schema), new TrinoPrincipal(USER, "alice"));
        accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("SCHEMA", schema), new TrinoPrincipal(ROLE, "role"));
        accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("TABLE", tableOrView), new TrinoPrincipal(USER, "alice"));
        accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("TABLE", tableOrView), new TrinoPrincipal(ROLE, "role"));
        accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("VIEW", tableOrView), new TrinoPrincipal(USER, "alice"));
        accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("VIEW", tableOrView), new TrinoPrincipal(ROLE, "role"));
        assertAccessDenied(
                () -> accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("SCHEMA", schema), new TrinoPrincipal(USER, "bob")),
                "Cannot set authorization for schema catalog.schema to USER bob");
        assertAccessDenied(
                () -> accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("TABLE", tableOrView), new TrinoPrincipal(USER, "bob")),
                "Cannot set authorization for table catalog.schema.table_or_view to USER bob");
        assertAccessDenied(
                () -> accessControlManager.checkCanSetEntityAuthorization(ADMIN, new EntityKindAndName("VIEW", tableOrView), new TrinoPrincipal(USER, "bob")),
                "Cannot set authorization for view catalog.schema.table_or_view to USER bob");
    }

    private static SystemSecurityContext user(String user, String group, String role)
    {
        Identity identity = Identity.forUser(user)
                .withGroups(ImmutableSet.of(group))
                .withEnabledRoles(ImmutableSet.of(role))
                .build();
        return new SystemSecurityContext(identity, queryId, queryStart);
    }

    @Test
    public void parseUnknownRules()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("file-based-system-security-config-file-with-unknown-rules.json"))
                .hasMessageContaining("Failed to convert JSON tree node");
    }

    @Test
    public void testTableRulesForCheckCanInsertIntoTableWithJsonPointer()
            throws Exception
    {
        Path configFile = getResourcePath("file-based-system-access-table-with-json-pointer.json");
        SystemAccessControl accessControl = newFileBasedSystemAccessControl(configFile, ImmutableMap.of("security.json-pointer", "/data"));
        assertTableRulesForCheckCanInsertIntoTable(accessControl);
    }

    protected SystemAccessControl newFileBasedSystemAccessControl(String rulesName)
            throws URISyntaxException
    {
        Path configFile = getResourcePath(rulesName);
        return newFileBasedSystemAccessControl(configFile, ImmutableMap.of());
    }

    protected SystemAccessControl newFileBasedSystemAccessControl(Map<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config, new TestingSystemAccessControlContext());
    }

    protected Path getResourcePath(String resourceName)
            throws URISyntaxException
    {
        return Paths.get(requireNonNull(this.getClass().getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).toURI());
    }

    private static void assertAccessDenied(ThrowingCallable callable, String expectedMessage)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied: " + expectedMessage);
    }
}
