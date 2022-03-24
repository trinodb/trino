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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
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
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.io.Files.copy;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Files.newTemporaryFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFileBasedSystemAccessControl
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

    private static final String SHOWN_SCHEMAS_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot show schemas";
    private static final String CREATE_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot create schema .*";
    private static final String DROP_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot drop schema .*";
    private static final String RENAME_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot rename schema from .* to .*";
    private static final String AUTH_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set authorization for schema .* to .*";
    private static final String SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot show create schema for .*";
    private static final String GRANT_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot grant privilege %s on schema %s%s";
    private static final String DENY_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot deny privilege %s on schema %s%s";
    private static final String REVOKE_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot revoke privilege %s on schema %s%s";

    private static final String SHOWN_TABLES_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot show tables of .*";
    private static final String SELECT_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot select from table .*";
    private static final String SHOW_COLUMNS_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot show columns of table .*";
    private static final String ADD_COLUMNS_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot add a column to table .*";
    private static final String DROP_COLUMNS_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot drop a column from table .*";
    private static final String RENAME_COLUMNS_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot rename a column in table .*";
    private static final String AUTH_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set authorization for table .* to .*";
    private static final String AUTH_VIEW_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set authorization for view .* to .*";
    private static final String TABLE_COMMENT_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot comment table to .*";
    private static final String INSERT_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot insert into table .*";
    private static final String DELETE_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot delete from table .*";
    private static final String TRUNCATE_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot truncate table .*";
    private static final String DROP_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot drop table .*";
    private static final String CREATE_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot show create table for .*";
    private static final String RENAME_TABLE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot rename table .*";
    private static final String SET_TABLE_PROPERTIES_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set table properties to .*";
    private static final String CREATE_VIEW_ACCESS_DENIED_MESSAGE = "Access Denied: View owner '.*' cannot create view that selects from .*";
    private static final String CREATE_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot create materialized view .*";
    private static final String DROP_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot drop materialized view .*";
    private static final String REFRESH_MATERIALIZED_VIEW_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot refresh materialized view .*";
    private static final String SET_MATERIALIZED_VIEW_PROPERTIES_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set properties of materialized view .*";
    private static final String GRANT_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot grant privilege DELETE on table .*";
    private static final String DENY_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot deny privilege DELETE on table .*";
    private static final String REVOKE_DELETE_PRIVILEGE_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot revoke privilege DELETE on table .*";

    private static final String SET_SYSTEM_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set system session property .*";
    private static final String SET_CATALOG_SESSION_PROPERTY_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set catalog session property .*";

    @Test
    public void testEmptyFile()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("empty.json");

        accessControl.checkCanCreateSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"));
        accessControl.checkCanDropSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"));
        accessControl.checkCanRenameSchema(UNKNOWN, new CatalogSchemaName("some-catalog", "unknown"), "new_unknown");
        accessControl.checkCanSetSchemaAuthorization(UNKNOWN,
                new CatalogSchemaName("some-catalog", "unknown"),
                new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
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

        accessControl.checkCanCreateMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"), Map.of());
        accessControl.checkCanDropMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));
        accessControl.checkCanRefreshMaterializedView(UNKNOWN, new CatalogSchemaTableName("some-catalog", "unknown", "unknown"));

        accessControl.checkCanSetUser(Optional.empty(), "unknown");
        accessControl.checkCanSetUser(Optional.of(new KerberosPrincipal("stuff@example.com")), "unknown");

        accessControl.checkCanSetSystemSessionProperty(UNKNOWN, "anything");
        accessControl.checkCanSetCatalogSessionProperty(UNKNOWN, "unknown", "anything");

        accessControl.checkCanExecuteQuery(UNKNOWN);
        accessControl.checkCanViewQueryOwnedBy(UNKNOWN, anyone);
        accessControl.checkCanKillQueryOwnedBy(UNKNOWN, anyone);

        // system information access is denied by default
        assertThatThrownBy(() -> accessControl.checkCanReadSystemInformation(UNKNOWN))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot read system information");
        assertThatThrownBy(() -> accessControl.checkCanWriteSystemInformation(UNKNOWN))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");
    }

    @Test
    public void testSchemaRulesForCheckCanCreateSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"));
        accessControl.checkCanCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"));

        accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(BOB, new CatalogSchemaName("some-catalog", "test")), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob")), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff")), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test")), CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
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
    public void testSchemaRulesForCheckCanSetSchemaAuthorization()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanSetSchemaAuthorization(ADMIN, new CatalogSchemaName("some-catalog", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetSchemaAuthorization(ADMIN, new CatalogSchemaName("some-catalog", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user"));
        accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "bob"), new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "bob"), new TrinoPrincipal(PrincipalType.USER, "some_user"));
        assertAccessDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role")), AUTH_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user")), AUTH_SCHEMA_ACCESS_DENIED_MESSAGE);
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
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.USER, "alice");

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
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.USER, "alice");

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
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.USER, "alice");

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
    public void testCheckCanSetTableAuthorizationForAdmin()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetTableAuthorization(ADMIN, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetTableAuthorization(ADMIN, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user"));
    }

    @Test
    public void testCheckCanSetViewAuthorizationForAdmin()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetViewAuthorization(ADMIN, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetViewAuthorization(ADMIN, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user"));
    }

    @Test
    public void testCheckCanSetTableAuthorizationForOwner()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetTableAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetTableAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user"));
    }

    @Test
    public void testCheckCanSetViewAuthorizationForOwner()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        accessControl.checkCanSetViewAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetViewAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "aliceschema", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user"));
    }

    @Test
    public void testCheckCanSetTableAuthorizationForNonOwner()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertAccessDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role")), AUTH_TABLE_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user")), AUTH_TABLE_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testCheckCanSetViewAuthorizationForNonOwner()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertAccessDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.ROLE, "some_role")), AUTH_VIEW_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, new CatalogSchemaTableName("some-catalog", "test", "test"), new TrinoPrincipal(PrincipalType.USER, "some_user")), AUTH_VIEW_ACCESS_DENIED_MESSAGE);
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

        try {
            accessControl.checkCanSetUser(Optional.empty(), alice.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
        accessControl.checkCanSetUser(kerberosValidNonAsciiUser.getPrincipal(), kerberosValidNonAsciiUser.getUser());
        try {
            accessControl.checkCanSetUser(kerberosInvalidAlice.getPrincipal(), kerberosInvalidAlice.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSetUser(kerberosValidShare.getPrincipal(), kerberosValidShare.getUser());
        try {
            accessControl.checkCanSetUser(kerberosInValidShare.getPrincipal(), kerberosInValidShare.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSetUser(validSpecialRegexWildDot.getPrincipal(), validSpecialRegexWildDot.getUser());
        accessControl.checkCanSetUser(validSpecialRegexEndQuote.getPrincipal(), validSpecialRegexEndQuote.getUser());
        try {
            accessControl.checkCanSetUser(invalidSpecialRegex.getPrincipal(), invalidSpecialRegex.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        SystemAccessControl accessControlNoPatterns = newFileBasedSystemAccessControl("file-based-system-catalog.json");
        accessControlNoPatterns.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
    }

    @Test
    public void testQuery()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("query.json");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(admin, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(admin, queryId), any);

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(alice, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(alice, queryId), any))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");

        assertThatThrownBy(() -> accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob, queryId)))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), any))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob, queryId), any);

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(dave, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice);
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), dave);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), ImmutableSet.of("alice", "bob", "dave", "admin")),
                ImmutableSet.of("alice", "dave"));
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), admin))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");

        Identity contractor = Identity.forUser("some-other-contractor").withGroups(ImmutableSet.of("contractors")).build();
        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(contractor, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave);
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(nonAsciiUser, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), any);
    }

    @Test
    public void testInvalidQuery()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("query-invalid.json"))
                .getRootCause().hasMessage("A valid query rule cannot combine an queryOwner condition with access mode 'execute'");
    }

    @Test
    public void testQueryNotSet()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("file-based-system-catalog.json");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob, queryId), any);
    }

    @Test
    public void testQueryDocsExample()
    {
        String rulesFile = new File("../../docs/src/main/sphinx/security/query-access.json").getAbsolutePath();
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", rulesFile));

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(admin, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), any);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(admin, queryId), any);

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(alice, queryId));
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), any))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(alice, queryId), any);

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob, queryId));
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), any))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of());
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob, queryId), any))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(dave, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice);
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), dave);
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), ImmutableSet.of("alice", "bob", "dave", "admin")),
                ImmutableSet.of("alice", "dave"));
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), alice))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), bob))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(dave, queryId), admin))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");

        Identity contractor = Identity.forUser("some-other-contractor").withGroups(ImmutableSet.of("contractors")).build();
        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(contractor, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave);
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(contractor, queryId), dave))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot view query");
    }

    @Test
    public void testSystemInformation()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("system-information.json");

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(admin, Optional.empty()));
        accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(admin, Optional.empty()));

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(alice, Optional.empty()));
        assertThatThrownBy(() -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(alice, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");

        assertThatThrownBy(() -> accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(bob, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot read system information");
        assertThatThrownBy(() -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(bob, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(nonAsciiUser, Optional.empty()));
        accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(nonAsciiUser, Optional.empty()));
    }

    @Test
    public void testSystemInformationNotSet()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("file-based-system-catalog.json");

        assertThatThrownBy(() -> accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(bob, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot read system information");
        assertThatThrownBy(() -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(bob, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");
    }

    @Test
    public void testSystemInformationDocsExample()
    {
        String rulesFile = new File("../../docs/src/main/sphinx/security/system-information-access.json").getAbsolutePath();
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", rulesFile));

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(admin, Optional.empty()));
        accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(admin, Optional.empty()));

        accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(alice, Optional.empty()));
        assertThatThrownBy(() -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(alice, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");

        assertThatThrownBy(() -> accessControlManager.checkCanReadSystemInformation(new SystemSecurityContext(bob, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot read system information");
        assertThatThrownBy(() -> accessControlManager.checkCanWriteSystemInformation(new SystemSecurityContext(bob, Optional.empty())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot write system information");
    }

    @Test
    public void testSchemaOperations()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-catalog.json");

        TrinoPrincipal user = new TrinoPrincipal(PrincipalType.USER, "some_user");
        TrinoPrincipal role = new TrinoPrincipal(PrincipalType.ROLE, "some_user");

        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(admin, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), user);
        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(admin, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), role);

        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), user);
        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), role);

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(bob, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), user))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema alice-catalog.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(bob, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), role))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema alice-catalog.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("secret", "some_schema"), user))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema secret.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("secret", "some_schema"), role))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema secret.some_schema");
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
        String rulesFile = new File("../../docs/src/main/sphinx/security/session-property-access.json").getAbsolutePath();
        SystemAccessControl accessControl = newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", rulesFile));
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
                "unknown");

        assertEquals(accessControl.filterCatalogs(ADMIN, allCatalogs), Sets.difference(allCatalogs, ImmutableSet.of("blocked-catalog")));
        Set<String> aliceCatalogs = ImmutableSet.of("specific-catalog", "alice-catalog");
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
                        VARCHAR),
                new ViewExpression(CHARLIE.getIdentity().getUser(), Optional.of("some-catalog"), Optional.of("bobschema"), "'mask'"));

        assertViewExpressionEquals(
                accessControl.getColumnMask(
                        CHARLIE,
                        new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns"),
                        "masked_with_user",
                        VARCHAR),
                new ViewExpression("mask-user", Optional.of("some-catalog"), Optional.of("bobschema"), "'mask-with-user'"));
    }

    @Test
    public void testGetRowFilter()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-table.json");

        assertEquals(
                accessControl.getRowFilter(ALICE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns")),
                Optional.empty());

        assertViewExpressionEquals(
                accessControl.getRowFilter(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns")),
                new ViewExpression(CHARLIE.getIdentity().getUser(), Optional.of("some-catalog"), Optional.of("bobschema"), "starts_with(value, 'filter')"));

        assertViewExpressionEquals(
                accessControl.getRowFilter(CHARLIE, new CatalogSchemaTableName("some-catalog", "bobschema", "bobcolumns_with_grant")),
                new ViewExpression("filter-user", Optional.of("some-catalog"), Optional.of("bobschema"), "starts_with(value, 'filter-with-user')"));
    }

    private static void assertViewExpressionEquals(Optional<ViewExpression> result, ViewExpression expected)
    {
        assertTrue(result.isPresent());
        ViewExpression actual = result.get();
        assertEquals(actual.getIdentity(), expected.getIdentity(), "Identity");
        assertEquals(actual.getCatalog(), expected.getCatalog(), "Catalog");
        assertEquals(actual.getSchema(), expected.getSchema(), "Schema");
        assertEquals(actual.getExpression(), expected.getExpression(), "Expression");
    }

    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(SystemAccessControl.class, FileBasedSystemAccessControl.class, ImmutableSet.of(
                FileBasedSystemAccessControl.class.getMethod("checkCanViewQueryOwnedBy", SystemSecurityContext.class, Identity.class),
                FileBasedSystemAccessControl.class.getMethod("filterViewQueryOwnedBy", SystemSecurityContext.class, Collection.class),
                FileBasedSystemAccessControl.class.getMethod("checkCanKillQueryOwnedBy", SystemSecurityContext.class, Identity.class)));
    }

    @Test
    public void testRefreshing()
            throws Exception
    {
        File configFile = newTemporaryFile();
        configFile.deleteOnExit();
        copy(new File(getResourcePath("file-based-system-catalog.json")), configFile);

        SystemAccessControl accessControl = newFileBasedSystemAccessControl(ImmutableMap.of(
                SECURITY_CONFIG_FILE, configFile.getAbsolutePath(),
                SECURITY_REFRESH_PERIOD, "1ms"));

        SystemSecurityContext alice = new SystemSecurityContext(TestFileBasedSystemAccessControl.alice, queryId);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);

        copy(new File(getResourcePath("file-based-system-security-config-file-with-unknown-rules.json")), configFile);
        sleep(2);

        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");

        // test if file based cached control was not cached somewhere
        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");

        copy(new File(getResourcePath("file-based-system-catalog.json")), configFile);
        sleep(2);

        accessControl.checkCanCreateView(alice, aliceView);
    }

    @Test
    public void parseUnknownRules()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("file-based-system-security-config-file-with-unknown-rules.json"))
                .hasMessageContaining("Invalid JSON");
    }

    private SystemAccessControl newFileBasedSystemAccessControl(String resourceName)
    {
        return newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", getResourcePath(resourceName)));
    }

    private SystemAccessControl newFileBasedSystemAccessControl(ImmutableMap<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config);
    }

    private String getResourcePath(String resourceName)
    {
        return requireNonNull(this.getClass().getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).getPath();
    }

    private static void assertAccessDenied(ThrowingCallable callable, String expectedMessage)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching(expectedMessage);
    }
}
