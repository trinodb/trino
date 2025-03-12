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
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Thread.sleep;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseFileBasedConnectorAccessControlTest
{
    private static final ConnectorSecurityContext ADMIN = user("admin", ImmutableSet.of("admin", "staff"));
    private static final ConnectorSecurityContext ALICE = user("alice", ImmutableSet.of("staff"));
    private static final ConnectorSecurityContext BOB = user("bob", ImmutableSet.of("staff"));
    private static final ConnectorSecurityContext CHARLIE = user("charlie", ImmutableSet.of("guests"));
    private static final ConnectorSecurityContext JOE = user("joe", ImmutableSet.of());
    private static final ConnectorSecurityContext UNKNOWN = user("unknown", ImmutableSet.of());

    protected abstract ConnectorAccessControl createAccessControl(Path configFile, Map<String, String> properties);

    @Test
    public void testEmptyFile()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("empty.json");

        accessControl.checkCanCreateSchema(UNKNOWN, "unknown", ImmutableMap.of());
        accessControl.checkCanDropSchema(UNKNOWN, "unknown");
        accessControl.checkCanRenameSchema(UNKNOWN, "unknown", "new_unknown");
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(UNKNOWN, "unknown", new TrinoPrincipal(ROLE, "some_role")));
        accessControl.checkCanShowCreateSchema(UNKNOWN, "unknown");

        accessControl.checkCanSelectFromColumns(UNKNOWN, new SchemaTableName("unknown", "unknown"), ImmutableSet.of());
        accessControl.checkCanShowColumns(UNKNOWN, new SchemaTableName("unknown", "unknown"));
        accessControl.checkCanInsertIntoTable(UNKNOWN, new SchemaTableName("unknown", "unknown"));
        accessControl.checkCanDeleteFromTable(UNKNOWN, new SchemaTableName("unknown", "unknown"));

        accessControl.checkCanCreateTable(UNKNOWN, new SchemaTableName("unknown", "unknown"), Map.of());
        accessControl.checkCanDropTable(UNKNOWN, new SchemaTableName("unknown", "unknown"));
        accessControl.checkCanTruncateTable(UNKNOWN, new SchemaTableName("unknown", "unknown"));
        accessControl.checkCanRenameTable(UNKNOWN,
                new SchemaTableName("unknown", "unknown"),
                new SchemaTableName("unknown", "new_unknown"));
        accessControl.checkCanAlterColumn(UNKNOWN, new SchemaTableName("unknown", "unknown"));

        accessControl.checkCanSetCatalogSessionProperty(UNKNOWN, "anything");

        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertThat(accessControl.filterTables(UNKNOWN, tables)).isEqualTo(tables);

        // permissions management APIs are hard coded to deny
        TrinoPrincipal someUser = new TrinoPrincipal(USER, "some_user");
        assertDenied(() -> accessControl.checkCanGrantTablePrivilege(ADMIN, Privilege.SELECT, new SchemaTableName("any", "any"), someUser, false));
        assertDenied(() -> accessControl.checkCanDenyTablePrivilege(ADMIN, Privilege.SELECT, new SchemaTableName("any", "any"), someUser));
        assertDenied(() -> accessControl.checkCanRevokeTablePrivilege(ADMIN, Privilege.SELECT, new SchemaTableName("any", "any"), someUser, false));
        assertDenied(() -> accessControl.checkCanCreateRole(ADMIN, "role", Optional.empty()));
        assertDenied(() -> accessControl.checkCanDropRole(ADMIN, "role"));
        assertDenied(() -> accessControl.checkCanGrantRoles(
                ADMIN,
                ImmutableSet.of("test"),
                ImmutableSet.of(someUser),
                false,
                Optional.empty()));
        assertDenied(() -> accessControl.checkCanRevokeRoles(
                ADMIN,
                ImmutableSet.of("test"),
                ImmutableSet.of(someUser),
                false,
                Optional.empty()));
        assertDenied(() -> accessControl.checkCanSetRole(ADMIN, "role"));

        // showing roles and permissions is hard coded to allow
        accessControl.checkCanShowRoles(UNKNOWN);
        accessControl.checkCanShowCurrentRoles(UNKNOWN);
        accessControl.checkCanShowRoleGrants(UNKNOWN);
    }

    @Test
    public void testSchemaRules()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");

        Map<String, Object> properties = ImmutableMap.of();
        accessControl.checkCanCreateSchema(ADMIN, "bob", properties);
        accessControl.checkCanCreateSchema(ADMIN, "staff", properties);
        accessControl.checkCanCreateSchema(ADMIN, "authenticated", properties);
        accessControl.checkCanCreateSchema(ADMIN, "test", properties);

        accessControl.checkCanCreateSchema(BOB, "bob", properties);
        accessControl.checkCanCreateSchema(BOB, "staff", properties);
        accessControl.checkCanCreateSchema(BOB, "authenticated", properties);
        assertDenied(() -> accessControl.checkCanCreateSchema(BOB, "test", properties));

        assertDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, "bob", properties));
        assertDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, "staff", properties));
        accessControl.checkCanCreateSchema(CHARLIE, "authenticated", properties);
        assertDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, "test", properties));

        accessControl.checkCanDropSchema(ADMIN, "bob");
        accessControl.checkCanDropSchema(ADMIN, "staff");
        accessControl.checkCanDropSchema(ADMIN, "authenticated");
        accessControl.checkCanDropSchema(ADMIN, "test");

        accessControl.checkCanDropSchema(BOB, "bob");
        accessControl.checkCanDropSchema(BOB, "staff");
        accessControl.checkCanDropSchema(BOB, "authenticated");
        assertDenied(() -> accessControl.checkCanDropSchema(BOB, "test"));

        assertDenied(() -> accessControl.checkCanDropSchema(CHARLIE, "bob"));
        assertDenied(() -> accessControl.checkCanDropSchema(CHARLIE, "staff"));
        accessControl.checkCanDropSchema(CHARLIE, "authenticated");
        assertDenied(() -> accessControl.checkCanDropSchema(CHARLIE, "test"));

        accessControl.checkCanRenameSchema(ADMIN, "bob", "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, "staff", "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, "authenticated", "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, "test", "new_schema");

        accessControl.checkCanRenameSchema(BOB, "bob", "staff");
        accessControl.checkCanRenameSchema(BOB, "staff", "authenticated");
        accessControl.checkCanRenameSchema(BOB, "authenticated", "bob");
        assertDenied(() -> accessControl.checkCanRenameSchema(BOB, "test", "bob"));
        assertDenied(() -> accessControl.checkCanRenameSchema(BOB, "bob", "test"));

        assertDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, "bob", "new_schema"));
        assertDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, "staff", "new_schema"));
        accessControl.checkCanRenameSchema(CHARLIE, "authenticated", "authenticated");
        assertDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, "test", "new_schema"));

        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(ADMIN, "test", new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(ADMIN, "test", new TrinoPrincipal(USER, "some_user")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, "bob", new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, "bob", new TrinoPrincipal(USER, "some_user")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, "test", new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, "test", new TrinoPrincipal(USER, "some_user")));

        accessControl.checkCanShowCreateSchema(ADMIN, "bob");
        accessControl.checkCanShowCreateSchema(ADMIN, "staff");
        accessControl.checkCanShowCreateSchema(ADMIN, "authenticated");
        accessControl.checkCanShowCreateSchema(ADMIN, "test");

        accessControl.checkCanShowCreateSchema(BOB, "bob");
        accessControl.checkCanShowCreateSchema(BOB, "staff");
        accessControl.checkCanShowCreateSchema(BOB, "authenticated");
        assertDenied(() -> accessControl.checkCanShowCreateSchema(BOB, "test"));

        assertDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, "bob"));
        assertDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, "staff"));
        accessControl.checkCanShowCreateSchema(CHARLIE, "authenticated");
        assertDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, "test"));
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
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(USER, "alice");

        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, "bob", grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, "staff", grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, "authenticated", grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(ADMIN, privilege, "test", grantee, grantOption);

        accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, "bob", grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, "staff", grantee, grantOption);
        accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, "authenticated", grantee, grantOption);
        assertDenied(() -> accessControl.checkCanGrantSchemaPrivilege(BOB, privilege, "test", grantee, grantOption));

        assertDenied(() -> accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, "bob", grantee, grantOption));
        assertDenied(() -> accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, "staff", grantee, grantOption));
        accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, "authenticated", grantee, grantOption);
        assertDenied(() -> accessControl.checkCanGrantSchemaPrivilege(CHARLIE, privilege, "test", grantee, grantOption));
    }

    @Test
    public void testDenySchemaPrivilege()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(USER, "alice");

        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, "bob", grantee);
        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, "staff", grantee);
        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, "authenticated", grantee);
        accessControl.checkCanDenySchemaPrivilege(ADMIN, UPDATE, "test", grantee);

        accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, "bob", grantee);
        accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, "staff", grantee);
        accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, "authenticated", grantee);
        assertDenied(() -> accessControl.checkCanDenySchemaPrivilege(BOB, UPDATE, "test", grantee));

        assertDenied(() -> accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, "bob", grantee));
        assertDenied(() -> accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, "staff", grantee));
        accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, "authenticated", grantee);
        assertDenied(() -> accessControl.checkCanDenySchemaPrivilege(CHARLIE, UPDATE, "test", grantee));
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
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(USER, "alice");

        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, "bob", grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, "staff", grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, "authenticated", grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(ADMIN, privilege, "test", grantee, grantOption);

        accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, "bob", grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, "staff", grantee, grantOption);
        accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, "authenticated", grantee, grantOption);
        assertDenied(() -> accessControl.checkCanRevokeSchemaPrivilege(BOB, privilege, "test", grantee, grantOption));

        assertDenied(() -> accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, "bob", grantee, grantOption));
        assertDenied(() -> accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, "staff", grantee, grantOption));
        accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, "authenticated", grantee, grantOption);
        assertDenied(() -> accessControl.checkCanRevokeSchemaPrivilege(CHARLIE, privilege, "test", grantee, grantOption));
    }

    @Test
    public void testTableRules()
            throws Exception
    {
        SchemaTableName testTable = new SchemaTableName("test", "test");
        SchemaTableName aliceTable = new SchemaTableName("aliceschema", "alicetable");
        SchemaTableName bobTable = new SchemaTableName("bobschema", "bobtable");

        ConnectorAccessControl accessControl = createAccessControl("table.json");
        accessControl.checkCanSelectFromColumns(ALICE, testTable, ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(ALICE, bobTable, ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(ALICE, bobTable, ImmutableSet.of("bobcolumn"));

        accessControl.checkCanShowColumns(ALICE, bobTable);
        assertThat(accessControl.filterColumns(ALICE, Map.of(bobTable, ImmutableSet.of("a"))))
                .isEqualTo(Map.of(bobTable, ImmutableSet.of("a")));
        accessControl.checkCanSelectFromColumns(BOB, bobTable, ImmutableSet.of());
        accessControl.checkCanShowColumns(BOB, bobTable);
        assertThat(accessControl.filterColumns(BOB, Map.of(bobTable, ImmutableSet.of("a"))))
                .isEqualTo(Map.of(bobTable, ImmutableSet.of("a")));

        accessControl.checkCanInsertIntoTable(BOB, bobTable);
        accessControl.checkCanDeleteFromTable(BOB, bobTable);
        accessControl.checkCanTruncateTable(BOB, bobTable);
        accessControl.checkCanSelectFromColumns(CHARLIE, bobTable, ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(CHARLIE, bobTable, ImmutableSet.of("bobcolumn"));
        accessControl.checkCanInsertIntoTable(CHARLIE, bobTable);
        accessControl.checkCanSelectFromColumns(JOE, bobTable, ImmutableSet.of());

        accessControl.checkCanCreateTable(ADMIN, new SchemaTableName("bob", "test"), Map.of());
        accessControl.checkCanCreateTable(ADMIN, testTable, Map.of());
        accessControl.checkCanCreateTable(ADMIN, new SchemaTableName("authenticated", "test"), Map.of());
        assertDenied(() -> accessControl.checkCanCreateTable(ADMIN, new SchemaTableName("secret", "test"), Map.of()));

        accessControl.checkCanCreateTable(ALICE, new SchemaTableName("aliceschema", "test"), Map.of());
        assertDenied(() -> accessControl.checkCanCreateTable(ALICE, testTable, Map.of()));
        assertDenied(() -> accessControl.checkCanCreateTable(CHARLIE, new SchemaTableName("aliceschema", "test"), Map.of()));
        assertDenied(() -> accessControl.checkCanCreateTable(CHARLIE, testTable, Map.of()));

        accessControl.checkCanCreateViewWithSelectFromColumns(BOB, bobTable, ImmutableSet.of());
        accessControl.checkCanDropTable(ADMIN, bobTable);
        accessControl.checkCanTruncateTable(ADMIN, bobTable);

        accessControl.checkCanRenameTable(ADMIN, bobTable, new SchemaTableName("aliceschema", "newbobtable"));
        accessControl.checkCanRenameTable(ALICE, aliceTable, new SchemaTableName("aliceschema", "newalicetable"));
        accessControl.checkCanRenameView(ADMIN, new SchemaTableName("bobschema", "bobview"), new SchemaTableName("aliceschema", "newbobview"));
        accessControl.checkCanRenameView(ALICE, new SchemaTableName("aliceschema", "aliceview"), new SchemaTableName("aliceschema", "newaliceview"));
        accessControl.checkCanRenameMaterializedView(ADMIN, new SchemaTableName("bobschema", "bobmaterializedview"), new SchemaTableName("aliceschema", "newbobaterializedview"));
        accessControl.checkCanRenameMaterializedView(ALICE, new SchemaTableName("aliceschema", "alicevaterializediew"), new SchemaTableName("aliceschema", "newaliceaterializedview"));
        accessControl.checkCanSetMaterializedViewProperties(ADMIN, new SchemaTableName("bobschema", "bobmaterializedview"), ImmutableMap.of());
        accessControl.checkCanSetMaterializedViewProperties(ALICE, new SchemaTableName("aliceschema", "alicevaterializediew"), ImmutableMap.of());
        accessControl.checkCanSetViewComment(ALICE, new SchemaTableName("aliceschema", "aliceview"));

        accessControl.checkCanAlterColumn(ADMIN, bobTable);
        accessControl.checkCanAlterColumn(ALICE, aliceTable);

        accessControl.checkCanSetTableProperties(ADMIN, bobTable, ImmutableMap.of());
        accessControl.checkCanSetTableProperties(ALICE, aliceTable, ImmutableMap.of());

        assertDenied(() -> accessControl.checkCanInsertIntoTable(ALICE, bobTable));
        assertDenied(() -> accessControl.checkCanDropTable(BOB, bobTable));
        assertDenied(() -> accessControl.checkCanRenameTable(BOB, bobTable, new SchemaTableName("bobschema", "newbobtable")));
        assertDenied(() -> accessControl.checkCanRenameTable(ALICE, aliceTable, new SchemaTableName("bobschema", "newalicetable")));
        assertDenied(() -> accessControl.checkCanSetViewComment(ALICE, new SchemaTableName("bobschema", "newalicetable")));
        assertDenied(() -> accessControl.checkCanAlterColumn(BOB, bobTable));
        assertDenied(() -> accessControl.checkCanSetTableProperties(BOB, bobTable, ImmutableMap.of()));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(BOB, testTable));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(ADMIN, new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(JOE, new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanCreateViewWithSelectFromColumns(JOE, bobTable, ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanRenameView(BOB, new SchemaTableName("bobschema", "bobview"), new SchemaTableName("bobschema", "newbobview")));
        assertDenied(() -> accessControl.checkCanRenameView(ALICE, aliceTable, new SchemaTableName("bobschema", "newalicetable")));
        assertDenied(() -> accessControl.checkCanRenameMaterializedView(BOB, new SchemaTableName("bobschema", "bobmaterializedview"), new SchemaTableName("bobschema", "newbobaterializedview")));
        assertDenied(() -> accessControl.checkCanRenameMaterializedView(ALICE, aliceTable, new SchemaTableName("bobschema", "newaliceaterializedview")));
        assertDenied(() -> accessControl.checkCanSetMaterializedViewProperties(ALICE, new SchemaTableName("bobschema", "bobmaterializedview"), ImmutableMap.of()));
        assertDenied(() -> accessControl.checkCanSetMaterializedViewProperties(BOB, new SchemaTableName("bobschema", "bobmaterializedview"), ImmutableMap.of()));

        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ADMIN, testTable, new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ADMIN, testTable, new TrinoPrincipal(USER, "some_user")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, aliceTable, new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, aliceTable, new TrinoPrincipal(USER, "some_user")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, bobTable, new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, bobTable, new TrinoPrincipal(USER, "some_user")));

        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ADMIN, testTable, new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ADMIN, testTable, new TrinoPrincipal(USER, "some_user")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, aliceTable, new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, aliceTable, new TrinoPrincipal(USER, "some_user")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, bobTable, new TrinoPrincipal(ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, bobTable, new TrinoPrincipal(USER, "some_user")));
    }

    @Test
    public void testTableRulesForMixedGroupUsers()
            throws Exception
    {
        SchemaTableName myTable = new SchemaTableName("my_schema", "my_table");

        ConnectorAccessControl accessControl = createAccessControl("table-mixed-groups.json");

        ConnectorSecurityContext userGroup1Group2 = user("user_1_2", ImmutableSet.of("group1", "group2"));
        ConnectorSecurityContext userGroup2 = user("user_2", ImmutableSet.of("group2"));

        accessControl.checkCanCreateTable(userGroup1Group2, myTable, Map.of());
        accessControl.checkCanInsertIntoTable(userGroup1Group2, myTable);
        accessControl.checkCanDeleteFromTable(userGroup1Group2, myTable);
        accessControl.checkCanDropTable(userGroup1Group2, myTable);
        accessControl.checkCanSelectFromColumns(userGroup1Group2, myTable, ImmutableSet.of());
        assertThat(accessControl.getColumnMask(userGroup1Group2, myTable, "col_a", VARCHAR)).isEqualTo(Optional.empty());
        assertThat(accessControl.getRowFilters(userGroup1Group2, myTable)).isEqualTo(ImmutableList.of());

        assertDenied(() -> accessControl.checkCanCreateTable(userGroup2, myTable, Map.of()));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(userGroup2, myTable));
        assertDenied(() -> accessControl.checkCanDeleteFromTable(userGroup2, myTable));
        assertDenied(() -> accessControl.checkCanDropTable(userGroup2, myTable));
        accessControl.checkCanSelectFromColumns(userGroup2, myTable, ImmutableSet.of());
        assertViewExpressionEquals(
                accessControl.getColumnMask(userGroup2, myTable, "col_a", VARCHAR).orElseThrow(),
                ViewExpression.builder()
                        .catalog("test_catalog")
                        .schema("my_schema")
                        .expression("'mask_a'")
                        .build());
        assertThat(accessControl.getRowFilters(userGroup2, myTable)).isEqualTo(ImmutableList.of());

        ConnectorSecurityContext userGroup1Group3 = user("user_1_3", ImmutableSet.of("group1", "group3"));
        ConnectorSecurityContext userGroup3 = user("user_3", ImmutableSet.of("group3"));

        accessControl.checkCanCreateTable(userGroup1Group3, myTable, Map.of());
        accessControl.checkCanInsertIntoTable(userGroup1Group3, myTable);
        accessControl.checkCanDeleteFromTable(userGroup1Group3, myTable);
        accessControl.checkCanDropTable(userGroup1Group3, myTable);
        accessControl.checkCanSelectFromColumns(userGroup1Group3, myTable, ImmutableSet.of());
        assertThat(accessControl.getColumnMask(userGroup1Group3, myTable, "col_a", VARCHAR)).isEqualTo(Optional.empty());

        assertDenied(() -> accessControl.checkCanCreateTable(userGroup3, myTable, Map.of()));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(userGroup3, myTable));
        assertDenied(() -> accessControl.checkCanDeleteFromTable(userGroup3, myTable));
        assertDenied(() -> accessControl.checkCanDropTable(userGroup3, myTable));
        accessControl.checkCanSelectFromColumns(userGroup3, myTable, ImmutableSet.of());
        assertViewExpressionEquals(
                accessControl.getColumnMask(userGroup3, myTable, "col_a", VARCHAR).orElseThrow(),
                ViewExpression.builder()
                        .catalog("test_catalog")
                        .schema("my_schema")
                        .expression("'mask_a'")
                        .build());

        List<ViewExpression> rowFilters = accessControl.getRowFilters(userGroup3, myTable);
        assertThat(rowFilters).hasSize(1);
        assertViewExpressionEquals(
                rowFilters.get(0),
                ViewExpression.builder()
                        .catalog("test_catalog")
                        .schema("my_schema")
                        .expression("country='US'")
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
    public void testTableFilter()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("table-filter.json");
        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("restricted", "any"))
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .add(new SchemaTableName("bobschema", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertThat(accessControl.filterTables(ALICE, tables)).isEqualTo(ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .build());
        assertThat(accessControl.filterTables(BOB, tables)).isEqualTo(ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .build());
        assertThat(accessControl.filterTables(ADMIN, tables)).isEqualTo(ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .add(new SchemaTableName("bobschema", "any"))
                .add(new SchemaTableName("any", "any"))
                .build());
    }

    @Test
    public void testNoTableRules()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("no-access.json");
        SchemaTableName bobTable = new SchemaTableName("bobschema", "bobtable");
        assertDenied(() -> accessControl.checkCanShowColumns(BOB, bobTable));
        assertDenied(() -> accessControl.checkCanShowTables(BOB, "bobschema"));
        assertThat(accessControl.filterColumns(BOB, Map.of(bobTable, ImmutableSet.of("a"))))
                .isEqualTo(Map.of(bobTable, ImmutableSet.of()));

        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("restricted", "any"))
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertThat(accessControl.filterTables(ALICE, tables)).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterTables(BOB, tables)).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testNoFunctionRules()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("no-access.json");

        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("schema", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("schema", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("ptf_schema", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("schema", "some_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new SchemaRoutineName("schema", "some_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new SchemaRoutineName("schema", "some_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new SchemaRoutineName("ptf_schema", "some_function"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(ALICE, new SchemaRoutineName("schema", "some_function"))).isFalse();

        Set<SchemaFunctionName> functions = ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("restricted", "any"))
                .add(new SchemaFunctionName("secret", "any"))
                .add(new SchemaFunctionName("any", "any"))
                .build();
        assertThat(accessControl.filterFunctions(ALICE, functions)).isEqualTo(ImmutableSet.of());
        assertThat(accessControl.filterFunctions(BOB, functions)).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testSessionPropertyRules()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("session_property.json");
        accessControl.checkCanSetCatalogSessionProperty(ADMIN, "dangerous");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "safe");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "unsafe");
        accessControl.checkCanSetCatalogSessionProperty(ALICE, "staff");
        accessControl.checkCanSetCatalogSessionProperty(BOB, "safe");
        accessControl.checkCanSetCatalogSessionProperty(BOB, "staff");
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(BOB, "unsafe"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(ALICE, "dangerous"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(CHARLIE, "safe"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(CHARLIE, "staff"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(JOE, "staff"));
    }

    @Test
    public void testFilterSchemas()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("visibility.json");
        assertFilterSchemas(accessControl);
    }

    private static void assertFilterSchemas(ConnectorAccessControl accessControl)
    {
        ImmutableSet<String> allSchemas = ImmutableSet.of("specific-schema", "alice-schema", "bob-schema", "unknown", "ptf_schema", "procedure-schema");
        assertThat(accessControl.filterSchemas(ADMIN, allSchemas)).isEqualTo(allSchemas);
        assertThat(accessControl.filterSchemas(ALICE, allSchemas)).isEqualTo(ImmutableSet.of("specific-schema", "alice-schema", "ptf_schema"));
        assertThat(accessControl.filterSchemas(BOB, allSchemas)).isEqualTo(ImmutableSet.of("specific-schema", "bob-schema", "procedure-schema"));
        assertThat(accessControl.filterSchemas(CHARLIE, allSchemas)).isEqualTo(ImmutableSet.of("specific-schema"));
    }

    @Test
    public void testSchemaRulesForCheckCanShowTables()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("visibility.json");
        accessControl.checkCanShowTables(ADMIN, "specific-schema");
        accessControl.checkCanShowTables(ADMIN, "bob-schema");
        accessControl.checkCanShowTables(ADMIN, "alice-schema");
        accessControl.checkCanShowTables(ADMIN, "secret");
        accessControl.checkCanShowTables(ADMIN, "any");
        accessControl.checkCanShowTables(ALICE, "specific-schema");
        accessControl.checkCanShowTables(ALICE, "alice-schema");
        assertDenied(() -> accessControl.checkCanShowTables(ALICE, "bob-schema"));
        assertDenied(() -> accessControl.checkCanShowTables(ALICE, "secret"));
        assertDenied(() -> accessControl.checkCanShowTables(ALICE, "any"));
        accessControl.checkCanShowTables(BOB, "specific-schema");
        accessControl.checkCanShowTables(BOB, "bob-schema");
        assertDenied(() -> accessControl.checkCanShowTables(BOB, "alice-schema"));
        assertDenied(() -> accessControl.checkCanShowTables(BOB, "secret"));
        accessControl.checkCanShowTables(BOB, "any");
        accessControl.checkCanShowTables(CHARLIE, "specific-schema");
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "bob-schema"));
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "alice-schema"));
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "secret"));
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "any"));
    }

    @Test
    public void testSchemaRulesForCheckCanShowFunctions()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("visibility.json");
        accessControl.checkCanShowFunctions(ADMIN, "specific-schema");
        accessControl.checkCanShowFunctions(ADMIN, "bob-schema");
        accessControl.checkCanShowFunctions(ADMIN, "alice-schema");
        accessControl.checkCanShowFunctions(ADMIN, "secret");
        accessControl.checkCanShowFunctions(ADMIN, "any");
        accessControl.checkCanShowFunctions(ALICE, "specific-schema");
        accessControl.checkCanShowFunctions(ALICE, "alice-schema");
        assertDenied(() -> accessControl.checkCanShowFunctions(ALICE, "bob-schema"));
        assertDenied(() -> accessControl.checkCanShowFunctions(ALICE, "secret"));
        assertDenied(() -> accessControl.checkCanShowFunctions(ALICE, "any"));
        accessControl.checkCanShowFunctions(BOB, "specific-schema");
        accessControl.checkCanShowFunctions(BOB, "bob-schema");
        assertDenied(() -> accessControl.checkCanShowFunctions(BOB, "alice-schema"));
        assertDenied(() -> accessControl.checkCanShowFunctions(BOB, "secret"));
        accessControl.checkCanShowFunctions(BOB, "any");
        accessControl.checkCanShowFunctions(CHARLIE, "specific-schema");
        assertDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, "bob-schema"));
        assertDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, "alice-schema"));
        assertDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, "secret"));
        assertDenied(() -> accessControl.checkCanShowFunctions(CHARLIE, "any"));
    }

    @Test
    public void testFunctionRulesForCheckCanExecute()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("visibility.json");
        assertThat(accessControl.canExecuteFunction(ADMIN, new SchemaRoutineName("ptf_schema", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ADMIN, new SchemaRoutineName("any", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ADMIN, new SchemaRoutineName("any", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ADMIN, new SchemaRoutineName("any", "some_function"))).isFalse();

        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("ptf_schema", "some_function"))).isTrue();
        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("any", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("any", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(ALICE, new SchemaRoutineName("any", "some_function"))).isFalse();

        assertThat(accessControl.canExecuteFunction(BOB, new SchemaRoutineName("ptf_schema", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(BOB, new SchemaRoutineName("any", "some_function"))).isTrue();
        assertThat(accessControl.canExecuteFunction(BOB, new SchemaRoutineName("any", "some_function"))).isTrue();
        assertThat(accessControl.canExecuteFunction(BOB, new SchemaRoutineName("any", "some_function"))).isTrue();

        assertThat(accessControl.canExecuteFunction(CHARLIE, new SchemaRoutineName("ptf_schema", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(CHARLIE, new SchemaRoutineName("any", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(CHARLIE, new SchemaRoutineName("any", "some_function"))).isFalse();
        assertThat(accessControl.canExecuteFunction(CHARLIE, new SchemaRoutineName("any", "some_function"))).isFalse();
    }

    @Test
    public void testProcedureRulesForCheckCanExecute()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("visibility.json");

        accessControl.checkCanExecuteProcedure(BOB, new SchemaRoutineName("procedure-schema", "some_procedure"));
        assertDenied(() -> accessControl.checkCanExecuteProcedure(BOB, new SchemaRoutineName("some-schema", "some_procedure")));
        assertDenied(() -> accessControl.checkCanExecuteProcedure(BOB, new SchemaRoutineName("procedure-schema", "another_procedure")));

        assertDenied(() -> accessControl.checkCanExecuteProcedure(CHARLIE, new SchemaRoutineName("procedure-schema", "some_procedure")));

        assertDenied(() -> accessControl.checkCanExecuteProcedure(ALICE, new SchemaRoutineName("procedure-schema", "some_procedure")));
    }

    @Test
    public void testInvalidRules()
    {
        assertThatThrownBy(() -> createAccessControl("invalid.json"))
                .hasMessageContaining("Failed to convert JSON tree node");
    }

    @Test
    public void testFilterSchemasWithJsonPointer()
            throws Exception
    {
        Path configFile = getResourcePath("visibility-with-json-pointer.json");
        ConnectorAccessControl accessControl = createAccessControl(configFile,
                ImmutableMap.of("security.json-pointer", "/data"));
        assertFilterSchemas(accessControl);
    }

    @Test
    public void testSchemaAuthorization()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("authorization-no-roles.json");

        String schema = "test";
        String ownedByUser = "owned_by_user";
        String ownedByGroup = "owned_by_group";

        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(user("user", "group"), schema, new TrinoPrincipal(ROLE, "new_role")));

        // access to schema granted to user
        accessControl.checkCanSetSchemaAuthorization(user("owner_authorized", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user"));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(user("owner_DENY_authorized", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(user("owner", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(user("owner_authorized", "group"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")));

        // access to schema granted to group
        accessControl.checkCanSetSchemaAuthorization(user("authorized", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user"));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(user("DENY_authorized", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(user("user", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(user("authorized", "owner"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")));
    }

    @Test
    public void testTableAuthorization()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("authorization-no-roles.json");

        SchemaTableName table = new SchemaTableName("test", "table");
        SchemaTableName ownedByUser = new SchemaTableName("test", "owned_by_user");
        SchemaTableName ownedByGroup = new SchemaTableName("test", "owned_by_group");

        assertDenied(() -> accessControl.checkCanSetTableAuthorization(user("user", "group"), table, new TrinoPrincipal(ROLE, "new_role")));

        // access to table granted to user
        accessControl.checkCanSetTableAuthorization(user("owner_authorized", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user"));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(user("owner", "group"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(user("owner_authorized", "group"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(user("owner_DENY_authorized", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user")));

        // access to table granted to group
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(user("user", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user")));
        accessControl.checkCanSetTableAuthorization(user("authorized", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user"));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(user("authorized", "owner"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(user("DENY_authorized", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user")));
    }

    @Test
    public void testViewAuthorization()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("authorization-no-roles.json");

        SchemaTableName table = new SchemaTableName("test", "table");
        SchemaTableName ownedByUser = new SchemaTableName("test", "owned_by_user");
        SchemaTableName ownedByGroup = new SchemaTableName("test", "owned_by_group");

        assertDenied(() -> accessControl.checkCanSetViewAuthorization(user("user", "group"), table, new TrinoPrincipal(ROLE, "new_role")));

        // access to schema granted to user
        accessControl.checkCanSetViewAuthorization(user("owner_authorized", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user"));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(user("owner_DENY_authorized", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(user("owner", "group"), ownedByUser, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(user("owner_authorized", "group"), ownedByUser, new TrinoPrincipal(ROLE, "new_role")));

        // access to schema granted to group
        accessControl.checkCanSetViewAuthorization(user("authorized", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user"));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(user("DENY_authorized", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(user("user", "owner"), ownedByGroup, new TrinoPrincipal(USER, "new_user")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(user("authorized", "owner"), ownedByGroup, new TrinoPrincipal(ROLE, "new_role")));
    }

    @Test
    public void testFunctionFilter()
            throws Exception
    {
        ConnectorAccessControl accessControl = createAccessControl("function-filter.json");
        Set<SchemaFunctionName> functions = ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("restricted", "any"))
                .add(new SchemaFunctionName("secret", "any"))
                .add(new SchemaFunctionName("aliceschema", "any"))
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .add(new SchemaFunctionName("bobschema", "bob_any"))
                .add(new SchemaFunctionName("bobschema", "any"))
                .add(new SchemaFunctionName("any", "any"))
                .build();
        assertThat(accessControl.filterFunctions(ALICE, functions)).isEqualTo(ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("aliceschema", "any"))
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .build());
        assertThat(accessControl.filterFunctions(BOB, functions)).isEqualTo(ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .add(new SchemaFunctionName("bobschema", "bob_any"))
                .build());
        assertThat(accessControl.filterFunctions(ADMIN, functions)).isEqualTo(ImmutableSet.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("secret", "any"))
                .add(new SchemaFunctionName("aliceschema", "any"))
                .add(new SchemaFunctionName("aliceschema", "bobfunction"))
                .add(new SchemaFunctionName("bobschema", "bob_any"))
                .add(new SchemaFunctionName("bobschema", "any"))
                .add(new SchemaFunctionName("any", "any"))
                .build());
    }

    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, FileBasedAccessControl.class);
    }

    @Test
    public void testRefreshing(@TempDir Path tempDir)
            throws Exception
    {
        Path configFile = tempDir.resolve("visibility.json");
        Files.copy(getResourcePath("visibility.json"), configFile, REPLACE_EXISTING);

        ConnectorAccessControl accessControl = createAccessControl(configFile, ImmutableMap.of(
                "security.refresh-period", "1ms"));
        accessControl.checkCanShowTables(ALICE, "alice-schema");
        accessControl.checkCanShowTables(ALICE, "alice-schema");
        accessControl.checkCanShowTables(ALICE, "alice-schema");

        Files.copy(getResourcePath("no-access.json"), configFile, REPLACE_EXISTING);
        sleep(2);

        assertThatThrownBy(() -> accessControl.checkCanShowTables(ALICE, "alice-schema"))
                .hasMessageContaining("Access Denied");

        Files.copy(getResourcePath("visibility.json"), configFile, REPLACE_EXISTING);
        sleep(2);

        accessControl.checkCanShowTables(ALICE, "alice-schema");
    }

    protected ConnectorAccessControl createAccessControl(Map<String, String> configProperties)
    {
        Bootstrap bootstrap = new Bootstrap(new CatalogNameModule("test_catalog"), new FileBasedAccessControlModule());

        Injector injector = bootstrap
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(configProperties)
                .initialize();

        return injector.getInstance(ConnectorAccessControl.class);
    }

    private ConnectorAccessControl createAccessControl(String rulesName)
            throws URISyntaxException
    {
        Path configFile = getResourcePath(rulesName);
        return createAccessControl(configFile, ImmutableMap.of());
    }

    private Path getResourcePath(String resourceName)
            throws URISyntaxException
    {
        return Paths.get(requireNonNull(this.getClass().getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).toURI());
    }

    private static ConnectorSecurityContext user(String user, String group)
    {
        return user(user, ImmutableSet.of(group));
    }

    private static ConnectorSecurityContext user(String name, Set<String> groups)
    {
        return new ConnectorSecurityContext(
                new ConnectorTransactionHandle() {},
                ConnectorIdentity.forUser(name).withGroups(groups).build(),
                new QueryId("query_id"));
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(AccessDeniedException.class)
                // TODO test expected message precisely, as in TestFileBasedSystemAccessControl
                .hasMessageStartingWith("Access Denied");
    }

    interface ThrowingRunnable
    {
        void run()
                throws Exception;
    }
}
