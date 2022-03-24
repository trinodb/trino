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
import io.trino.plugin.base.CatalogName;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestFileBasedAccessControl
{
    private static final ConnectorSecurityContext ADMIN = user("admin", ImmutableSet.of("admin", "staff"));
    private static final ConnectorSecurityContext ALICE = user("alice", ImmutableSet.of("staff"));
    private static final ConnectorSecurityContext BOB = user("bob", ImmutableSet.of("staff"));
    private static final ConnectorSecurityContext CHARLIE = user("charlie", ImmutableSet.of("guests"));
    private static final ConnectorSecurityContext JOE = user("joe", ImmutableSet.of());
    private static final ConnectorSecurityContext UNKNOWN = user("unknown", ImmutableSet.of());

    @Test
    public void testEmptyFile()
    {
        ConnectorAccessControl accessControl = createAccessControl("empty.json");

        accessControl.checkCanCreateSchema(UNKNOWN, "unknown");
        accessControl.checkCanDropSchema(UNKNOWN, "unknown");
        accessControl.checkCanRenameSchema(UNKNOWN, "unknown", "new_unknown");
        accessControl.checkCanSetSchemaAuthorization(UNKNOWN, "unknown", new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
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

        accessControl.checkCanSetCatalogSessionProperty(UNKNOWN, "anything");

        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertEquals(accessControl.filterTables(UNKNOWN, tables), tables);

        // permissions management APIs are hard coded to deny
        TrinoPrincipal someUser = new TrinoPrincipal(PrincipalType.USER, "some_user");
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
        accessControl.checkCanShowRoleAuthorizationDescriptors(UNKNOWN);
        accessControl.checkCanShowRoles(UNKNOWN);
        accessControl.checkCanShowCurrentRoles(UNKNOWN);
        accessControl.checkCanShowRoleGrants(UNKNOWN);
    }

    @Test
    public void testSchemaRules()
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");

        accessControl.checkCanCreateSchema(ADMIN, "bob");
        accessControl.checkCanCreateSchema(ADMIN, "staff");
        accessControl.checkCanCreateSchema(ADMIN, "authenticated");
        accessControl.checkCanCreateSchema(ADMIN, "test");

        accessControl.checkCanCreateSchema(BOB, "bob");
        accessControl.checkCanCreateSchema(BOB, "staff");
        accessControl.checkCanCreateSchema(BOB, "authenticated");
        assertDenied(() -> accessControl.checkCanCreateSchema(BOB, "test"));

        assertDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, "bob"));
        assertDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, "staff"));
        accessControl.checkCanCreateSchema(CHARLIE, "authenticated");
        assertDenied(() -> accessControl.checkCanCreateSchema(CHARLIE, "test"));

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

        accessControl.checkCanSetSchemaAuthorization(ADMIN, "test", new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetSchemaAuthorization(ADMIN, "test", new TrinoPrincipal(PrincipalType.USER, "some_user"));
        accessControl.checkCanSetSchemaAuthorization(BOB, "bob", new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetSchemaAuthorization(BOB, "bob", new TrinoPrincipal(PrincipalType.USER, "some_user"));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, "test", new TrinoPrincipal(PrincipalType.ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, "test", new TrinoPrincipal(PrincipalType.USER, "some_user")));

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

    @Test(dataProvider = "privilegeGrantOption")
    public void testGrantSchemaPrivilege(Privilege privilege, boolean grantOption)
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.USER, "alice");

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
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.USER, "alice");

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

    @Test(dataProvider = "privilegeGrantOption")
    public void testRevokeSchemaPrivilege(Privilege privilege, boolean grantOption)
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.USER, "alice");

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

    @DataProvider(name = "privilegeGrantOption")
    public Object[][] privilegeGrantOption()
    {
        return EnumSet.allOf(Privilege.class)
                .stream()
                .flatMap(privilege -> Stream.of(true, false).map(grantOption -> new Object[] {privilege, grantOption}))
                .toArray(Object[][]::new);
    }

    @Test
    public void testTableRules()
    {
        SchemaTableName testTable = new SchemaTableName("test", "test");
        SchemaTableName aliceTable = new SchemaTableName("aliceschema", "alicetable");
        SchemaTableName bobTable = new SchemaTableName("bobschema", "bobtable");

        ConnectorAccessControl accessControl = createAccessControl("table.json");
        accessControl.checkCanSelectFromColumns(ALICE, testTable, ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(ALICE, bobTable, ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(ALICE, bobTable, ImmutableSet.of("bobcolumn"));

        accessControl.checkCanShowColumns(ALICE, bobTable);
        assertEquals(
                accessControl.filterColumns(ALICE, bobTable, ImmutableSet.of("a")),
                ImmutableSet.of("a"));
        accessControl.checkCanSelectFromColumns(BOB, bobTable, ImmutableSet.of());
        accessControl.checkCanShowColumns(BOB, bobTable);
        assertEquals(
                accessControl.filterColumns(BOB, bobTable, ImmutableSet.of("a")),
                ImmutableSet.of("a"));

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

        accessControl.checkCanSetTableProperties(ADMIN, bobTable, ImmutableMap.of());
        accessControl.checkCanSetTableProperties(ALICE, aliceTable, ImmutableMap.of());

        assertDenied(() -> accessControl.checkCanInsertIntoTable(ALICE, bobTable));
        assertDenied(() -> accessControl.checkCanDropTable(BOB, bobTable));
        assertDenied(() -> accessControl.checkCanRenameTable(BOB, bobTable, new SchemaTableName("bobschema", "newbobtable")));
        assertDenied(() -> accessControl.checkCanRenameTable(ALICE, aliceTable, new SchemaTableName("bobschema", "newalicetable")));
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

        accessControl.checkCanSetTableAuthorization(ADMIN, testTable, new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetTableAuthorization(ADMIN, testTable, new TrinoPrincipal(PrincipalType.USER, "some_user"));
        accessControl.checkCanSetTableAuthorization(ALICE, aliceTable, new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetTableAuthorization(ALICE, aliceTable, new TrinoPrincipal(PrincipalType.USER, "some_user"));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, bobTable, new TrinoPrincipal(PrincipalType.ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetTableAuthorization(ALICE, bobTable, new TrinoPrincipal(PrincipalType.USER, "some_user")));

        accessControl.checkCanSetViewAuthorization(ADMIN, testTable, new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetViewAuthorization(ADMIN, testTable, new TrinoPrincipal(PrincipalType.USER, "some_user"));
        accessControl.checkCanSetViewAuthorization(ALICE, aliceTable, new TrinoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetViewAuthorization(ALICE, aliceTable, new TrinoPrincipal(PrincipalType.USER, "some_user"));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, bobTable, new TrinoPrincipal(PrincipalType.ROLE, "some_role")));
        assertDenied(() -> accessControl.checkCanSetViewAuthorization(ALICE, bobTable, new TrinoPrincipal(PrincipalType.USER, "some_user")));
    }

    @Test
    public void testTableFilter()
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
        assertEquals(accessControl.filterTables(ALICE, tables), ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "any"))
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .build());
        assertEquals(accessControl.filterTables(BOB, tables), ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("aliceschema", "bobtable"))
                .add(new SchemaTableName("bobschema", "bob_any"))
                .build());
        assertEquals(accessControl.filterTables(ADMIN, tables), ImmutableSet.<SchemaTableName>builder()
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
    {
        ConnectorAccessControl accessControl = createAccessControl("no-access.json");
        assertDenied(() -> accessControl.checkCanShowColumns(BOB, new SchemaTableName("bobschema", "bobtable")));
        assertDenied(() -> accessControl.checkCanShowTables(BOB, "bobschema"));
        assertEquals(
                accessControl.filterColumns(BOB, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of("a")),
                ImmutableSet.of());

        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("restricted", "any"))
                .add(new SchemaTableName("secret", "any"))
                .add(new SchemaTableName("any", "any"))
                .build();
        assertEquals(accessControl.filterTables(ALICE, tables), ImmutableSet.of());
        assertEquals(accessControl.filterTables(BOB, tables), ImmutableSet.of());
    }

    @Test
    public void testSessionPropertyRules()
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
    public void testInvalidRules()
    {
        assertThatThrownBy(() -> createAccessControl("invalid.json"))
                .hasMessageContaining("Invalid JSON");
    }

    @Test
    public void testFilterSchemas()
    {
        ConnectorAccessControl accessControl = createAccessControl("visibility.json");

        ImmutableSet<String> allSchemas = ImmutableSet.of("specific-schema", "alice-schema", "bob-schema", "unknown");
        assertEquals(accessControl.filterSchemas(ADMIN, allSchemas), allSchemas);
        assertEquals(accessControl.filterSchemas(ALICE, allSchemas), ImmutableSet.of("specific-schema", "alice-schema"));
        assertEquals(accessControl.filterSchemas(BOB, allSchemas), ImmutableSet.of("specific-schema", "bob-schema"));
        assertEquals(accessControl.filterSchemas(CHARLIE, allSchemas), ImmutableSet.of("specific-schema"));
    }

    @Test
    public void testSchemaRulesForCheckCanShowTables()
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
        assertDenied(() -> accessControl.checkCanShowTables(BOB, "any"));
        accessControl.checkCanShowTables(CHARLIE, "specific-schema");
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "bob-schema"));
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "alice-schema"));
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "secret"));
        assertDenied(() -> accessControl.checkCanShowTables(CHARLIE, "any"));
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, FileBasedAccessControl.class);
    }

    private static ConnectorSecurityContext user(String name, Set<String> groups)
    {
        return new ConnectorSecurityContext(
                new ConnectorTransactionHandle() {},
                ConnectorIdentity.forUser(name).withGroups(groups).build(),
                new QueryId("query_id"));
    }

    private static ConnectorAccessControl createAccessControl(String fileName)
    {
        File configFile = new File(getResource(fileName).getPath());
        return new FileBasedAccessControl(new CatalogName("test_catalog"), configFile);
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(AccessDeniedException.class)
                // TODO test expected message precisely, as in TestFileBasedSystemAccessControl
                .hasMessageStartingWith("Access Denied");
    }
}
