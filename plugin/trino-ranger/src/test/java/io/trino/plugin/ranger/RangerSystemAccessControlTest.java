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
package io.trino.plugin.ranger;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.VarcharType;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.SELECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RangerSystemAccessControlTest
{
    static RangerSystemAccessControl accessControlManager;

    private static final Identity alice = new Identity.Builder("alice").withPrincipal(new BasicPrincipal("alice")).build();
    private static final Identity admin = new Identity.Builder("admin").withPrincipal(new BasicPrincipal("admin")).build();
    private static final Identity kerberosInvalidAlice = Identity.from(alice).withPrincipal(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")).build();
    private static final Identity bob = Identity.ofUser("bob");

    private static final Set<String> allCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "alice-catalog");
    private static final Set<Identity> queryOwners = ImmutableSet.of(Identity.ofUser("bob"), Identity.ofUser("alice"), Identity.ofUser("frank"));
    private static final String aliceCatalog = "alice-catalog";
    private static final CatalogSchemaName aliceSchema = new CatalogSchemaName("alice-catalog", "schema");
    private static final CatalogSchemaTableName aliceTable = new CatalogSchemaTableName("alice-catalog", "schema", "table");
    private static final CatalogSchemaTableName aliceView = new CatalogSchemaTableName("alice-catalog", "schema", "view");
    private static final CatalogSchemaRoutineName aliceProcedure = new CatalogSchemaRoutineName("alice-catalog", "schema", "procedure");
    private static final CatalogSchemaRoutineName aliceFunction = new CatalogSchemaRoutineName("alice-catalog", "schema", "function");

    @BeforeClass
    public static void setUpBeforeClass()
            throws Exception
    {
        accessControlManager = new RangerSystemAccessControl(new RangerConfig());
    }

    @Test
    @SuppressWarnings("PMD")
    public void testCanSetUserOperations()
    {
        accessControlManager.checkCanSetUser(admin.getPrincipal(), bob.getUser());
        accessControlManager.checkCanImpersonateUser(admin, bob.getUser());
        accessControlManager.checkCanViewQueryOwnedBy(admin, bob);
        accessControlManager.checkCanKillQueryOwnedBy(admin, bob);
        assertEquals(Collections.emptyList(), accessControlManager.filterViewQueryOwnedBy(alice, queryOwners));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetUser(alice.getPrincipal(), bob.getUser()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanImpersonateUser(alice, bob.getUser()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanViewQueryOwnedBy(alice, bob));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanKillQueryOwnedBy(alice, bob));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanImpersonateUser(kerberosInvalidAlice, bob.getUser()));
    }

    @Test
    public void testSystemInformationOperations()
    {
        accessControlManager.checkCanReadSystemInformation(admin);
        accessControlManager.checkCanWriteSystemInformation(admin);

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanReadSystemInformation(alice));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanWriteSystemInformation(alice));
    }

    @Test
    public void testSystemSessionPropertyOperations()
    {
        accessControlManager.checkCanSetSystemSessionProperty(admin, "test-property");
        accessControlManager.checkCanSetSystemSessionProperty(admin, new QueryId("q1"), "test-property");

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetSystemSessionProperty(alice, "test-property"));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetSystemSessionProperty(alice, new QueryId("q1"), "test-property"));
    }

    @Test
    public void testQueryOperations()
    {
        accessControlManager.checkCanExecuteQuery(admin);
        accessControlManager.checkCanExecuteQuery(admin, new QueryId("1"));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanExecuteQuery(alice));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanExecuteQuery(alice, new QueryId("1")));
    }

    @Test
    public void testCatalogOperations()
    {
        accessControlManager.canAccessCatalog(context(alice), aliceCatalog);
        accessControlManager.checkCanCreateCatalog(context(alice), aliceCatalog);
        accessControlManager.checkCanDropCatalog(context(alice), aliceCatalog);
        accessControlManager.checkCanSetCatalogSessionProperty(context(alice), aliceCatalog, "property");
        assertEquals(allCatalogs, accessControlManager.filterCatalogs(context(alice), allCatalogs));
        assertEquals(ImmutableSet.of("open-to-all", "all-allowed"), accessControlManager.filterCatalogs(context(bob), allCatalogs));

        assertFalse(accessControlManager.canAccessCatalog(context(bob), aliceCatalog));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateCatalog(context(bob), aliceCatalog));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropCatalog(context(bob), aliceCatalog));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetCatalogSessionProperty(context(bob), aliceCatalog, "property"));
    }

    @Test
    @SuppressWarnings("PMD")
    public void testSchemaOperations()
    {
        accessControlManager.checkCanCreateSchema(context(alice), aliceSchema, null);
        accessControlManager.checkCanDropSchema(context(alice), aliceSchema);
        accessControlManager.checkCanRenameSchema(context(alice), aliceSchema, "new-schema");
        accessControlManager.checkCanShowSchemas(context(alice), aliceCatalog);
        accessControlManager.checkCanSetSchemaAuthorization(context(alice), aliceSchema, new TrinoPrincipal(USER, "principal"));
        accessControlManager.checkCanShowCreateSchema(context(alice), aliceSchema);
        accessControlManager.checkCanGrantSchemaPrivilege(context(alice), SELECT, aliceSchema, new TrinoPrincipal(USER, "principal"), true);
        accessControlManager.checkCanDenySchemaPrivilege(context(alice), SELECT, aliceSchema, new TrinoPrincipal(USER, "principal"));
        accessControlManager.checkCanRevokeSchemaPrivilege(context(alice), SELECT, aliceSchema, new TrinoPrincipal(USER, "principal"), true);

        Set<String> aliceSchemas = ImmutableSet.of("schema");
        assertEquals(aliceSchemas, accessControlManager.filterSchemas(context(alice), aliceCatalog, aliceSchemas));
        assertEquals(ImmutableSet.of(), accessControlManager.filterSchemas(context(bob), "alice-catalog", aliceSchemas));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateSchema(context(bob), aliceSchema, null));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropSchema(context(bob), aliceSchema));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameSchema(context(bob), aliceSchema, "new-schema"));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanShowSchemas(context(bob), aliceSchema.getCatalogName()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetSchemaAuthorization(context(bob), aliceSchema, new TrinoPrincipal(USER, "principal")));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanShowCreateSchema(context(bob), aliceSchema));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantSchemaPrivilege(context(bob), SELECT, aliceSchema, new TrinoPrincipal(USER, "principal"), true));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDenySchemaPrivilege(context(bob), SELECT, aliceSchema, new TrinoPrincipal(USER, "principal")));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeSchemaPrivilege(context(bob), SELECT, aliceSchema, new TrinoPrincipal(USER, "principal"), true));
    }

    @Test
    @SuppressWarnings("PMD")
    public void testTableOperations()
    {
        CatalogSchemaTableName newTableName = new CatalogSchemaTableName("alice-catalog", "schema", "new-table");

        accessControlManager.checkCanCreateTable(context(alice), aliceTable, Map.of());
        accessControlManager.checkCanDropTable(context(alice), aliceTable);
        accessControlManager.checkCanRenameTable(context(alice), aliceTable, newTableName);
        accessControlManager.checkCanSetTableProperties(context(alice), aliceTable, Collections.emptyMap());
        accessControlManager.checkCanSetTableComment(context(alice), aliceTable);
        accessControlManager.checkCanSetTableAuthorization(context(alice), aliceTable, new TrinoPrincipal(USER, "principal"));
        accessControlManager.checkCanShowTables(context(alice), aliceSchema);
        accessControlManager.checkCanShowCreateTable(context(alice), aliceTable);
        accessControlManager.checkCanInsertIntoTable(context(alice), aliceTable);
        accessControlManager.checkCanDeleteFromTable(context(alice), aliceTable);
        accessControlManager.checkCanTruncateTable(context(alice), aliceTable);
        accessControlManager.checkCanGrantTablePrivilege(context(alice), SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true);
        accessControlManager.checkCanDenyTablePrivilege(context(alice), SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"));
        accessControlManager.checkCanRevokeTablePrivilege(context(alice), SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true);

        Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
        assertEquals(aliceTables, accessControlManager.filterTables(context(alice), aliceCatalog, aliceTables));
        assertEquals(ImmutableSet.of(), accessControlManager.filterTables(context(bob), "alice-catalog", aliceTables));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateTable(context(bob), aliceTable, Map.of()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropTable(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameTable(context(bob), aliceTable, newTableName));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetTableProperties(context(bob), aliceTable, Collections.emptyMap()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetTableComment(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetTableAuthorization(context(bob), aliceTable, new TrinoPrincipal(USER, "principal")));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanShowTables(context(bob), aliceSchema));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanShowCreateTable(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanInsertIntoTable(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDeleteFromTable(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanTruncateTable(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantTablePrivilege(context(bob), SELECT, aliceTable, new TrinoPrincipal(USER, "grantee"), true));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDenyTablePrivilege(context(bob), SELECT, aliceTable, new TrinoPrincipal(USER, "grantee")));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeTablePrivilege(context(bob), SELECT, aliceTable, new TrinoPrincipal(USER, "revokee"), true));
    }

    @Test
    public void testColumnOperations()
    {
        accessControlManager.checkCanAddColumn(context(alice), aliceTable);
        accessControlManager.checkCanAlterColumn(context(alice), aliceTable);
        accessControlManager.checkCanDropColumn(context(alice), aliceTable);
        accessControlManager.checkCanRenameColumn(context(alice), aliceTable);
        accessControlManager.checkCanSetColumnComment(context(alice), aliceTable);
        accessControlManager.checkCanShowColumns(context(alice), aliceTable);
        accessControlManager.checkCanSelectFromColumns(context(alice), aliceTable, ImmutableSet.of());
        accessControlManager.checkCanUpdateTableColumns(context(alice), aliceTable, Collections.emptySet());

        Set<String> columns = Collections.singleton("column-1");
        Map<SchemaTableName, Set<String>> tableColumns = Collections.singletonMap(aliceTable.getSchemaTableName(), columns);

        assertEquals(columns, accessControlManager.filterColumns(context(alice), aliceTable, columns));
        assertEquals(tableColumns, accessControlManager.filterColumns(context(alice), aliceTable.getCatalogName(), tableColumns));
        assertEquals(Collections.emptySet(), accessControlManager.filterColumns(context(bob), aliceTable, columns));
        assertEquals(Collections.singletonMap(aliceTable.getSchemaTableName(), Collections.emptySet()), accessControlManager.filterColumns(context(bob), aliceTable.getCatalogName(), tableColumns));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanAddColumn(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanAlterColumn(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropColumn(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameColumn(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetColumnComment(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanShowColumns(context(bob), aliceTable));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSelectFromColumns(context(bob), aliceTable, ImmutableSet.of()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanUpdateTableColumns(context(bob), aliceTable, Collections.emptySet()));
    }

    @Test
    @SuppressWarnings("PMD")
    public void testViewOperations()
    {
        CatalogSchemaTableName newViewName = new CatalogSchemaTableName(aliceView.getCatalogName(), aliceView.getSchemaTableName().getSchemaName(), "new-view");

        accessControlManager.checkCanCreateView(context(alice), aliceView);
        accessControlManager.checkCanDropView(context(alice), aliceView);
        accessControlManager.checkCanRenameView(context(alice), aliceView, newViewName);
        accessControlManager.checkCanSetViewAuthorization(context(alice), aliceView, new TrinoPrincipal(USER, "user"));
        accessControlManager.checkCanCreateViewWithSelectFromColumns(context(alice), aliceTable, ImmutableSet.of());
        accessControlManager.checkCanSetViewAuthorization(context(alice), aliceView, new TrinoPrincipal(USER, "user"));
        accessControlManager.checkCanSetViewComment(context(alice), aliceView);

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateView(context(bob), aliceView));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropView(context(bob), aliceView));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameView(context(bob), aliceView, newViewName));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetViewAuthorization(context(bob), aliceView, new TrinoPrincipal(USER, "user")));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateViewWithSelectFromColumns(context(bob), aliceTable, ImmutableSet.of()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetViewAuthorization(context(bob), aliceView, new TrinoPrincipal(USER, "user")));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetViewComment(context(bob), aliceView));
    }

    @Test
    @SuppressWarnings("PMD")
    public void testMaterializedViewOperations()
    {
        CatalogSchemaTableName newViewName = new CatalogSchemaTableName(aliceView.getCatalogName(), aliceView.getSchemaTableName().getSchemaName(), "new-view");

        accessControlManager.checkCanCreateMaterializedView(context(alice), aliceView, Collections.emptyMap());
        accessControlManager.checkCanRefreshMaterializedView(context(alice), aliceView);
        accessControlManager.checkCanSetMaterializedViewProperties(context(alice), aliceView, Collections.emptyMap());
        accessControlManager.checkCanDropMaterializedView(context(alice), aliceView);
        accessControlManager.checkCanRenameMaterializedView(context(alice), aliceView, newViewName);

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateMaterializedView(context(bob), aliceView, Collections.emptyMap()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRefreshMaterializedView(context(bob), aliceView));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanSetMaterializedViewProperties(context(bob), aliceView, Collections.emptyMap()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropMaterializedView(context(bob), aliceView));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRenameMaterializedView(context(bob), aliceView, newViewName));
    }

    @Test
    public void testRoleOperations()
    {
        accessControlManager.checkCanCreateRole(context(admin), "role-1", Optional.of(new TrinoPrincipal(USER, "principal")));
        accessControlManager.checkCanDropRole(context(admin), "role-1");
        accessControlManager.checkCanShowRoles(context(admin));
        accessControlManager.checkCanGrantRoles(context(admin), Collections.singleton("role-1"), Collections.singleton(new TrinoPrincipal(USER, "principal")), false, Optional.empty());
        accessControlManager.checkCanRevokeRoles(context(admin), Collections.singleton("role-1"), Collections.singleton(new TrinoPrincipal(USER, "principal")), false, Optional.empty());
        accessControlManager.checkCanShowCurrentRoles(context(alice));
        accessControlManager.checkCanShowRoleGrants(context(alice));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanCreateRole(context(bob), "role-1", Optional.of(new TrinoPrincipal(USER, "principal"))));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDropRole(context(bob), "role-1"));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanShowRoles(context(bob)));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantRoles(context(bob), Collections.singleton("role-1"), Collections.singleton(new TrinoPrincipal(USER, "principal")), false, Optional.empty()));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeRoles(context(bob), Collections.singleton("role-1"), Collections.singleton(new TrinoPrincipal(USER, "principal")), false, Optional.empty()));
    }

    @Test
    public void testProcedureOperations()
    {
        accessControlManager.checkCanExecuteProcedure(context(alice), aliceProcedure);
        accessControlManager.checkCanExecuteTableProcedure(context(alice), aliceTable, aliceProcedure.getRoutineName());

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanExecuteProcedure(context(bob), aliceProcedure));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanExecuteTableProcedure(context(bob), aliceTable, aliceProcedure.getRoutineName()));
    }

    @Test
    public void testFunctionOperations()
    {
        accessControlManager.checkCanCreateFunction(context(alice), aliceFunction);
        accessControlManager.checkCanDropFunction(context(alice), aliceFunction);
        accessControlManager.checkCanShowCreateFunction(context(alice), aliceFunction);
        accessControlManager.checkCanShowFunctions(context(alice), aliceSchema);
        accessControlManager.canCreateViewWithExecuteFunction(context(alice), aliceFunction);

        Set<SchemaFunctionName> functionNames = Collections.singleton(new SchemaFunctionName(aliceSchema.getSchemaName(), aliceFunction.getRoutineName()));

        assertEquals(functionNames, accessControlManager.filterFunctions(context(alice), aliceCatalog, functionNames));
        assertEquals(Collections.emptySet(), accessControlManager.filterFunctions(context(bob), aliceCatalog, functionNames));
    }

    @Test
    public void testEntityPrivileges()
    {
        EntityPrivilege entPrivSelect = new EntityPrivilege("select");
        TrinoPrincipal grantee = new TrinoPrincipal(USER, "user");
        boolean grantOption = false;
        EntityKindAndName entAliceSchema = new EntityKindAndName("schema", Arrays.asList(aliceSchema.getCatalogName(), aliceSchema.getSchemaName()));
        EntityKindAndName entAliceTable = new EntityKindAndName("table", Arrays.asList(aliceTable.getCatalogName(), aliceSchema.getSchemaName(), aliceTable.getSchemaTableName().getTableName()));

        accessControlManager.checkCanGrantEntityPrivilege(context(alice), entPrivSelect, entAliceSchema, grantee, grantOption);
        accessControlManager.checkCanGrantEntityPrivilege(context(alice), entPrivSelect, entAliceTable, grantee, grantOption);
        accessControlManager.checkCanDenyEntityPrivilege(context(alice), entPrivSelect, entAliceSchema, grantee);
        accessControlManager.checkCanDenyEntityPrivilege(context(alice), entPrivSelect, entAliceTable, grantee);
        accessControlManager.checkCanRevokeEntityPrivilege(context(alice), entPrivSelect, entAliceSchema, grantee, grantOption);
        accessControlManager.checkCanRevokeEntityPrivilege(context(alice), entPrivSelect, entAliceTable, grantee, grantOption);

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantEntityPrivilege(context(bob), entPrivSelect, entAliceSchema, grantee, grantOption));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanGrantEntityPrivilege(context(bob), entPrivSelect, entAliceTable, grantee, grantOption));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDenyEntityPrivilege(context(bob), entPrivSelect, entAliceSchema, grantee));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanDenyEntityPrivilege(context(bob), entPrivSelect, entAliceTable, grantee));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeEntityPrivilege(context(bob), entPrivSelect, entAliceSchema, grantee, grantOption));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanRevokeEntityPrivilege(context(bob), entPrivSelect, entAliceTable, grantee, grantOption));
    }

    @Test
    @SuppressWarnings("PMD")
    public void testColumnMask()
    {
        final VarcharType varcharType = VarcharType.createVarcharType(20);

        // MASK_NONE
        Optional<ViewExpression> ret = accessControlManager.getColumnMask(context(alice), aliceTable, "national_id", varcharType);
        assertFalse(ret.isPresent());

        // MASK_SHOW_FIRST_4
        ret = accessControlManager.getColumnMask(context(bob), aliceTable, "national_id", varcharType);
        assertTrue(ret.isPresent());
        assertEquals("cast(regexp_replace(national_id, '(^.{4})(.*)', x -> x[1] || regexp_replace(x[2], '.', 'X')) as varchar(20))", ret.get().getExpression());
    }

    @Test
    public void testRowFilters()
    {
        List<ViewExpression> retArray = accessControlManager.getRowFilters(context(alice), aliceTable);
        assertTrue(retArray.isEmpty());

        retArray = accessControlManager.getRowFilters(context(bob), aliceTable);
        assertFalse(retArray.isEmpty());
        assertEquals(1, retArray.size());
        assertEquals("status = 'active'", retArray.get(0).getExpression());
    }

    private SystemSecurityContext context(Identity id)
    {
        return new SystemSecurityContext(id, new QueryId("id_1"), Instant.now());
    }
}
