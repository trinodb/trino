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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.security.PrincipalType.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRangerSystemAccessControl
{
    private static RangerSystemAccessControl accessControlManager;

    private static final Identity ALICE = new Identity.Builder("alice").withPrincipal(new BasicPrincipal("alice")).build();
    private static final Identity ADMIN = new Identity.Builder("admin").withPrincipal(new BasicPrincipal("admin")).build();
    private static final Identity KERBEROS_INVALID_ALICE = Identity.from(ALICE).withPrincipal(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")).build();
    private static final Identity BOB = Identity.ofUser("bob");

    private static final Set<String> ALL_CATALOGS = ImmutableSet.of("open-to-all", "all-allowed", "alice-catalog", "user-catalog");
    private static final Set<Identity> QUERY_OWNERS = ImmutableSet.of(Identity.ofUser("bob"), Identity.ofUser("alice"), Identity.ofUser("frank"));
    private static final String CATALOG_ALICE = "alice-catalog";
    private static final String CATALOG_USER_HOME = "user-catalog";
    private static final CatalogSchemaName SCHEMA_ALICE_SCH1 = new CatalogSchemaName(CATALOG_ALICE, "sch1");
    private static final CatalogSchemaTableName TABLE_ALICE_SCH1_TBL1 = new CatalogSchemaTableName(CATALOG_ALICE, "sch1", "tbl1");
    private static final CatalogSchemaTableName VIEW_ALICE_SCH1_VW1 = new CatalogSchemaTableName(CATALOG_ALICE, "sch1", "vw1");
    private static final CatalogSchemaRoutineName PROC_ALICE_SCH1_PROC1 = new CatalogSchemaRoutineName(CATALOG_ALICE, "sch1", "proc1");
    private static final CatalogSchemaRoutineName FUNC_ALICE_SCH1_FUNC1 = new CatalogSchemaRoutineName(CATALOG_ALICE, "sch1", "func1");
    private static final CatalogSchemaName SCHEMA_USER_BOB = new CatalogSchemaName(CATALOG_USER_HOME, "bob_schema");
    private static final CatalogSchemaTableName TABLE_USER_BOB_TBL1 = new CatalogSchemaTableName(CATALOG_USER_HOME, SCHEMA_USER_BOB.getSchemaName(), "tbl1");

    @BeforeAll
    public static void setUpBeforeClass()
            throws Exception
    {
        RangerConfig config = new RangerConfig();

        config.setServiceName("dev_trino");

        accessControlManager = new RangerSystemAccessControl(config);
    }

    @Test
    void testCanSetUserOperations()
    {
        accessControlManager.checkCanSetUser(ADMIN.getPrincipal(), BOB.getUser());
        accessControlManager.checkCanImpersonateUser(ADMIN, BOB.getUser());
        accessControlManager.checkCanViewQueryOwnedBy(ADMIN, BOB);
        accessControlManager.checkCanKillQueryOwnedBy(ADMIN, BOB);
        assertThat(accessControlManager.filterViewQueryOwnedBy(ALICE, QUERY_OWNERS)).isEmpty();

        assertThatThrownBy(() -> accessControlManager.checkCanSetUser(ALICE.getPrincipal(), BOB.getUser())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(ALICE, BOB.getUser())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanViewQueryOwnedBy(ALICE, BOB)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanKillQueryOwnedBy(ALICE, BOB)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(KERBEROS_INVALID_ALICE, BOB.getUser())).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testSystemInformationOperations()
    {
        accessControlManager.checkCanReadSystemInformation(ADMIN);
        accessControlManager.checkCanWriteSystemInformation(ADMIN);

        assertThatThrownBy(() -> accessControlManager.checkCanReadSystemInformation(ALICE)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanWriteSystemInformation(ALICE)).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testSystemSessionPropertyOperations()
    {
        accessControlManager.checkCanSetSystemSessionProperty(ADMIN, new QueryId("q1"), "test-property");

        assertThatThrownBy(() -> accessControlManager.checkCanSetSystemSessionProperty(ALICE, new QueryId("q1"), "test-property")).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testQueryOperations()
    {
        accessControlManager.checkCanExecuteQuery(ADMIN, new QueryId("1"));

        assertThatThrownBy(() -> accessControlManager.checkCanExecuteQuery(ALICE, new QueryId("1"))).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testCatalogOperations()
    {
        accessControlManager.canAccessCatalog(context(ALICE), CATALOG_ALICE);
        accessControlManager.checkCanCreateCatalog(context(ALICE), CATALOG_ALICE);
        accessControlManager.checkCanDropCatalog(context(ALICE), CATALOG_ALICE);
        accessControlManager.checkCanSetCatalogSessionProperty(context(ALICE), CATALOG_ALICE, "property");
        assertThat(accessControlManager.filterCatalogs(context(ALICE), ALL_CATALOGS)).isEqualTo(ALL_CATALOGS);
        assertThat(accessControlManager.filterCatalogs(context(BOB), ALL_CATALOGS)).isEqualTo(ImmutableSet.of("open-to-all", "all-allowed", "user-catalog"));

        assertThat(accessControlManager.canAccessCatalog(context(BOB), CATALOG_ALICE)).isFalse();

        assertThatThrownBy(() -> accessControlManager.checkCanCreateCatalog(context(BOB), CATALOG_ALICE)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanDropCatalog(context(BOB), CATALOG_ALICE)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetCatalogSessionProperty(context(BOB), CATALOG_ALICE, "property")).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testSchemaOperations()
    {
        accessControlManager.checkCanCreateSchema(context(ALICE), SCHEMA_ALICE_SCH1, null);
        accessControlManager.checkCanDropSchema(context(ALICE), SCHEMA_ALICE_SCH1);
        accessControlManager.checkCanRenameSchema(context(ALICE), SCHEMA_ALICE_SCH1, "new-schema");
        accessControlManager.checkCanShowSchemas(context(ALICE), CATALOG_ALICE);
        accessControlManager.checkCanSetSchemaAuthorization(context(ALICE), SCHEMA_ALICE_SCH1, new TrinoPrincipal(USER, "principal"));
        accessControlManager.checkCanShowCreateSchema(context(ALICE), SCHEMA_ALICE_SCH1);

        Set<String> aliceSchemas = ImmutableSet.of("sch1");
        assertThat(accessControlManager.filterSchemas(context(ALICE), CATALOG_ALICE, aliceSchemas)).isEqualTo(aliceSchemas);
        assertThat(accessControlManager.filterSchemas(context(BOB), "alice-catalog", aliceSchemas)).isEmpty();

        assertThatThrownBy(() -> accessControlManager.checkCanCreateSchema(context(BOB), SCHEMA_ALICE_SCH1, null)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanDropSchema(context(BOB), SCHEMA_ALICE_SCH1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRenameSchema(context(BOB), SCHEMA_ALICE_SCH1, "new-schema")).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanShowSchemas(context(BOB), SCHEMA_ALICE_SCH1.getCatalogName())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetSchemaAuthorization(context(BOB), SCHEMA_ALICE_SCH1, new TrinoPrincipal(USER, "principal"))).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanShowCreateSchema(context(BOB), SCHEMA_ALICE_SCH1)).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testTableOperations()
    {
        CatalogSchemaTableName newTableName = new CatalogSchemaTableName("alice-catalog", "sch1", "new-table");

        accessControlManager.checkCanCreateTable(context(ALICE), TABLE_ALICE_SCH1_TBL1, ImmutableMap.of());
        accessControlManager.checkCanDropTable(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanRenameTable(context(ALICE), TABLE_ALICE_SCH1_TBL1, newTableName);
        accessControlManager.checkCanSetTableProperties(context(ALICE), TABLE_ALICE_SCH1_TBL1, ImmutableMap.of());
        accessControlManager.checkCanSetTableComment(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanSetTableAuthorization(context(ALICE), TABLE_ALICE_SCH1_TBL1, new TrinoPrincipal(USER, "principal"));
        accessControlManager.checkCanShowTables(context(ALICE), SCHEMA_ALICE_SCH1);
        accessControlManager.checkCanShowCreateTable(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanInsertIntoTable(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanDeleteFromTable(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanTruncateTable(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanCreateTable(context(BOB), TABLE_USER_BOB_TBL1, ImmutableMap.of());
        accessControlManager.checkCanDropTable(context(BOB), TABLE_USER_BOB_TBL1);

        Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("sch1", "tbl1"));
        assertThat(accessControlManager.filterTables(context(ALICE), CATALOG_ALICE, aliceTables)).isEqualTo(aliceTables);
        assertThat(accessControlManager.filterTables(context(BOB), "alice-catalog", aliceTables)).isEmpty();

        assertThatThrownBy(() -> accessControlManager.checkCanCreateTable(context(BOB), TABLE_ALICE_SCH1_TBL1, ImmutableMap.of())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanDropTable(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRenameTable(context(BOB), TABLE_ALICE_SCH1_TBL1, newTableName)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetTableProperties(context(BOB), TABLE_ALICE_SCH1_TBL1, Collections.emptyMap())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetTableComment(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetTableAuthorization(context(BOB), TABLE_ALICE_SCH1_TBL1, new TrinoPrincipal(USER, "principal"))).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanShowTables(context(BOB), SCHEMA_ALICE_SCH1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanShowCreateTable(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanInsertIntoTable(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanDeleteFromTable(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanTruncateTable(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testColumnOperations()
    {
        accessControlManager.checkCanAddColumn(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanAlterColumn(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanDropColumn(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanRenameColumn(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanSetColumnComment(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanShowColumns(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        accessControlManager.checkCanSelectFromColumns(context(ALICE), TABLE_ALICE_SCH1_TBL1, ImmutableSet.of());
        accessControlManager.checkCanUpdateTableColumns(context(ALICE), TABLE_ALICE_SCH1_TBL1, ImmutableSet.of());
        accessControlManager.checkCanAddColumn(context(BOB), TABLE_USER_BOB_TBL1);
        accessControlManager.checkCanSelectFromColumns(context(BOB), TABLE_USER_BOB_TBL1, ImmutableSet.of());
        accessControlManager.checkCanDropColumn(context(BOB), TABLE_USER_BOB_TBL1);

        Set<String> columns = ImmutableSet.of("column-1");
        Map<SchemaTableName, Set<String>> tableColumns = ImmutableMap.of(TABLE_ALICE_SCH1_TBL1.getSchemaTableName(), columns);

        assertThat(accessControlManager.filterColumns(context(ALICE), TABLE_ALICE_SCH1_TBL1, columns)).isEqualTo(columns);
        assertThat(accessControlManager.filterColumns(context(ALICE), TABLE_ALICE_SCH1_TBL1.getCatalogName(), tableColumns)).isEqualTo(tableColumns);
        assertThat(accessControlManager.filterColumns(context(BOB), TABLE_ALICE_SCH1_TBL1, columns)).isEmpty();
        assertThat(accessControlManager.filterColumns(context(BOB), TABLE_ALICE_SCH1_TBL1.getCatalogName(), tableColumns)).isEqualTo(Collections.singletonMap(TABLE_ALICE_SCH1_TBL1.getSchemaTableName(), Collections.emptySet()));

        assertThatThrownBy(() -> accessControlManager.checkCanAddColumn(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanAlterColumn(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanDropColumn(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRenameColumn(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetColumnComment(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanShowColumns(context(BOB), TABLE_ALICE_SCH1_TBL1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSelectFromColumns(context(BOB), TABLE_ALICE_SCH1_TBL1, ImmutableSet.of())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanUpdateTableColumns(context(BOB), TABLE_ALICE_SCH1_TBL1, Collections.emptySet())).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testViewOperations()
    {
        CatalogSchemaTableName newViewName = new CatalogSchemaTableName(VIEW_ALICE_SCH1_VW1.getCatalogName(), VIEW_ALICE_SCH1_VW1.getSchemaTableName().getSchemaName(), "new-view");

        accessControlManager.checkCanCreateView(context(ALICE), VIEW_ALICE_SCH1_VW1, Optional.empty());
        accessControlManager.checkCanDropView(context(ALICE), VIEW_ALICE_SCH1_VW1);
        accessControlManager.checkCanRenameView(context(ALICE), VIEW_ALICE_SCH1_VW1, newViewName);
        accessControlManager.checkCanSetViewAuthorization(context(ALICE), VIEW_ALICE_SCH1_VW1, new TrinoPrincipal(USER, "user"));
        accessControlManager.checkCanCreateViewWithSelectFromColumns(context(ALICE), TABLE_ALICE_SCH1_TBL1, ImmutableSet.of());
        accessControlManager.checkCanSetViewAuthorization(context(ALICE), VIEW_ALICE_SCH1_VW1, new TrinoPrincipal(USER, "user"));
        accessControlManager.checkCanSetViewComment(context(ALICE), VIEW_ALICE_SCH1_VW1);

        assertThatThrownBy(() -> accessControlManager.checkCanCreateView(context(BOB), VIEW_ALICE_SCH1_VW1, Optional.empty())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanDropView(context(BOB), VIEW_ALICE_SCH1_VW1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRenameView(context(BOB), VIEW_ALICE_SCH1_VW1, newViewName)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetViewAuthorization(context(BOB), VIEW_ALICE_SCH1_VW1, new TrinoPrincipal(USER, "user"))).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanCreateViewWithSelectFromColumns(context(BOB), TABLE_ALICE_SCH1_TBL1, ImmutableSet.of())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetViewAuthorization(context(BOB), VIEW_ALICE_SCH1_VW1, new TrinoPrincipal(USER, "user"))).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetViewComment(context(BOB), VIEW_ALICE_SCH1_VW1)).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testMaterializedViewOperations()
    {
        CatalogSchemaTableName newViewName = new CatalogSchemaTableName(VIEW_ALICE_SCH1_VW1.getCatalogName(), VIEW_ALICE_SCH1_VW1.getSchemaTableName().getSchemaName(), "new-view");

        accessControlManager.checkCanCreateMaterializedView(context(ALICE), VIEW_ALICE_SCH1_VW1, ImmutableMap.of());
        accessControlManager.checkCanRefreshMaterializedView(context(ALICE), VIEW_ALICE_SCH1_VW1);
        accessControlManager.checkCanSetMaterializedViewProperties(context(ALICE), VIEW_ALICE_SCH1_VW1, ImmutableMap.of());
        accessControlManager.checkCanDropMaterializedView(context(ALICE), VIEW_ALICE_SCH1_VW1);
        accessControlManager.checkCanRenameMaterializedView(context(ALICE), VIEW_ALICE_SCH1_VW1, newViewName);

        assertThatThrownBy(() -> accessControlManager.checkCanCreateMaterializedView(context(BOB), VIEW_ALICE_SCH1_VW1, ImmutableMap.of())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRefreshMaterializedView(context(BOB), VIEW_ALICE_SCH1_VW1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetMaterializedViewProperties(context(BOB), VIEW_ALICE_SCH1_VW1, ImmutableMap.of())).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanDropMaterializedView(context(BOB), VIEW_ALICE_SCH1_VW1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRenameMaterializedView(context(BOB), VIEW_ALICE_SCH1_VW1, newViewName)).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testProcedureOperations()
    {
        accessControlManager.checkCanExecuteProcedure(context(ALICE), PROC_ALICE_SCH1_PROC1);
        accessControlManager.checkCanExecuteTableProcedure(context(ALICE), TABLE_ALICE_SCH1_TBL1, PROC_ALICE_SCH1_PROC1.getRoutineName());

        assertThatThrownBy(() -> accessControlManager.checkCanExecuteProcedure(context(BOB), PROC_ALICE_SCH1_PROC1)).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanExecuteTableProcedure(context(BOB), TABLE_ALICE_SCH1_TBL1, PROC_ALICE_SCH1_PROC1.getRoutineName())).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testFunctionOperations()
    {
        accessControlManager.checkCanCreateFunction(context(ALICE), FUNC_ALICE_SCH1_FUNC1);
        accessControlManager.checkCanDropFunction(context(ALICE), FUNC_ALICE_SCH1_FUNC1);
        accessControlManager.checkCanShowCreateFunction(context(ALICE), FUNC_ALICE_SCH1_FUNC1);
        accessControlManager.checkCanShowFunctions(context(ALICE), SCHEMA_ALICE_SCH1);
        accessControlManager.canCreateViewWithExecuteFunction(context(ALICE), FUNC_ALICE_SCH1_FUNC1);

        assertThat(accessControlManager.canExecuteFunction(context(ALICE), FUNC_ALICE_SCH1_FUNC1)).isTrue();
        assertThat(accessControlManager.canExecuteFunction(context(BOB), FUNC_ALICE_SCH1_FUNC1)).isFalse();

        Set<SchemaFunctionName> functionNames = Set.of(new SchemaFunctionName(SCHEMA_ALICE_SCH1.getSchemaName(), FUNC_ALICE_SCH1_FUNC1.getRoutineName()));

        assertThat(accessControlManager.filterFunctions(context(ALICE), CATALOG_ALICE, functionNames)).isEqualTo(functionNames);
        assertThat(accessControlManager.filterFunctions(context(BOB), CATALOG_ALICE, functionNames)).isEmpty();
    }

    @Test
    public void testColumnMask()
    {
        VarcharType varcharType = VarcharType.createVarcharType(20);

        // MASK_NONE
        Optional<ViewExpression> ret = accessControlManager.getColumnMask(context(ALICE), TABLE_ALICE_SCH1_TBL1, "national_id", varcharType);
        assertThat(ret).isNotPresent();

        // MASK_SHOW_FIRST_4
        ret = accessControlManager.getColumnMask(context(BOB), TABLE_ALICE_SCH1_TBL1, "national_id", varcharType);
        assertThat(ret).isPresent();
        assertThat(ret.get().getExpression()).isEqualTo("cast(regexp_replace(national_id, '(^.{4})(.*)', x -> x[1] || regexp_replace(x[2], '.', 'X')) as varchar(20))");
    }

    @Test
    void testRowFilters()
    {
        List<ViewExpression> retArray = accessControlManager.getRowFilters(context(ALICE), TABLE_ALICE_SCH1_TBL1);
        assertThat(retArray).isEmpty();

        retArray = accessControlManager.getRowFilters(context(BOB), TABLE_ALICE_SCH1_TBL1);
        assertThat(retArray).isNotEmpty();
        assertThat(retArray).hasSize(1);
        assertThat(retArray.getFirst().getExpression()).isEqualTo("status = 'active'");
    }

    private static SystemSecurityContext context(Identity id)
    {
        return new SystemSecurityContext(id, new QueryId("id_1"), Instant.now());
    }
}
