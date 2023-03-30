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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.context.ContextDsl.executeWith;
import static io.trino.tempto.sql.SqlContexts.createViewAs;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestGrantRevoke
        extends ProductTest
{
    private static final Set<String> PREDEFINED_ROLES = ImmutableSet.of("admin", "public");

    @Inject
    @Named("databases.presto.jdbc_user")
    private String userName;

    private String tableName;
    private String viewName;
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;
    private QueryExecutor charlieExecutor;

    /*
     * Pre-requisites for the tests in this class:
     *
     * (1) hive.properties file should have this property set: hive.security=sql-standard
     * (2) tempto-configuration.yaml file should have definitions for the following connections to Presto server:
     *          - "alice@presto" that has "jdbc_user: alice"
     *          - "bob@presto" that has "jdbc_user: bob"
     *          - "charlie@presto" that has "jdbc_user: charlie"
     *     (all other values of the connection are same as that of the default "presto" connection).
     */

    @BeforeTestWithContext
    public void setup()
    {
        tableName = "alice_owned_table";
        viewName = "alice_view";
        aliceExecutor = connectToTrino("alice@presto");
        bobExecutor = connectToTrino("bob@presto");
        charlieExecutor = connectToTrino("charlie@presto");

        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        aliceExecutor.executeQuery(format("CREATE TABLE %s(month bigint, day bigint)", tableName));

        onTrino().executeQuery("SET ROLE admin IN hive");
        onHive().executeQuery("SET ROLE admin");
        assertAccessDeniedOnAllOperationsOnTable(bobExecutor, tableName);
    }

    @AfterTestWithContext
    public void cleanup()
    {
        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        aliceExecutor.executeQuery(format("DROP VIEW IF EXISTS %s", viewName));
        cleanupRoles();

        // should not be closed, this would close a shared, global QueryExecutor
        aliceExecutor = null;
        bobExecutor = null;
        charlieExecutor = null;
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void cleanupRoles()
    {
        for (String role : listRoles()) {
            if (!PREDEFINED_ROLES.contains(role)) {
                onHive().executeQuery(format("DROP ROLE %s", role));
            }
        }
    }

    private Set<String> listRoles()
    {
        return onHive().executeQuery("SHOW ROLES")
                .rows()
                .stream()
                .map(row -> row.get(0).toString())
                .collect(toImmutableSet());
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevoke()
    {
        // test GRANT
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        aliceExecutor.executeQuery(format("GRANT INSERT, SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).hasRowsCount(1);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        assertQueryFailure(() -> bobExecutor.executeQuery(format("DELETE FROM %s WHERE day=3", tableName)))
                .hasMessageContaining("Access Denied: Cannot delete from table default.%s", tableName);

        // test REVOKE
        aliceExecutor.executeQuery(format("REVOKE INSERT ON %s FROM bob", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("INSERT INTO %s VALUES ('y', 5)", tableName)))
                .hasMessageContaining("Access Denied: Cannot insert into table default.%s", tableName);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        aliceExecutor.executeQuery(format("REVOKE INSERT, SELECT ON %s FROM bob", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantOptionsOnGrantedPrivilege()
    {
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertQueryFailure(() -> aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName)))
                // Updating a privilege with GRANT OPTION is not supported by Hive. https://issues.apache.org/jira/browse/HIVE-15689
                .hasMessageContaining("Granting SELECT WITH GRANT OPTION is not supported while USER bob possesses SELECT");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeWithGrantOption()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER charlie IN hive");

        // test GRANT WITH GRANT OPTION
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));
        assertQueryFailure(() -> charlieExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        bobExecutor.executeQuery(format("GRANT SELECT ON %s TO ROLE role1", tableName));
        assertThat(charlieExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();

        // test REVOKE WITH GRANT OPTION
        aliceExecutor.executeQuery(format("REVOKE GRANT OPTION FOR SELECT ON %s FROM bob", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("GRANT SELECT ON %s TO ROLE role1 ", tableName)))
                .hasMessageContaining("Access Denied: Cannot grant privilege SELECT on table default.%s", tableName);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        // Since Hive doesn't support REVOKE with CASCADE, charlie would still have access to table
        assertThat(charlieExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();

        // test GRANT WITH GRANT OPTION post revoke
        assertQueryFailure(() -> aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName)))
                // Updating a privilege with GRANT OPTION is not supported by Hive. https://issues.apache.org/jira/browse/HIVE-15689
                .hasMessageContaining("Granting SELECT WITH GRANT OPTION is not supported while USER bob possesses SELECT");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowGrants()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery(format("GRANT SELECT ON %s TO ROLE role1", tableName));
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));
        aliceExecutor.executeQuery(format("GRANT INSERT ON %s TO bob", tableName));

        assertThat(bobExecutor.executeQuery(format("SHOW GRANTS ON %s", tableName)))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "bob", "USER", "hive", "default", "alice_owned_table", "SELECT", "YES", null),
                        row("alice", "USER", "bob", "USER", "hive", "default", "alice_owned_table", "INSERT", "NO", null),
                        row(userName, "USER", "role1", "ROLE", "hive", "default", "alice_owned_table", "SELECT", "NO", null)));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAll()
    {
        aliceExecutor.executeQuery(format("GRANT ALL PRIVILEGES ON %s TO USER bob", tableName));
        assertThat(bobExecutor.executeQuery(format("INSERT INTO %s VALUES (4, 13)", tableName))).hasRowsCount(1);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        bobExecutor.executeQuery(format("DELETE FROM %s", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();

        aliceExecutor.executeQuery(format("REVOKE ALL PRIVILEGES ON %s FROM USER bob", tableName));
        assertAccessDeniedOnAllOperationsOnTable(bobExecutor, tableName);

        assertThat(bobExecutor.executeQuery(format("SHOW GRANTS ON %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublic()
    {
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO ROLE PUBLIC", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        aliceExecutor.executeQuery(format("REVOKE SELECT ON %s FROM ROLE PUBLIC", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCustomRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO ROLE role1", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        aliceExecutor.executeQuery(format("REVOKE SELECT ON %s FROM ROLE role1", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testTransitiveRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO ROLE role2", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        aliceExecutor.executeQuery(format("REVOKE SELECT ON %s FROM ROLE role2", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropRoleWithPermissionsGranted()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO ROLE role1", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        onTrino().executeQuery("DROP ROLE role1 IN hive");
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testTableOwnerPrivileges()
    {
        onHive().executeQuery("set role admin;");
        assertThat(onHive().executeQuery(format("SHOW GRANT USER alice ON TABLE %s", tableName))
                .project(7, 8)) // Project only two relevant columns of SHOW GRANT: Privilege and Grant Option
                .containsOnly(ownerGrants());
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testViewOwnerPrivileges()
    {
        onHive().executeQuery("set role admin;");
        executeWith(createViewAs(viewName, format("SELECT * FROM %s", tableName), aliceExecutor), view -> {
            assertThat(onHive().executeQuery(format("SHOW GRANT USER alice ON %s", viewName))
                    .project(7, 8)) // Project only two relevant columns of SHOW GRANT: Privilege and Grant Option
                    .containsOnly(ownerGrants());
        });
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testTablePrivilegesWithHiveOnlyViews()
    {
        executeWith(createViewAs("hive_only_view", format("SELECT * FROM %s", tableName), onHive()), view -> {
            assertThat(onTrino().executeQuery("SELECT DISTINCT table_name FROM information_schema.table_privileges"))
                    .contains(row(tableName))
                    .contains(row(view.getName()));
            assertThat(onTrino().executeQuery("SHOW GRANTS").project(7))
                    .contains(row(tableName))
                    .contains(row(view.getName()));
        });
    }

    private ImmutableList<Row> ownerGrants()
    {
        return ImmutableList.of(row("SELECT", Boolean.TRUE), row("INSERT", Boolean.TRUE), row("UPDATE", Boolean.TRUE), row("DELETE", Boolean.TRUE));
    }

    private static void assertAccessDeniedOnAllOperationsOnTable(QueryExecutor queryExecutor, String tableName)
    {
        assertQueryFailure(() -> queryExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertQueryFailure(() -> queryExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName)))
                .hasMessageContaining("Access Denied: Cannot insert into table default.%s", tableName);
        assertQueryFailure(() -> queryExecutor.executeQuery(format("DELETE FROM %s WHERE day=3", tableName)))
                .hasMessageContaining("Access Denied: Cannot delete from table default.%s", tableName);
    }
}
