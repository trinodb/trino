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
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.context.ContextDsl.executeWith;
import static io.trino.tempto.sql.SqlContexts.createViewAs;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGrantRevoke
        extends ProductTest
{
    private static final Set<String> PREDEFINED_ROLES = ImmutableSet.of("admin", "public");

    @Inject
    @Named("databases.trino.jdbc_user")
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
     * (2) tempto-configuration.yaml file should have definitions for the following connections to Trino server:
     *          - "alice@trino" that has "jdbc_user: alice"
     *          - "bob@trino" that has "jdbc_user: bob"
     *          - "charlie@trino" that has "jdbc_user: charlie"
     *     (all other values of the connection are same as that of the default "trino" connection).
     */

    @BeforeMethodWithContext
    public void setup()
    {
        tableName = "alice_owned_table";
        viewName = "alice_view";
        aliceExecutor = connectToTrino("alice@trino");
        bobExecutor = connectToTrino("bob@trino");
        charlieExecutor = connectToTrino("charlie@trino");

        aliceExecutor.executeQuery("DROP TABLE IF EXISTS %s".formatted(tableName));
        aliceExecutor.executeQuery("CREATE TABLE %s(month bigint, day bigint)".formatted(tableName));

        onTrino().executeQuery("SET ROLE admin IN hive");
        onHive().executeQuery("SET ROLE admin");
        assertAccessDeniedOnAllOperationsOnTable(bobExecutor, tableName);
    }

    @AfterMethodWithContext
    public void cleanup()
    {
        aliceExecutor.executeQuery("DROP TABLE IF EXISTS %s".formatted(tableName));
        aliceExecutor.executeQuery("DROP VIEW IF EXISTS %s".formatted(viewName));
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
                onHive().executeQuery("DROP ROLE %s".formatted(role));
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
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
        aliceExecutor.executeQuery("GRANT INSERT, SELECT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("INSERT INTO %s VALUES (3, 22)".formatted(tableName))).hasRowsCount(1);
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasRowsCount(1);
        assertQueryFailure(() -> bobExecutor.executeQuery("DELETE FROM %s WHERE day=3".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot delete from table default.%s", tableName);

        // test REVOKE
        aliceExecutor.executeQuery("REVOKE INSERT ON %s FROM bob".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("INSERT INTO %s VALUES ('y', 5)".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot insert into table default.%s", tableName);
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasRowsCount(1);
        aliceExecutor.executeQuery("REVOKE INSERT, SELECT ON %s FROM bob".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantOptionsOnGrantedPrivilege()
    {
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        assertQueryFailure(() -> aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob WITH GRANT OPTION".formatted(tableName)))
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
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob WITH GRANT OPTION".formatted(tableName));
        assertQueryFailure(() -> charlieExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        bobExecutor.executeQuery("GRANT SELECT ON %s TO ROLE role1".formatted(tableName));
        assertThat(charlieExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();

        // test REVOKE WITH GRANT OPTION
        aliceExecutor.executeQuery("REVOKE GRANT OPTION FOR SELECT ON %s FROM bob".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("GRANT SELECT ON %s TO ROLE role1 ".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot grant privilege SELECT on table default.%s", tableName);
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
        // Since Hive doesn't support REVOKE with CASCADE, charlie would still have access to table
        assertThat(charlieExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();

        // test GRANT WITH GRANT OPTION post revoke
        assertQueryFailure(() -> aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob WITH GRANT OPTION".formatted(tableName)))
                // Updating a privilege with GRANT OPTION is not supported by Hive. https://issues.apache.org/jira/browse/HIVE-15689
                .hasMessageContaining("Granting SELECT WITH GRANT OPTION is not supported while USER bob possesses SELECT");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowGrants()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT SELECT ON %s TO ROLE role1".formatted(tableName));
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob WITH GRANT OPTION".formatted(tableName));
        aliceExecutor.executeQuery("GRANT INSERT ON %s TO bob".formatted(tableName));

        assertThat(bobExecutor.executeQuery("SHOW GRANTS ON %s".formatted(tableName)))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "bob", "USER", "hive", "default", "alice_owned_table", "SELECT", "YES", null),
                        row("alice", "USER", "bob", "USER", "hive", "default", "alice_owned_table", "INSERT", "NO", null),
                        row(userName, "USER", "role1", "ROLE", "hive", "default", "alice_owned_table", "SELECT", "NO", null)));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAll()
    {
        aliceExecutor.executeQuery("GRANT ALL PRIVILEGES ON %s TO USER bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("INSERT INTO %s VALUES (4, 13)".formatted(tableName))).hasRowsCount(1);
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasRowsCount(1);
        bobExecutor.executeQuery("DELETE FROM %s".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();

        aliceExecutor.executeQuery("REVOKE ALL PRIVILEGES ON %s FROM USER bob".formatted(tableName));
        assertAccessDeniedOnAllOperationsOnTable(bobExecutor, tableName);

        assertThat(bobExecutor.executeQuery("SHOW GRANTS ON %s".formatted(tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublic()
    {
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO ROLE PUBLIC".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
        aliceExecutor.executeQuery("REVOKE SELECT ON %s FROM ROLE PUBLIC".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCustomRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO ROLE role1".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
        aliceExecutor.executeQuery("REVOKE SELECT ON %s FROM ROLE role1".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testTransitiveRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO ROLE role2".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
        aliceExecutor.executeQuery("REVOKE SELECT ON %s FROM ROLE role2".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropRoleWithPermissionsGranted()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO ROLE role1".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
        onTrino().executeQuery("DROP ROLE role1 IN hive");
        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertThat(aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testTableOwnerPrivileges()
    {
        onHive().executeQuery("set role admin;");
        assertThat(onHive().executeQuery("SHOW GRANT USER alice ON TABLE %s".formatted(tableName))
                .project(7, 8)) // Project only two relevant columns of SHOW GRANT: Privilege and Grant Option
                .containsOnly(ownerGrants());
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testViewOwnerPrivileges()
    {
        onHive().executeQuery("set role admin;");
        executeWith(createViewAs(viewName, "SELECT * FROM %s".formatted(tableName), aliceExecutor), _ -> {
            assertThat(onHive().executeQuery("SHOW GRANT USER alice ON %s".formatted(viewName))
                    .project(7, 8)) // Project only two relevant columns of SHOW GRANT: Privilege and Grant Option
                    .containsOnly(ownerGrants());
        });
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testTablePrivilegesWithHiveOnlyViews()
    {
        executeWith(createViewAs("hive_only_view", "SELECT * FROM %s".formatted(tableName), onHive()), view -> {
            assertThat(onTrino().executeQuery("SELECT DISTINCT table_name FROM information_schema.table_privileges"))
                    .contains(row(tableName))
                    .contains(row(view.getName()));
            assertThat(onTrino().executeQuery("SHOW GRANTS").project(7))
                    .contains(row(tableName))
                    .contains(row(view.getName()));
        });
    }

    private List<Row> ownerGrants()
    {
        return ImmutableList.of(row("SELECT", Boolean.TRUE), row("INSERT", Boolean.TRUE), row("UPDATE", Boolean.TRUE), row("DELETE", Boolean.TRUE));
    }

    private static void assertAccessDeniedOnAllOperationsOnTable(QueryExecutor queryExecutor, String tableName)
    {
        assertQueryFailure(() -> queryExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);
        assertQueryFailure(() -> queryExecutor.executeQuery("INSERT INTO %s VALUES (3, 22)".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot insert into table default.%s", tableName);
        assertQueryFailure(() -> queryExecutor.executeQuery("DELETE FROM %s WHERE day=3".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot delete from table default.%s", tableName);
    }
}
