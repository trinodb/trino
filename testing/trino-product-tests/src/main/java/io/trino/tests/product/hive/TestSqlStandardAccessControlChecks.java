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

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlStandardAccessControlChecks
        extends ProductTest
{
    private String tableName = "alice_owned_table";
    private String viewName = "alice_owned_view";
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;
    private QueryExecutor charlieExecutor;
    private QueryExecutor caseSensitiveUserNameExecutor;
    private QueryExecutor hdfsExecutor;

    @BeforeMethodWithContext
    public void setup()
    {
        aliceExecutor = connectToTrino("alice@trino");
        bobExecutor = connectToTrino("bob@trino");
        charlieExecutor = connectToTrino("charlie@trino");
        caseSensitiveUserNameExecutor = connectToTrino("CaseSensitiveUserName@trino");
        hdfsExecutor = connectToTrino("hdfs@trino");

        aliceExecutor.executeQuery("DROP TABLE IF EXISTS %s".formatted(tableName));
        aliceExecutor.executeQuery("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])".formatted(tableName));

        aliceExecutor.executeQuery("DROP VIEW IF EXISTS %s".formatted(viewName));
        aliceExecutor.executeQuery("CREATE VIEW %s AS SELECT month, day FROM %s".formatted(viewName, tableName));
    }

    @AfterMethodWithContext
    public void cleanup()
    {
        // should not be closed, this would close a shared, global QueryExecutor
        aliceExecutor = null;
        bobExecutor = null;
        charlieExecutor = null;
        caseSensitiveUserNameExecutor = null;
        hdfsExecutor = null;
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelect()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();

        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM %s".formatted(viewName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(viewName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(viewName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelectFromPartitions()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("SELECT * FROM \"%s$partitions\"".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s$partitions", tableName);

        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM \"%s$partitions\"".formatted(tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlInsert()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("INSERT INTO %s VALUES (3, 22)".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot insert into table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT INSERT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("INSERT INTO %s VALUES (3, 22)".formatted(tableName))).hasRowsCount(1);
        assertThat(aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasRowsCount(1);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlUpdate()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("UPDATE %s SET month=3, day=22".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot update columns [month, day] in table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT INSERT ON %s TO bob".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("UPDATE %s SET month=3, day=22".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot update columns [month, day] in table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT UPDATE ON %s TO bob".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("UPDATE %s SET month=3, day=22".formatted(tableName)))
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDelete()
    {
        aliceExecutor.executeQuery("INSERT INTO %s VALUES (4, 13)".formatted(tableName));
        assertThat(aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasRowsCount(1);
        assertQueryFailure(() -> bobExecutor.executeQuery("DELETE FROM %s WHERE day=4".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot delete from table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT DELETE ON %s TO bob".formatted(tableName));
        bobExecutor.executeQuery("DELETE FROM %s".formatted(tableName));
        assertThat(aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateTableAsSelect()
    {
        String createTableAsSelect = "bob_create_table_as_select";

        bobExecutor.executeQuery("DROP TABLE IF EXISTS " + createTableAsSelect);
        assertQueryFailure(() -> bobExecutor.executeQuery("CREATE TABLE %s AS SELECT * FROM %s".formatted(createTableAsSelect, tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        bobExecutor.executeQuery("CREATE TABLE %s AS SELECT * FROM %s".formatted(createTableAsSelect, tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(createTableAsSelect))).hasNoRows();
        bobExecutor.executeQuery("DROP TABLE " + createTableAsSelect);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropTable()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("DROP TABLE %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot drop table default.%s", tableName);

        aliceExecutor.executeQuery("DROP TABLE %s".formatted(tableName));
        assertQueryFailure(() -> aliceExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("does not exist");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlAlterTable()
    {
        assertThat(aliceExecutor.executeQuery("SHOW COLUMNS FROM %s".formatted(tableName))).hasRowsCount(2);
        assertQueryFailure(() -> bobExecutor.executeQuery("ALTER TABLE %s ADD COLUMN year bigint".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot add a column to table default.%s", tableName);

        aliceExecutor.executeQuery("ALTER TABLE %s ADD COLUMN year bigint".formatted(tableName));
        assertThat(aliceExecutor.executeQuery("SHOW COLUMNS FROM %s".formatted(tableName))).hasRowsCount(3);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateView()
    {
        String viewName = "bob_view";
        String selectTableSql = "SELECT * FROM %s".formatted(tableName);
        String createViewSql = "CREATE VIEW %s AS %s".formatted(viewName, selectTableSql);

        bobExecutor.executeQuery("DROP VIEW IF EXISTS %s".formatted(viewName));

        // Bob needs SELECT on the table to create the view
        bobExecutor.executeQuery("DROP VIEW IF EXISTS " + viewName);
        assertQueryFailure(() -> bobExecutor.executeQuery(createViewSql))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        // Give Bob access to table, then create and execute view
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        bobExecutor.executeQuery(createViewSql);
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(viewName))).hasNoRows();

        // Verify that Charlie does not have SELECT on the view, then grant access
        assertQueryFailure(() -> charlieExecutor.executeQuery("SELECT * FROM %s".formatted(viewName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
        bobExecutor.executeQuery("GRANT SELECT ON %s TO charlie".formatted(viewName));

        // Charlie still cannot access view because Bob does not have SELECT WITH GRANT OPTION
        assertQueryFailure(() -> charlieExecutor.executeQuery("SELECT * FROM %s".formatted(viewName)))
                .hasMessageContaining("Access Denied: View owner does not have sufficient privileges: View owner 'bob' cannot create view that selects from default.%s", tableName);

        // Give Bob SELECT WITH GRANT OPTION on the underlying table
        aliceExecutor.executeQuery("REVOKE SELECT ON %s FROM bob".formatted(tableName));
        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob WITH GRANT OPTION".formatted(tableName));

        // Bob has GRANT OPTION, so both Bob and Charlie can access the view
        assertThat(bobExecutor.executeQuery("SELECT * FROM %s".formatted(viewName))).hasNoRows();
        assertThat(charlieExecutor.executeQuery("SELECT * FROM %s".formatted(viewName))).hasNoRows();

        bobExecutor.executeQuery("DROP VIEW " + viewName);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropView()
    {
        String viewName = "alice_view_for_drop";

        aliceExecutor.executeQuery("DROP VIEW IF EXISTS " + viewName);
        aliceExecutor.executeQuery("CREATE VIEW %s AS SELECT * FROM %s".formatted(viewName, tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("DROP VIEW %s".formatted(viewName)))
                .hasMessageContaining("Access Denied: Cannot drop view default.%s", viewName);

        aliceExecutor.executeQuery("DROP VIEW %s".formatted(viewName));
        assertQueryFailure(() -> aliceExecutor.executeQuery("SELECT * FROM %s".formatted(viewName)))
                .hasMessageContaining("does not exist");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelectWithCaseSensitiveUserName()
    {
        assertQueryFailure(() -> caseSensitiveUserNameExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        onHive().executeQuery("SET ROLE admin");

        // make sure that case matters
        onHive().executeQuery("GRANT SELECT ON TABLE %s TO USER casesensitiveusername".formatted(tableName));
        assertQueryFailure(() -> caseSensitiveUserNameExecutor.executeQuery("SELECT * FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        onHive().executeQuery("GRANT SELECT ON TABLE %s TO USER CaseSensitiveUserName".formatted(tableName));
        assertThat(caseSensitiveUserNameExecutor.executeQuery("SELECT * FROM %s".formatted(tableName))).hasNoRows();

        assertQueryFailure(() -> caseSensitiveUserNameExecutor.executeQuery("SELECT * FROM %s".formatted(viewName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
        onHive().executeQuery("GRANT SELECT ON TABLE %s TO USER CaseSensitiveUserName".formatted(viewName));
        assertThat(caseSensitiveUserNameExecutor.executeQuery("SELECT * FROM %s".formatted(viewName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSetTableAuthorization()
    {
        // Using custom table name as drop table as different user may leave some data in HDFS
        String tableName = "set_table_authorization_test";
        aliceExecutor.executeQuery("CREATE TABLE %s(col1 bigint, col2 bigint)".formatted(tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery("ALTER TABLE %s SET AUTHORIZATION bob".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot set authorization for table default.%s to USER bob", tableName);
        hdfsExecutor.executeQuery("SET SESSION legacy_catalog_roles=true");
        hdfsExecutor.executeQuery("SET ROLE ADMIN");
        hdfsExecutor.executeQuery("ALTER TABLE %s SET AUTHORIZATION bob".formatted(tableName));
        bobExecutor.executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSetViewAuthorization()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("ALTER VIEW %s SET AUTHORIZATION bob".formatted(viewName)))
                .hasMessageContaining("Access Denied: Cannot set authorization for view default.%s to USER bob", viewName);
        assertQueryFailure(() -> bobExecutor.executeQuery("DROP VIEW %s".formatted(viewName)))
                .hasMessageContaining("Access Denied: Cannot drop view default.%s", viewName);

        hdfsExecutor.executeQuery("SET SESSION legacy_catalog_roles=true");
        hdfsExecutor.executeQuery("SET ROLE ADMIN");
        hdfsExecutor.executeQuery("ALTER VIEW %s SET AUTHORIZATION bob".formatted(viewName));
        bobExecutor.executeQuery("DROP VIEW %s".formatted(viewName));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSetHiveViewAuthorization()
    {
        onHive().executeQuery("CREATE TABLE test_hive_table (col1 int)");
        onHive().executeQuery("CREATE VIEW test_hive_view AS SELECT * FROM test_hive_table");

        assertQueryFailure(() -> bobExecutor.executeQuery("ALTER VIEW test_hive_view SET AUTHORIZATION bob"))
                .hasMessageContaining("Access Denied: Cannot set authorization for view default.test_hive_view to USER bob");
        assertQueryFailure(() -> bobExecutor.executeQuery("DROP VIEW test_hive_view"))
                .hasMessageContaining("Access Denied: Cannot drop view default.test_hive_view");

        hdfsExecutor.executeQuery("SET SESSION legacy_catalog_roles=true");
        hdfsExecutor.executeQuery("SET ROLE ADMIN");
        hdfsExecutor.executeQuery("ALTER VIEW test_hive_view SET AUTHORIZATION bob");
        bobExecutor.executeQuery("DROP VIEW test_hive_view");

        onHive().executeQuery("DROP TABLE test_hive_table");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlShowColumns()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("SHOW COLUMNS FROM %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot show columns of table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SHOW COLUMNS FROM %s".formatted(tableName))).hasRowsCount(2);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlShowStatsFor()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery("SHOW STATS FOR %s".formatted(tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SHOW STATS FOR %s".formatted(tableName))).hasRowsCount(3);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlFilterColumns()
    {
        assertThat(bobExecutor.executeQuery("SELECT * FROM information_schema.columns WHERE table_name = '%s'".formatted(tableName))).hasNoRows();

        aliceExecutor.executeQuery("GRANT SELECT ON %s TO bob".formatted(tableName));
        assertThat(bobExecutor.executeQuery("SELECT * FROM information_schema.columns WHERE table_name = '%s'".formatted(tableName))).hasRowsCount(2);
    }
}
