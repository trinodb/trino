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

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static java.lang.String.format;

public class TestSqlStandardAccessControlChecks
        extends ProductTest
{
    private String tableName = "alice_owned_table";
    private String viewName = "alice_owned_view";
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;
    private QueryExecutor charlieExecutor;
    private QueryExecutor caseSensitiveUserNameExecutor;

    @BeforeTestWithContext
    public void setup()
    {
        aliceExecutor = connectToTrino("alice@presto");
        bobExecutor = connectToTrino("bob@presto");
        charlieExecutor = connectToTrino("charlie@presto");
        caseSensitiveUserNameExecutor = connectToTrino("CaseSensitiveUserName@presto");

        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        aliceExecutor.executeQuery(format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

        aliceExecutor.executeQuery(format("DROP VIEW IF EXISTS %s", viewName));
        aliceExecutor.executeQuery(format("CREATE VIEW %s AS SELECT month, day FROM %s", viewName, tableName));
    }

    @AfterTestWithContext
    public void cleanup()
    {
        // should not be closed, this would close a shared, global QueryExecutor
        aliceExecutor = null;
        bobExecutor = null;
        charlieExecutor = null;
        caseSensitiveUserNameExecutor = null;
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelect()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();

        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", viewName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelectFromPartitions()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SELECT * FROM \"%s$partitions\"", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s$partitions", tableName);

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM \"%s$partitions\"", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlInsert()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName)))
                .hasMessageContaining("Access Denied: Cannot insert into table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT INSERT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).hasRowsCount(1);
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlUpdate()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("UPDATE %s SET month=3, day=22", tableName)))
                .hasMessageContaining("Access Denied: Cannot update columns [month, day] in table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT INSERT ON %s TO bob", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("UPDATE %s SET month=3, day=22", tableName)))
                .hasMessageContaining("Access Denied: Cannot update columns [month, day] in table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT UPDATE ON %s TO bob", tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("UPDATE %s SET month=3, day=22", tableName)))
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDelete()
    {
        aliceExecutor.executeQuery(format("INSERT INTO %s VALUES (4, 13)", tableName));
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        assertQueryFailure(() -> bobExecutor.executeQuery(format("DELETE FROM %s WHERE day=4", tableName)))
                .hasMessageContaining("Access Denied: Cannot delete from table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT DELETE ON %s TO bob", tableName));
        bobExecutor.executeQuery(format("DELETE FROM %s", tableName));
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateTableAsSelect()
    {
        String createTableAsSelect = "bob_create_table_as_select";

        bobExecutor.executeQuery("DROP TABLE IF EXISTS " + createTableAsSelect);
        assertQueryFailure(() -> bobExecutor.executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        bobExecutor.executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", createTableAsSelect))).hasNoRows();
        bobExecutor.executeQuery("DROP TABLE " + createTableAsSelect);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropTable()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("DROP TABLE %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot drop table default.%s", tableName);

        aliceExecutor.executeQuery(format("DROP TABLE %s", tableName));
        assertQueryFailure(() -> aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("does not exist");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlAlterTable()
    {
        assertThat(aliceExecutor.executeQuery(format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(2);
        assertQueryFailure(() -> bobExecutor.executeQuery(format("ALTER TABLE %s ADD COLUMN year bigint", tableName)))
                .hasMessageContaining("Access Denied: Cannot add a column to table default.%s", tableName);

        aliceExecutor.executeQuery(format("ALTER TABLE %s ADD COLUMN year bigint", tableName));
        assertThat(aliceExecutor.executeQuery(format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(3);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlCreateView()
    {
        String viewName = "bob_view";
        String selectTableSql = format("SELECT * FROM %s", tableName);
        String createViewSql = format("CREATE VIEW %s AS %s", viewName, selectTableSql);

        bobExecutor.executeQuery(format("DROP VIEW IF EXISTS %s", viewName));

        // Bob needs SELECT on the table to create the view
        bobExecutor.executeQuery("DROP VIEW IF EXISTS " + viewName);
        assertQueryFailure(() -> bobExecutor.executeQuery(createViewSql))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        // Give Bob access to table, then create and execute view
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        bobExecutor.executeQuery(createViewSql);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();

        // Verify that Charlie does not have SELECT on the view, then grant access
        assertQueryFailure(() -> charlieExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
        bobExecutor.executeQuery(format("GRANT SELECT ON %s TO charlie", viewName));

        // Charlie still cannot access view because Bob does not have SELECT WITH GRANT OPTION
        assertQueryFailure(() -> charlieExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .hasMessageContaining("Access Denied: View owner does not have sufficient privileges: View owner 'bob' cannot create view that selects from default.%s", tableName);

        // Give Bob SELECT WITH GRANT OPTION on the underlying table
        aliceExecutor.executeQuery(format("REVOKE SELECT ON %s FROM bob", tableName));
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));

        // Bob has GRANT OPTION, so both Bob and Charlie can access the view
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();
        assertThat(charlieExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();

        bobExecutor.executeQuery("DROP VIEW " + viewName);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlDropView()
    {
        String viewName = "alice_view_for_drop";

        aliceExecutor.executeQuery("DROP VIEW IF EXISTS " + viewName);
        aliceExecutor.executeQuery(format("CREATE VIEW %s AS SELECT * FROM %s", viewName, tableName));
        assertQueryFailure(() -> bobExecutor.executeQuery(format("DROP VIEW %s", viewName)))
                .hasMessageContaining("Access Denied: Cannot drop view default.%s", viewName);

        aliceExecutor.executeQuery(format("DROP VIEW %s", viewName));
        assertQueryFailure(() -> aliceExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .hasMessageContaining("does not exist");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSelectWithCaseSensitiveUserName()
    {
        assertQueryFailure(() -> caseSensitiveUserNameExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        onHive().executeQuery("SET ROLE admin");

        // make sure that case matters
        onHive().executeQuery(format("GRANT SELECT ON TABLE %s TO USER casesensitiveusername", tableName));
        assertQueryFailure(() -> caseSensitiveUserNameExecutor.executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        onHive().executeQuery(format("GRANT SELECT ON TABLE %s TO USER CaseSensitiveUserName", tableName));
        assertThat(caseSensitiveUserNameExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();

        assertQueryFailure(() -> caseSensitiveUserNameExecutor.executeQuery(format("SELECT * FROM %s", viewName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
        onHive().executeQuery(format("GRANT SELECT ON TABLE %s TO USER CaseSensitiveUserName", viewName));
        assertThat(caseSensitiveUserNameExecutor.executeQuery(format("SELECT * FROM %s", viewName))).hasNoRows();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSetTableAuthorization()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("ALTER TABLE %s SET AUTHORIZATION bob", tableName)))
                .hasMessageContaining("Access Denied: Cannot set authorization for table default.%s to USER bob", tableName);
        aliceExecutor.executeQuery(format("ALTER TABLE %s SET AUTHORIZATION bob", tableName));
        bobExecutor.executeQuery(format("ALTER TABLE %s SET AUTHORIZATION alice", tableName));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSetViewAuthorization()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("ALTER VIEW %s SET AUTHORIZATION bob", viewName)))
                .hasMessageContaining("Access Denied: Cannot set authorization for view default.%s to USER bob", viewName);
        assertQueryFailure(() -> bobExecutor.executeQuery(format("DROP VIEW %s", viewName)))
                .hasMessageContaining("Access Denied: Cannot drop view default.%s", viewName);

        aliceExecutor.executeQuery(format("ALTER VIEW %s SET AUTHORIZATION bob", viewName));
        bobExecutor.executeQuery(format("DROP VIEW %s", viewName));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlSetHiveViewAuthorization()
    {
        onHive().executeQuery("CREATE TABLE test_hive_table (col1 int)");
        onHive().executeQuery("CREATE VIEW test_hive_view AS SELECT * FROM test_hive_table");

        QueryExecutor hdfsExecutor = connectToTrino("hdfs@presto");

        assertQueryFailure(() -> bobExecutor.executeQuery("ALTER VIEW test_hive_view SET AUTHORIZATION bob"))
                .hasMessageContaining("Access Denied: Cannot set authorization for view default.test_hive_view to USER bob");
        assertQueryFailure(() -> bobExecutor.executeQuery("DROP VIEW test_hive_view"))
                .hasMessageContaining("Access Denied: Cannot drop view default.test_hive_view");

        hdfsExecutor.executeQuery("ALTER VIEW test_hive_view SET AUTHORIZATION bob");
        bobExecutor.executeQuery("DROP VIEW test_hive_view");

        onHive().executeQuery("DROP TABLE test_hive_table");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlShowColumns()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SHOW COLUMNS FROM %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot show columns of table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(2);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlShowStatsFor()
    {
        assertQueryFailure(() -> bobExecutor.executeQuery(format("SHOW STATS FOR %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SHOW STATS FOR %s", tableName))).hasRowsCount(3);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAccessControlFilterColumns()
    {
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM information_schema.columns WHERE table_name = '%s'", tableName))).hasNoRows();

        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM information_schema.columns WHERE table_name = '%s'", tableName))).hasRowsCount(2);
    }
}
