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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIgniteConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingIgniteServer igniteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.igniteServer = closeAfterClass(new TestingIgniteServer());
        return createIgniteQueryRunner(
                igniteServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            // TODO: SUPPORTS
            case SUPPORTS_DELETE:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ARRAY:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testInsert()
    {
        String tableName = "test_insert_" + randomTableSuffix();
        @Language("SQL") String create = "CREATE TABLE " + tableName + "(phone varchar(15) , custkey bigint , acctbal double ) with (primary_key = ARRAY['custkey'])";
        @Language("SQL") String query = "SELECT phone, custkey, acctbal FROM customer";

        assertUpdate(create);
        assertQuery("SELECT count(*) FROM " + tableName + "", "SELECT 0");

        assertUpdate("INSERT INTO " + tableName + " " + query, "SELECT count(*) FROM customer");

        assertQuery("SELECT * FROM " + tableName + "", query);

        assertQueryFails("INSERT INTO " + tableName + " (custkey) VALUES (null)", "Failed to insert data: Null value is not allowed for column 'CUSTKEY'");

        assertUpdate("INSERT INTO " + tableName + " (custkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO " + tableName + " (custkey, phone) VALUES (-4, '3283-2001-01-01')", 1);
        assertUpdate("INSERT INTO " + tableName + " (custkey, phone) VALUES (-2, '3283-2001-01-02')", 1);
        assertUpdate("INSERT INTO " + tableName + " (phone, custkey) VALUES ('3283-2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO " + tableName + " (acctbal, custkey) VALUES (1234, -2234)", 1);

        assertQuery("SELECT * FROM " + tableName + "", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT '3283-2001-01-01', -4, null"
                + " UNION ALL SELECT '3283-2001-01-02', -2, null"
                + " UNION ALL SELECT '3283-2001-01-03', -3, null"
                + " UNION ALL SELECT null, -2234, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        // max custkey => 1500
        assertUpdate(
                "INSERT INTO " + tableName + " (custkey, phone, acctbal) " +
                        "SELECT custkey + 1600, phone, acctbal FROM customer " +
                        "UNION ALL " +
                        "SELECT 100000 + custkey, phone, acctbal FROM customer",
                "SELECT 2 * count(*) FROM customer");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar) with (primary_key = ARRAY['a'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQueryFails("CREATE TABLE " + tableName + " (a bad_type) WITH (primary_key = ARRAY['a'])", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        tableName = "test_cr_tab_not_exists_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b varchar, c double) WITH (primary_key = ARRAY['b'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (d bigint, e varchar) WITH (primary_key = ARRAY['e'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // Test CREATE TABLE LIKE
        tableName = "test_create_original_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar) WITH (primary_key = ARRAY['a'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        String tableNameLike = "test_create_like_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableNameLike + " (LIKE " + tableName + ", d bigint, e varchar) WITH (primary_key = ARRAY['d'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableNameLike));
        assertTableColumnNames(tableNameLike, "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableNameLike);
        assertFalse(getQueryRunner().tableExists(getSession(), tableNameLike));
    }

    @Test
    public void testAddColumn()
    {
        String tableName = "test_add_column_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x varchar, ignore varchar) with (primary_key = ARRAY['ignore'])");
        assertUpdate("INSERT INTO " + tableName + " (x , ignore) VALUES ('first', 'xx1')", 1);

        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 'second', 'xx2', 'xxx'", 1);
        assertQuery(
                "SELECT x, a FROM " + tableName,
                "VALUES ('first', NULL), ('second', 'xxx')");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b double");
        assertUpdate("INSERT INTO " + tableName + " SELECT 'third', 'xx3', 'yyy', 33.3E0", 1);
        assertQuery(
                "SELECT x, a, b FROM " + tableName,
                "VALUES ('first', NULL, NULL), ('second', 'xxx', NULL), ('third', 'yyy', 33.3)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 'fourth', 'xx4', 'zzz', 55.3E0, 'newColumn'", 1);
        assertQuery(
                "SELECT x, a, b, c FROM " + tableName,
                "VALUES ('first', NULL, NULL, NULL), ('second', 'xxx', NULL, NULL), ('third', 'yyy', 33.3, NULL), ('fourth', 'zzz', 55.3, 'newColumn')");
        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertForDefaultColumn()
    {
        try (TestTable testTable = createTableWithDefaultColumns()) {
            assertUpdate(format("INSERT INTO %s (col_required, col_required2) VALUES (1, 10)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s VALUES (7, null, null, 8, 9)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s (col_required2, col_required) VALUES (12, 13)", testTable.getName()), 1);

            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES (1, null, 43, 42, 10), (2, 3, 4, 5, 6), (7, null, null, 8, 9), (13, null, 43, 42, 12)");
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                igniteServer::execute,
                "public.tbl",
                "(col_required bigint NOT NULL PRIMARY KEY," +
                        "col_nullable bigint," +
                        "col_default bigint DEFAULT 43," +
                        "col_nonnull_default bigint DEFAULT 42," +
                        "col_required2 bigint NOT NULL)");
    }

    @Test
    public void testInsertUnicode()
    {
        String tableName = "test_insert_unicode_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(test varchar, ignore varchar) with (primary_key = ARRAY['test'])");
        assertUpdate("INSERT INTO " + tableName + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
        assertThat(computeActual("SELECT test FROM " + tableName).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + "(test varchar, ignore varchar) with (primary_key = ARRAY['test'])");
        assertUpdate("INSERT INTO " + tableName + "(test) VALUES 'aa', 'bé'", 2);
        assertQuery("SELECT test FROM " + tableName, "VALUES 'aa', 'bé'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test = 'aa'", "VALUES 'aa'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test > 'ba'", "VALUES 'bé'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test < 'ba'", "VALUES 'aa'");
        assertQueryReturnsEmptyResult("SELECT test FROM " + tableName + " WHERE test = 'ba'");
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + "(test varchar, ignore varchar) with (primary_key = ARRAY['test'])");
        assertUpdate("INSERT INTO " + tableName + "(test) VALUES 'a', 'é'", 2);
        assertQuery("SELECT test FROM " + tableName, "VALUES 'a', 'é'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test = 'a'", "VALUES 'a'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test > 'b'", "VALUES 'é'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test < 'b'", "VALUES 'a'");
        assertQueryReturnsEmptyResult("SELECT test FROM " + tableName + " WHERE test = 'b'");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint NOT NULL,\n" +
                        "   custkey bigint NOT NULL,\n" +
                        "   orderstatus varchar(1) NOT NULL,\n" +
                        "   totalprice double NOT NULL,\n" +
                        "   orderdate date NOT NULL,\n" +
                        "   orderpriority varchar(15) NOT NULL,\n" +
                        "   clerk varchar(15) NOT NULL,\n" +
                        "   shippriority integer NOT NULL,\n" +
                        "   comment varchar(79) NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   primary_key = ARRAY['orderkey']\n)");
    }

    @Test
    public void testCaseSensitiveTopNPushdown()
    {
        // topN over varchar/char columns should only be pushed down if the remote systems's sort order matches Trino
        boolean expectTopNPushdown = hasBehavior(SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR);
        PlanMatchPattern topNOverTableScan = node(TopNNode.class, anyTree(node(TableScanNode.class)));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_case_sensitive_topn_pushdown",
                "(a_string varchar(10), a_char char(10), a_bigint bigint) with ( primary_key = ARRAY['a_bigint'])",
                List.of(
                        "'A', 'A', 1",
                        "'B', 'B', 2",
                        "'a', 'a', 3",
                        "'b', 'b', 4"))) {
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string ASC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string DESC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char ASC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char DESC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);

            // multiple sort columns with at-least one case-sensitive column
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_char LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_string DESC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
        }
    }

    private void assertConditionallyOrderedPushedDown(
            Session session,
            @Language("SQL") String query,
            boolean condition,
            PlanMatchPattern otherwiseExpected)
    {
        QueryAssertions.QueryAssert queryAssert = assertThat(query(session, query)).ordered();
        if (condition) {
            queryAssert.isFullyPushedDown();
        }
        else {
            queryAssert.isNotFullyPushedDown(otherwiseExpected);
        }
    }

    @Test
    public void testNullSensitiveTopNPushdown()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_null_sensitive_topn_pushdown",
                "(name varchar(10), a bigint, ignored bigint) with (primary_key = ARRAY['ignored'])",
                List.of(
                        "'small', 42, 1",
                        "'big', 134134, 2",
                        "'negative', -15, 3",
                        "'null', NULL, 4"))) {
            verify(SortOrder.values().length == 4, "The test needs to be updated when new options are added");
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a ASC NULLS FIRST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a ASC NULLS LAST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a DESC NULLS FIRST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a DESC NULLS LAST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
        }
    }
}
