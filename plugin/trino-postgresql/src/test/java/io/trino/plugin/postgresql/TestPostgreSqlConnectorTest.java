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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.trino.SystemSessionProperties.USE_MARK_DISTINCT;
import static io.trino.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPostgreSqlConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = new TestingPostgreSqlServer();
        closeAfterClass(() -> {
            postgreSqlServer.close();
            postgreSqlServer = null;
        });
        // TODO: https://github.com/trinodb/trino/issues/7031
        return createPostgreSqlQueryRunner(postgreSqlServer, Map.of(), Map.of("topn-pushdown.enabled", "true"), REQUIRED_TPCH_TABLES);
    }

    @BeforeClass
    public void setExtensions()
            throws SQLException
    {
        execute("CREATE EXTENSION file_fdw");
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        // Arrays are supported conditionally. Check the defaults.
        return new PostgreSqlConfig().getArrayMapping() != PostgreSqlConfig.ArrayMapping.DISABLED;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl(), postgreSqlServer.getProperties()),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testInsertInPresenceOfNotSupportedColumn()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert_not_supported_column_present(x bigint, y decimal(50,0), z varchar(10))");
        // Check that column y is not supported.
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_insert_not_supported_column_present'", "VALUES 'x', 'z'");
        assertUpdate("INSERT INTO test_insert_not_supported_column_present (x, z) VALUES (123, 'test')", 1);
        assertQuery("SELECT x, z FROM test_insert_not_supported_column_present", "SELECT 123, 'test'");
        assertUpdate("DROP TABLE test_insert_not_supported_column_present");
    }

    @Test
    public void testViews()
            throws Exception
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        execute("CREATE MATERIALIZED VIEW tpch.test_mv as SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        execute("DROP MATERIALIZED VIEW tpch.test_mv");
    }

    @Test
    public void testForeignTable()
            throws Exception
    {
        execute("CREATE SERVER devnull FOREIGN DATA WRAPPER file_fdw");
        execute("CREATE FOREIGN TABLE tpch.test_ft (x bigint) SERVER devnull OPTIONS (filename '/dev/null')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_ft"));
        computeActual("SELECT * FROM test_ft");
        execute("DROP FOREIGN TABLE tpch.test_ft");
        execute("DROP SERVER devnull");
    }

    @Test
    public void testSystemTable()
    {
        assertThat(computeActual("SHOW TABLES FROM pg_catalog").getOnlyColumnAsSet())
                .contains("pg_tables", "pg_views", "pg_type", "pg_index");
        // SYSTEM TABLE
        assertThat(computeActual("SELECT typname FROM pg_catalog.pg_type").getOnlyColumnAsSet())
                .contains("char", "text");
        // SYSTEM VIEW
        assertThat(computeActual("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'tpch'").getOnlyColumn())
                .contains("orders");
    }

    @Test
    public void testTableWithNoSupportedColumns()
            throws Exception
    {
        String unsupportedDataType = "interval";
        String supportedDataType = "varchar(5)";

        try (AutoCloseable ignore1 = withTable("tpch.no_supported_columns", format("(c %s)", unsupportedDataType));
                AutoCloseable ignore2 = withTable("tpch.supported_columns", format("(good %s)", supportedDataType));
                AutoCloseable ignore3 = withTable("tpch.no_columns", "()")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            assertQueryFails("SELECT c FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT * FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");

            assertQueryFails("SELECT c FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT * FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_columns", "Table 'tpch.no_columns' not found");

            assertQueryFails("SELECT c FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT * FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT 'a' FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");

            assertQueryFails("SHOW COLUMNS FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SHOW COLUMNS FROM no_columns", "Table 'tpch.no_columns' not found");

            // Other tables should be visible in SHOW TABLES (the no_supported_columns might be included or might be not) and information_schema.tables
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");
            assertThat(computeActual("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            // Other tables should be introspectable with SHOW COLUMNS and information_schema.columns
            assertQuery("SHOW COLUMNS FROM supported_columns", "VALUES ('good', 'varchar(5)', '', '')");

            // Listing columns in all tables should not fail due to tables with no columns
            computeActual("SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch'");
        }
    }

    @Test
    public void testInsertWithFailureDoesNotLeaveBehindOrphanedTable()
            throws Exception
    {
        String schemaName = format("tmp_schema_%s", UUID.randomUUID().toString().replaceAll("-", ""));
        try (AutoCloseable schema = withSchema(schemaName);
                AutoCloseable table = withTable(format("%s.test_cleanup", schemaName), "(x INTEGER)")) {
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES 'test_cleanup'");

            execute(format("ALTER TABLE %s.test_cleanup ADD CHECK (x > 0)", schemaName));

            assertQueryFails(format("INSERT INTO %s.test_cleanup (x) VALUES (0)", schemaName), "ERROR: new row .* violates check constraint [\\s\\S]*");
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES 'test_cleanup'");
        }
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint equality with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("postgresql", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE nationkey IN (19, 21)"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over TopN result
        assertThat(query("" +
                "SELECT orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderdate DESC, orderkey ASC LIMIT 10)" +
                "WHERE orderdate = DATE '1998-08-01'"))
                .matches("VALUES BIGINT '27588', 22403, 37735")
                .ordered()
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT custkey " +
                "FROM (SELECT SUM(totalprice) as sum, custkey, COUNT(*) as cnt FROM orders GROUP BY custkey order by sum desc limit 10) " +
                "WHERE cnt > 30"))
                .matches("VALUES BIGINT '643', 898")
                .ordered()
                .isFullyPushedDown();

        // predicate over join
        Session joinPushdownEnabled = joinPushdownEnabled(getSession());
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE acctbal > 8000"))
                .isFullyPushedDown();

        // varchar predicate over join
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isFullyPushedDown();
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isNotFullyPushedDown(
                        node(JoinNode.class,
                                anyTree(node(TableScanNode.class)),
                                anyTree(node(TableScanNode.class))));
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (AutoCloseable ignore = withTable("tpch.test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            execute("INSERT INTO tpch.test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

            assertThat(query("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal <= 123456790"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal <= 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal <= 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal = 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal = 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCharPredicatePushdown()
            throws Exception
    {
        try (AutoCloseable ignore = withTable("tpch.test_char_pushdown",
                "(char_1 char(1), char_5 char(5), char_10 char(10))")) {
            execute("INSERT INTO tpch.test_char_pushdown VALUES" +
                    "('0', '0'    , '0'         )," +
                    "('1', '12345', '1234567890')");

            assertThat(query("SELECT * FROM tpch.test_char_pushdown WHERE char_1 = '0' AND char_5 = '0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_char_pushdown WHERE char_5 = CHAR'12345' AND char_10 = '1234567890'"))
                    .matches("VALUES (CHAR'1', CHAR'12345', CHAR'1234567890')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM tpch.test_char_pushdown WHERE char_10 = CHAR'0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCharTrailingSpace()
            throws Exception
    {
        execute("CREATE TABLE tpch.char_trailing_space (x char(10))");
        assertUpdate("INSERT INTO char_trailing_space VALUES ('test')", 1);

        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test'", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test  '", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test        '", "VALUES 'test'");

        assertEquals(getQueryRunner().execute("SELECT * FROM char_trailing_space WHERE x = char ' test'").getRowCount(), 0);

        assertUpdate("DROP TABLE char_trailing_space");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a date,\n" +
                        "   column_b date NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertUpdate(createTableSql);
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_a) VALUES (date '2012-12-31')", "(?s).*null value in column \"column_b\" violates not-null constraint.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_a, column_b) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_b");

        assertUpdate("ALTER TABLE test_insert_not_null ADD COLUMN column_c BIGINT NOT NULL");

        createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a date,\n" +
                        "   column_b date NOT NULL,\n" +
                        "   column_c bigint NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_b) VALUES (date '2012-12-31')", "(?s).*null value in column \"column_c\" violates not-null constraint.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_c");

        assertUpdate("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_insert_not_null (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_insert_not_null",
                "VALUES (NULL, CAST('2012-12-31' AS DATE), 1), (CAST('2013-01-01' AS DATE), CAST('2013-01-02' AS DATE), 2)");

        assertUpdate("DROP TABLE test_insert_not_null");
    }

    @Test
    public void testAggregationPushdown()
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        Session withMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "true")
                .build();

        // distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);

        Session withoutMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "false")
                .build();

        // distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class);
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class);

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isNotFullyPushedDown(FilterNode.class);

        // GROUP BY above WHERE and LIMIT
        assertThat(query("" +
                "SELECT regionkey, sum(nationkey) " +
                "FROM (SELECT * FROM nation WHERE regionkey < 2 LIMIT 11) " +
                "GROUP BY regionkey"))
                .isFullyPushedDown();

        // GROUP BY above TopN
        assertThat(query("" +
                "SELECT clerk, sum(totalprice) " +
                "FROM (SELECT clerk, totalprice FROM orders ORDER BY orderdate ASC, totalprice ASC LIMIT 10) " +
                "GROUP BY clerk"))
                .isFullyPushedDown();

        // GROUP BY with JOIN
        assertThat(query(joinPushdownEnabled(getSession()), "" +
                "SELECT n.regionkey, sum(c.acctbal) acctbals " +
                "FROM nation n " +
                "LEFT JOIN customer c USING (nationkey) " +
                "GROUP BY 1"))
                .isFullyPushedDown();

        // decimals
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(postgreSqlServer::execute,
                schemaName + ".test_aggregation_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))",
                ImmutableList.of("100.000, 100000000.000000000", "123.321, 123456789.987654321"))) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // TODO array_agg returns array, so it could be supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testTopNPushdown()
    {
        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey limit 10"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey DESC limit 10"))
                .ordered()
                .isFullyPushedDown();

        // multiple sort columns with different orders
        assertThat(query("SELECT * FROM orders ORDER BY shippriority DESC, totalprice ASC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation column
        assertThat(query("SELECT sum(totalprice) AS total FROM orders GROUP BY custkey ORDER BY total DESC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // TopN over TopN
        assertThat(query("SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders ORDER BY 1, 2 LIMIT 10) ORDER BY 2, 1 LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT orderkey, totalprice " +
                "FROM (SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders ORDER BY 1, 2 LIMIT 10) " +
                "ORDER BY 2, 1 LIMIT 5) ORDER BY 1, 2 LIMIT 3"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit
        assertThat(query("SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders LIMIT 20) ORDER BY totalprice ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit with filter
        assertThat(query("" +
                "SELECT orderkey, totalprice " +
                "FROM (SELECT orderkey, totalprice FROM orders WHERE orderpriority = '1-URGENT' LIMIT 20) " +
                "ORDER BY totalprice ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation with filter
        assertThat(query("" +
                "SELECT * " +
                "FROM (SELECT SUM(totalprice) as sum, custkey AS total FROM orders GROUP BY custkey HAVING COUNT(*) > 3) " +
                "ORDER BY sum DESC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();
    }

    @Test
    public void testCaseSensitiveTopNPushdown()
    {
        // Create an enum with non-lexicographically sorted entries
        String enumType = "test_enum_" + randomTableSuffix();
        postgreSqlServer.execute("CREATE TYPE " + enumType + " AS ENUM ('A', 'b', 'B', 'a')");
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_case_sensitive_topn_pushdown",
                "(a_string varchar(10), a_char char(10), a_enum " + enumType + ", a_bigint bigint)",
                List.of(
                        "'A', 'A', 'A', 1",
                        "'B', 'B', 'B', 2",
                        "'a', 'a', 'a', 3",
                        "'b', 'b', 'b', 4"))) {
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string ASC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string DESC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char ASC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char DESC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);

            // enum values sort order is defined as the order in which the values were listed when creating the type
            // If we pushdown topn on enums our results will not be ordered according to Trino unless the enum was
            // declared with values in the same order as Trino expects
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_enum ASC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_enum DESC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);

            // multiple sort columns with at-least one case-sensitive column
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_char LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_string DESC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);

            // pushdown should still work for non-character sort column
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint ASC LIMIT 2"))
                    .ordered()
                    .isFullyPushedDown();
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint DESC LIMIT 2"))
                    .ordered()
                    .isFullyPushedDown();
        }
        finally {
            postgreSqlServer.execute("DROP TYPE " + enumType);
        }
    }

    @Test
    public void testStddevAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(postgreSqlServer::execute, schemaName + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            // with non-lowercase name on input
            assertThat(query("SELECT StdDEv(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT StdDEv_SaMP(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            // with delimited, non-lowercase name on input
            assertThat(query("SELECT \"StdDEv\"(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT \"StdDEv_SaMP\"(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            postgreSqlServer.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            postgreSqlServer.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            postgreSqlServer.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(postgreSqlServer::execute, schemaName + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)", ImmutableList.of("1", "2", "4", "5"))) {
            // Test non-whole number results
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testVarianceAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(postgreSqlServer::execute, schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            postgreSqlServer.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            postgreSqlServer.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            postgreSqlServer.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(postgreSqlServer::execute, schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)", ImmutableList.of("1", "2", "3", "4", "5"))) {
            // Test non-whole number results
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testCovarianceAggregationPushdown()
    {
        // empty table
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_covariance_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)")) {
            assertThat(query("SELECT covar_pop(t_double1, t_double2), covar_pop(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double1, t_double2), covar_samp(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // test some values for which the aggregate functions return whole numbers
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_covariance_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("2, 2, 2, 2", "4, 4, 4, 4"))) {
            assertThat(query("SELECT covar_pop(t_double1, t_double2), covar_pop(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double1, t_double2), covar_samp(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // non-whole number results
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_covariance_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("1, 2, 1, 2", "100000000.123456, 4, 100000000.123456, 4", "123456789.987654, 8, 123456789.987654, 8"))) {
            assertThat(query("SELECT covar_pop(t_double1, t_double2), covar_pop(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double1, t_double2), covar_samp(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testCorrAggregationPushdown()
    {
        // empty table
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_corr_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)")) {
            assertThat(query("SELECT corr(t_double1, t_double2), corr(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // test some values for which the aggregate functions return whole numbers
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_corr_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("2, 2, 2, 2", "4, 4, 4, 4"))) {
            assertThat(query("SELECT corr(t_double1, t_double2), corr(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // non-whole number results
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_corr_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("1, 2, 1, 2", "100000000.123456, 4, 100000000.123456, 4", "123456789.987654, 8, 123456789.987654, 8"))) {
            assertThat(query("SELECT corr(t_double1, t_double2), corr(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testRegrAggregationPushdown()
    {
        // empty table
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_regr_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)")) {
            assertThat(query("SELECT regr_intercept(t_double1, t_double2), regr_intercept(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT regr_slope(t_double1, t_double2), regr_slope(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // test some values for which the aggregate functions return whole numbers
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_regr_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("2, 2, 2, 2", "4, 4, 4, 4"))) {
            assertThat(query("SELECT regr_intercept(t_double1, t_double2), regr_intercept(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT regr_slope(t_double1, t_double2), regr_slope(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // non-whole number results
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "tpch.test_regr_pushdown",
                "(t_double1 DOUBLE PRECISION, t_double2 DOUBLE PRECISION, t_real1 REAL, t_real2 REAL)",
                ImmutableList.of("1, 2, 1, 2", "100000000.123456, 4, 100000000.123456, 4", "123456789.987654, 8, 123456789.987654, 8"))) {
            assertThat(query("SELECT regr_intercept(t_double1, t_double2), regr_intercept(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT regr_slope(t_double1, t_double2), regr_slope(t_real1, t_real2) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isNotFullyPushedDown(FilterNode.class);

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isNotFullyPushedDown(FilterNode.class);

        // with TopN over numeric column
        assertThat(query("SELECT * FROM (SELECT regionkey FROM nation ORDER BY nationkey ASC LIMIT 10) LIMIT 5")).isFullyPushedDown();
        // with TopN over varchar column
        assertThat(query("SELECT * FROM (SELECT regionkey FROM nation ORDER BY name ASC LIMIT 10) LIMIT 5")).isNotFullyPushedDown(TopNNode.class);

        // LIMIT with JOIN
        assertThat(query(joinPushdownEnabled(getSession()), "" +
                "SELECT n.name, r.name " +
                "FROM nation n " +
                "LEFT JOIN region r USING (regionkey) " +
                "LIMIT 30"))
                .isFullyPushedDown();
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
            throws SQLException
    {
        execute("SELECT count(*) FROM tpch.orders WHERE " + getLongInClause(0, 500_000));
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
            throws SQLException
    {
        String longInClauses = range(0, 20)
                .mapToObj(value -> getLongInClause(value * 10_000, 10_000))
                .collect(joining(" OR "));
        execute("SELECT count(*) FROM tpch.orders WHERE " + longInClauses);
    }

    /**
     * Regression test for https://github.com/trinodb/trino/issues/5543
     */
    @Test
    public void testTimestampColumnAndTimestampWithTimeZoneConstant()
            throws Exception
    {
        String tableName = "tpch.test_timestamptz_unwrap_cast" + randomTableSuffix();
        try (AutoCloseable ignored = withTable(tableName, "(id integer, ts_col timestamp(6))")) {
            execute("INSERT INTO " + tableName + " (id, ts_col) VALUES " +
                    "(1, timestamp '2020-01-01 01:01:01.000')," +
                    "(2, timestamp '2019-01-01 01:01:01.000')");

            assertThat(query(format("SELECT id FROM %s WHERE ts_col >= TIMESTAMP '2019-01-01 00:00:00 %s'", tableName, getSession().getTimeZoneKey().getId())))
                    .matches("VALUES 1, 2")
                    .isFullyPushedDown();

            assertThat(query(format("SELECT id FROM %s WHERE ts_col >= TIMESTAMP '2019-01-01 00:00:00 %s'", tableName, "UTC")))
                    .matches("VALUES 1")
                    .isFullyPushedDown();
        }
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    private AutoCloseable withSchema(String schema)
            throws Exception
    {
        execute(format("CREATE SCHEMA %s", schema));
        return () -> {
            try {
                execute(format("DROP SCHEMA %s", schema));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
            throws Exception
    {
        execute(format("CREATE TABLE %s%s", tableName, tableDefinition));
        return () -> {
            try {
                execute(format("DROP TABLE %s", tableName));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl(), postgreSqlServer.getProperties());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
