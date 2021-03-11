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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.USE_MARK_DISTINCT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseMySqlConnectorTest
        extends BaseJdbcConnectorTest
{
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
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    protected abstract SqlExecutor getMySqlExecutor();

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                getMySqlExecutor(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(255)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(255)", "", "")
                .row("clerk", "varchar(255)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(255)", "", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return nullToEmpty(exception.getMessage()).matches(".*(Incorrect column name).*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("real")
                || typeName.equals("timestamp")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        if (typeName.equals("boolean")) {
            // MySql does not have built-in support for boolean type. MySQL provides BOOLEAN as the synonym of TINYINT(1)
            // Querying the column with a boolean predicate subsequently fails with "Cannot apply operator: tinyint = boolean"
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(255)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(255)", "", "")
                .row("clerk", "varchar(255)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(255)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE mysql.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(255),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(255),\n" +
                        "   clerk varchar(255),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(255)\n" +
                        ")");
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
    public void testViews()
    {
        getMySqlExecutor().execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        getMySqlExecutor().execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Override
    @Test
    public void testInsert()
    {
        getMySqlExecutor().execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        getMySqlExecutor().execute("CREATE TABLE tpch.test_insert_not_supported_column_present(x bigint, y decimal(50,0), z varchar(10))");
        // Check that column y is not supported.
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_insert_not_supported_column_present'", "VALUES 'x', 'z'");
        assertUpdate("INSERT INTO test_insert_not_supported_column_present (x, z) VALUES (123, 'test')", 1);
        assertQuery("SELECT x, z FROM test_insert_not_supported_column_present", "SELECT 123, 'test'");
        assertUpdate("DROP TABLE test_insert_not_supported_column_present");
    }

    @Test
    public void testNameEscaping()
    {
        Session session = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema(getSession().getSchema().get())
                .build();

        assertFalse(getQueryRunner().tableExists(session, "test_table"));

        assertUpdate(session, "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(session, "test_table"));

        assertQuery(session, "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(session, "DROP TABLE test_table");
        assertFalse(getQueryRunner().tableExists(session, "test_table"));
    }

    @Test
    public void testMySqlTinyint()
    {
        getMySqlExecutor().execute("CREATE TABLE tpch.mysql_test_tinyint1 (c_tinyint tinyint(1))");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM mysql_test_tinyint1");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("c_tinyint", "tinyint", "", "")
                .build();

        assertEquals(actual, expected);

        getMySqlExecutor().execute("INSERT INTO tpch.mysql_test_tinyint1 VALUES (127), (-128)");
        MaterializedResult materializedRows = computeActual("SELECT * FROM tpch.mysql_test_tinyint1 WHERE c_tinyint = 127");
        assertEquals(materializedRows.getRowCount(), 1);
        MaterializedRow row = getOnlyElement(materializedRows);

        assertEquals(row.getFields().size(), 1);
        assertEquals(row.getField(0), (byte) 127);

        assertUpdate("DROP TABLE mysql_test_tinyint1");
    }

    @Test
    public void testCharTrailingSpace()
    {
        getMySqlExecutor().execute("CREATE TABLE tpch.char_trailing_space (x char(10))");
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

        assertQueryFails("INSERT INTO test_insert_not_null (column_a) VALUES (date '2012-12-31')", "Failed to insert data: Field 'column_b' doesn't have a default value");
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

        assertQueryFails("INSERT INTO test_insert_not_null (column_b) VALUES (date '2012-12-31')", "Failed to insert data: Field 'column_c' doesn't have a default value");
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
            throws Exception
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
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

        // decimals
        try (AutoCloseable ignoreTable = withTable("tpch.test_aggregation_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            getMySqlExecutor().execute("INSERT INTO tpch.test_aggregation_pushdown VALUES (100.000, 100000000.000000000)");
            getMySqlExecutor().execute("INSERT INTO tpch.test_aggregation_pushdown VALUES (123.321, 123456789.987654321)");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
        }

        // array_agg returns array, which is not supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testStddevAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(getMySqlExecutor(), schemaName + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            getMySqlExecutor().execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            getMySqlExecutor().execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            getMySqlExecutor().execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(getMySqlExecutor(), schemaName + ".test_stddev_pushdown",
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
        try (TestTable testTable = new TestTable(getMySqlExecutor(), schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            getMySqlExecutor().execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            getMySqlExecutor().execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            getMySqlExecutor().execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(getMySqlExecutor(), schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)", ImmutableList.of("1", "2", "3", "4", "5"))) {
            // Test non-whole number results
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
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
    }

    @Test
    public void testColumnComment()
    {
        // TODO add support for setting comments on existing column and replace the test with io.trino.testing.AbstractTestDistributedQueries#testCommentColumn

        getMySqlExecutor().execute("CREATE TABLE tpch.test_column_comment (col1 bigint COMMENT 'test comment', col2 bigint COMMENT '', col3 bigint)");

        assertQuery(
                "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('col1', 'test comment'), ('col2', null), ('col3', null)");

        assertUpdate("DROP TABLE test_column_comment");
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(255)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        getMySqlExecutor().execute("CREATE TABLE tpch.binary_test (x int, y varbinary(100))");
        getMySqlExecutor().execute("INSERT INTO tpch.binary_test VALUES (3, from_base64('AFCBhLrkidtNTZcA9Ru3hw=='))");

        // varbinary equality
        assertThat(query("SELECT x, y FROM tpch.binary_test WHERE y = from_base64('AFCBhLrkidtNTZcA9Ru3hw==')"))
                .matches("VALUES (3, from_base64('AFCBhLrkidtNTZcA9Ru3hw=='))")
                .isFullyPushedDown();

        getMySqlExecutor().execute("DROP TABLE tpch.binary_test");

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
            throws Exception
    {
        getMySqlExecutor().execute(format("CREATE TABLE %s%s", tableName, tableDefinition));
        return () -> {
            try {
                getMySqlExecutor().execute(format("DROP TABLE %s", tableName));
            }
            catch (RuntimeException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
