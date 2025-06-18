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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

import static io.trino.plugin.druid.DruidQueryRunner.copyAndIngestTpchData;
import static io.trino.plugin.druid.DruidTpchTables.SELECT_FROM_ORDERS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDruidConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingDruidServer druidServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        druidServer = closeAfterClass(new TestingDruidServer());
        return DruidQueryRunner.builder(druidServer)
                .addConnectorProperty("druid.count-distinct-strategy", "EXACT")
                .setInitialTables(ImmutableList.of(ORDERS, LINE_ITEM, NATION, REGION, PART, CUSTOMER))
                .build();
    }

    @AfterAll
    public void destroy()
    {
        druidServer = null; // closed by closeAfterClass
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_INSERT,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return druidServer::execute;
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice", "double", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).result().matches(getDescribeOrdersResult());
    }

    @Test
    public void testDriverBehaviorForStoredUpperCaseIdentifiers()
            throws SQLException
    {
        DatabaseMetaData metadata = druidServer.getConnection().getMetaData();
        // if this fails we need to revisit the RemoteIdentifierSupplier implementation since the driver has probably been fixed - today it returns true even though identifiers are stored in lowercase
        assertThat(metadata.storesUpperCaseIdentifiers()).isTrue();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE druid.druid.orders (\n" +
                        "   __time timestamp(3) NOT NULL,\n" +
                        "   clerk varchar,\n" +
                        "   comment varchar,\n" +
                        "   custkey bigint NOT NULL,\n" +
                        "   orderdate varchar,\n" +
                        "   orderkey bigint NOT NULL,\n" +
                        "   orderpriority varchar,\n" +
                        "   orderstatus varchar,\n" +
                        "   shippriority bigint NOT NULL,\n" +
                        "   totalprice double NOT NULL\n" +
                        ")");
    }

    @Override
    protected @Language("SQL") String getOrdersTableWithColumns()
    {
        return """
                VALUES
                   ('orders', 'orderkey'),
                   ('orders', 'custkey'),
                   ('orders', 'orderstatus'),
                   ('orders', 'totalprice'),
                   ('orders', 'orderdate'),
                   ('orders', '__time'),
                   ('orders', 'orderpriority'),
                   ('orders', 'clerk'),
                   ('orders', 'shippriority'),
                   ('orders', 'comment')
                """;
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as Druid has an additional __time column
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    /**
     * This test verifies that the filtering we have in place to overcome Druid's limitation of
     * not handling the escaping of search characters like % and _, works correctly.
     * <p>
     * See {@link DruidJdbcClient#getTableHandle(ConnectorSession, SchemaTableName)} and
     * {@link DruidJdbcClient#getColumns(ConnectorSession, SchemaTableName, io.trino.plugin.jdbc.RemoteTableName)}
     */
    @Test
    public void testFilteringForTablesAndColumns()
            throws Exception
    {
        String sql = SELECT_FROM_ORDERS + " LIMIT 10";
        String datasourceA = "some_table";
        MaterializedResult materializedRows = getQueryRunner().execute(sql);
        copyAndIngestTpchData(materializedRows, druidServer, datasourceA);
        String datasourceB = "somextable";
        copyAndIngestTpchData(materializedRows, druidServer, datasourceB);

        // Assert that only columns from datsourceA are returned
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice", "double", "", "")
                .build();
        assertThat(query("DESCRIBE " + datasourceA)).result().matches(expectedColumns);

        // Assert that only columns from datsourceB are returned
        expectedColumns = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk_x", "varchar", "", "") // String columns are reported only as varchar
                .row("comment_x", "varchar", "", "")
                .row("custkey_x", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate_x", "varchar", "", "")
                .row("orderkey_x", "bigint", "", "")
                .row("orderpriority_x", "varchar", "", "")
                .row("orderstatus_x", "varchar", "", "")
                .row("shippriority_x", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice_x", "double", "", "")
                .build();
        assertThat(query("DESCRIBE " + datasourceB)).result().matches(expectedColumns);
    }

    @Test
    public void testLimitPushDown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isFullyPushedDown();

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isNotFullyPushedDown(AggregationNode.class); // TODO https://github.com/trinodb/trino/pull/4313

        // distinct limit can be pushed down even without aggregation pushdown
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with aggregation and filter over numeric column
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        // with aggregation and filter over varchar column
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isFullyPushedDown();

        // with TopN over numeric column
        assertThat(query("SELECT * FROM (SELECT regionkey FROM nation ORDER BY nationkey ASC LIMIT 10) LIMIT 5")).isNotFullyPushedDown(TopNNode.class);
        // with TopN over varchar column
        assertThat(query("SELECT * FROM (SELECT regionkey FROM nation ORDER BY name ASC LIMIT 10) LIMIT 5")).isNotFullyPushedDown(TopNNode.class);

        // with join
        PlanMatchPattern joinOverTableScans = node(JoinNode.class,
                anyTree(node(TableScanNode.class)),
                anyTree(node(TableScanNode.class)));
        assertThat(query(
                joinPushdownEnabled(getSession()),
                "SELECT n.name, r.name " +
                        "FROM nation n " +
                        "LEFT JOIN region r USING (regionkey) " +
                        "LIMIT 30"))
                .isNotFullyPushedDown(joinOverTableScans);
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        abort("Druid connector does not map 'orderdate' column to date type and INSERT statement");
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        abort("Druid connector does not map 'orderdate' column to date type");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageContaining("Error while executing SQL \"CREATE TABLE druid.char_trailing_space");
        abort("Implement test for Druid");
    }

    @Test
    @Override
    public void testNativeQuerySelectFromTestTable()
    {
        abort("cannot create test table for Druid");
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        // override because Druid fails to prepare statement, while other connectors succeed in preparing statement and then fail because of no metadata available
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .failure().hasMessageContaining("Failed to get table handle for prepared query");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        abort("cannot create test table for Druid");
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        // varchar IN without domain compaction
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar)), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("druid", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar)), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar))")
                // Filter node is retained as no constraint is pushed into connector.
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        // bigint equality with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("druid", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE nationkey IN (19, 21)"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar)), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();
    }

    @Test
    public void testPredicatePushdownForTimestampWithSecondsPrecision()
    {
        // timestamp equality
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time = TIMESTAMP '1992-01-04 00:00:00'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isFullyPushedDown();

        // timestamp comparison
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time < TIMESTAMP '1992-01-05'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isFullyPushedDown();

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time <= TIMESTAMP '1992-01-04'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isFullyPushedDown();

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time > TIMESTAMP '1998-11-28'"))
                .matches("VALUES " +
                        "(BIGINT '2', BIGINT '370', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '2', BIGINT '468', CAST('AIR' AS varchar))")
                .isFullyPushedDown();

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time >= TIMESTAMP '1998-11-29 00:00:00'"))
                .matches("VALUES " +
                        "(BIGINT '2', BIGINT '370', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '2', BIGINT '468', CAST('AIR' AS varchar))")
                .isFullyPushedDown();

        // timestamp range
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time BETWEEN TIMESTAMP '1992-01-01' AND TIMESTAMP '1992-01-05'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isFullyPushedDown();

        // varchar IN without domain compaction
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time IN (TIMESTAMP '1992-01-04', TIMESTAMP '1998-11-27 00:00:00.000', TIMESTAMP '1998-11-28')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '1', BIGINT '574', CAST('AIR' AS varchar))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("druid", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time IN (TIMESTAMP '1992-01-04', TIMESTAMP '1998-11-27 00:00:00', TIMESTAMP '1998-11-28')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '1', BIGINT '574', CAST('AIR' AS varchar))")
                // Filter node is retained as no constraint is pushed into connector.
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testPredicatePushdownForTimestampWithMillisPrecision()
    {
        // timestamp equality
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time = TIMESTAMP '1992-01-04 00:00:00.001'"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // timestamp comparison
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time < TIMESTAMP '1992-01-05 00:00:00.001'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time <= TIMESTAMP '1992-01-04 00:00:00.001'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time > TIMESTAMP '1998-11-28 00:00:00.001'"))
                .matches("VALUES " +
                        "(BIGINT '2', BIGINT '370', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '2', BIGINT '468', CAST('AIR' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time >= TIMESTAMP '1998-11-29 00:00:00.001'"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // timestamp range
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time BETWEEN TIMESTAMP '1992-01-01 00:00:00.001' AND TIMESTAMP '1992-01-05'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time BETWEEN TIMESTAMP '1992-01-01' AND TIMESTAMP '1992-01-05 00:00:00.001'"))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        // timestamp IN without domain compaction
        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time IN (TIMESTAMP '1992-01-04', TIMESTAMP '1998-11-27 00:00:00.000', TIMESTAMP '1998-11-28')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '1', BIGINT '574', CAST('AIR' AS varchar))")
                .isFullyPushedDown();

        assertThat(query("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time IN (TIMESTAMP '1992-01-04', TIMESTAMP '1998-11-27', TIMESTAMP '1998-11-28 00:00:00.001')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '1', BIGINT '574', CAST('AIR' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        // timestamp IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("druid", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time IN (TIMESTAMP '1992-01-04', TIMESTAMP '1998-11-27 00:00:00.000', TIMESTAMP '1998-11-28')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '1', BIGINT '574', CAST('AIR' AS varchar))")
                // Filter node is retained as no constraint is pushed into connector.
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testPredicatePushdownForTimestampWithHigherPrecision()
    {
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.1234");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.12345");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.123456");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.1234567");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.12345678");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.123456789");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.1234567891");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.12345678912");
        testPredicatePushdownForTimestampWithHigherPrecision("1992-01-04 00:00:00.123456789123");
    }

    private void testPredicatePushdownForTimestampWithHigherPrecision(String timestamp)
    {
        // timestamp equality
        assertThat(query(format("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time = TIMESTAMP '%s'", timestamp)))
                .returnsEmptyResult()
                .matches(output(
                        values("linenumber", "partkey", "shipmode")));

        // timestamp comparison
        assertThat(query(format("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time < TIMESTAMP '%s'", timestamp)))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query(format("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time <= TIMESTAMP '%s'", timestamp)))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query(format("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time > (TIMESTAMP '%s' + INTERVAL '2520' DAY)", timestamp)))
                .matches("VALUES " +
                        "(BIGINT '2', BIGINT '370', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '2', BIGINT '468', CAST('AIR' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query(format("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time >= (TIMESTAMP '%s' + INTERVAL '2521' DAY)", timestamp)))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // timestamp range
        assertThat(query(format("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time BETWEEN TIMESTAMP '1992-01-04' AND TIMESTAMP '%s'", timestamp)))
                .matches("VALUES (BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar IN without domain compaction
        assertThat(query(format("SELECT linenumber, partkey, shipmode FROM lineitem WHERE __time IN (TIMESTAMP '1992-01-04', TIMESTAMP '1998-11-27', TIMESTAMP '%s')", timestamp)))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '1673', CAST('RAIL' AS varchar)), " +
                        "(BIGINT '1', BIGINT '574', CAST('AIR' AS varchar))")
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    @Override // Override because Druid doesn't support DDL
    public void testExecuteProcedure()
    {
        assertThatThrownBy(super::testExecuteProcedure)
                .hasMessageContaining("This connector does not support creating tables");
    }

    @Test
    @Override // Override because Druid doesn't support DDL
    public void testExecuteProcedureWithNamedArgument()
    {
        assertThatThrownBy(super::testExecuteProcedureWithNamedArgument)
                .hasMessageContaining("This connector does not support creating tables");
    }

    @Test
    @Override // Override because Druid allows SELECT query in update procedure
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertUpdate("CALL system.execute('SELECT 1')");
        assertQueryFails("CALL system.execute('invalid')", ".*Non-query expression encountered in illegal context.*");
    }

    @Test
    @Override
    public void testAggregationPushdown()
    {
        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(1) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isFullyPushedDown();
        // GROUP BY above WHERE and LIMIT
        assertThat(query("SELECT regionkey, sum(nationkey) FROM (SELECT * FROM nation WHERE regionkey < 2 LIMIT 11) GROUP BY regionkey")).isFullyPushedDown();
        // GROUP BY above TopN - TopN pushdown not yet supported
        assertThat(query("SELECT custkey, sum(totalprice) FROM (SELECT custkey, totalprice FROM orders ORDER BY orderdate ASC, totalprice ASC LIMIT 10) GROUP BY custkey")).isNotFullyPushedDown(project(node(TopNNode.class, anyTree(node(TableScanNode.class)))));
        // GROUP BY with WHERE on neither grouping nor aggregation column
        assertThat(query("SELECT nationkey, min(regionkey) FROM nation WHERE name = 'ARGENTINA' GROUP BY nationkey")).isFullyPushedDown();
        // aggregation on varchar column
        assertThat(query("SELECT count(name) FROM nation")).isFullyPushedDown();
        // aggregation on varchar column with GROUPING
        assertThat(query("SELECT nationkey, count(name) FROM nation GROUP BY nationkey")).isFullyPushedDown();
        // aggregation on varchar column with WHERE
        assertThat(query("SELECT count(name) FROM nation WHERE name = 'ARGENTINA'")).isFullyPushedDown();

        // pruned away aggregation
        assertThat(query("SELECT -13 FROM (SELECT count(*) FROM nation)"))
                .matches("VALUES -13")
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)));
        // aggregation over aggregation
        assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation)"))
                .matches("VALUES BIGINT '1'")
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)));
        assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation GROUP BY regionkey)"))
                .matches("VALUES BIGINT '5'")
                .isFullyPushedDown();

        // aggregation with UNION ALL and aggregation
        assertThat(query("SELECT count(*) FROM (SELECT name FROM nation UNION ALL SELECT name FROM region)"))
                .matches("VALUES BIGINT '30'")
                // TODO (https://github.com/trinodb/trino/issues/12547): support count(*) over UNION ALL pushdown
                .isNotFullyPushedDown(
                        node(ExchangeNode.class,
                                node(AggregationNode.class, node(TableScanNode.class)),
                                node(AggregationNode.class, node(TableScanNode.class))));

        // aggregation with UNION ALL and aggregation
        assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation UNION ALL SELECT count(*) FROM region)"))
                .matches("VALUES BIGINT '2'")
                .hasPlan(
                        // Note: engine could fold this to single ValuesNode
                        node(OutputNode.class,
                                node(AggregationNode.class,
                                        node(ExchangeNode.class,
                                                node(ExchangeNode.class,
                                                        node(AggregationNode.class, node(ValuesNode.class)),
                                                        node(AggregationNode.class, node(ValuesNode.class)))))));

        // test aggregation on empty results
        assertThat(query("SELECT count(*), count(nationkey), sum(nationkey), min(nationkey), max(nationkey), avg(nationkey) FROM nation WHERE name = 'ATLANTIS'"))
                .isFullyPushedDown()
                .matches("VALUES (BIGINT '0', BIGINT '0', cast(NULL AS BIGINT), cast(NULL AS BIGINT), cast(NULL AS BIGINT), cast(NULL AS DOUBLE))");
    }

    @Test
    @Override
    public void testDistinctAggregationPushdown()
    {
        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT min(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // distinct aggregation
        assertThat(query("SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query("SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query("SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation
        assertThat(query("SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query("SELECT count(DISTINCT regionkey), sum(DISTINCT nationkey) FROM nation")).isFullyPushedDown();
    }

    @Test
    @Override
    public void testCaseSensitiveAggregationPushdown()
    {
        String caseSensitiveTable = "case_sensitive";
        MaterializedResult rows = MaterializedResult.resultBuilder(getSession(), VarcharType.createVarcharType(1), BIGINT, TIMESTAMP_MILLIS)
                .columnNames(List.of("string_col", "bigint_col", "__time"))
                .row("A", 1, 0L)
                .row("a", 2, 0L)
                .build();
        try {
            copyAndIngestTpchData(rows, druidServer, caseSensitiveTable);
        }
        catch (Exception e) {
            fail("Could not complete table load for test", e);
        }

        // verify case sensitivity
        assertThat(query("SELECT min(string_col), max(string_col) FROM case_sensitive"))
                .skippingTypesCheck()
                .matches("VALUES ('A', 'a')");

        // test aggregations are pushed down, and result is case-sensitive
        assertThat(query("SELECT string_col, sum(bigint_col) FROM case_sensitive GROUP BY string_col"))
                .skippingTypesCheck()
                .matches("VALUES ('A', BIGINT '1'), ('a', BIGINT '2')")
                .isFullyPushedDown();
    }

    @Test
    @Override
    public void testNumericAggregationPushdown()
    {
        assertThat(query("SELECT min(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT max(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT sum(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT avg(nationkey) FROM nation")).isFullyPushedDown();

        // test diverse types: bigint, double, and int
        assertThat(query("SELECT min(orderkey), min(totalprice), min(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT max(orderkey), max(totalprice), max(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT sum(orderkey), sum(totalprice), sum(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT avg(orderkey), avg(totalprice), avg(shippriority) FROM orders")).isFullyPushedDown();

        // WHERE on aggregation column
        assertThat(query("SELECT min(orderkey), min(totalprice) FROM orders WHERE orderkey < 10 AND totalprice > 50000")).isFullyPushedDown();
        // WHERE on non-aggregation column
        assertThat(query("SELECT min(orderkey) FROM orders WHERE totalprice > 50000")).isFullyPushedDown();
        // GROUP BY
        assertThat(query("SELECT orderstatus, min(totalprice) FROM orders GROUP BY orderstatus")).isFullyPushedDown();
        // GROUP BY with WHERE on both grouping and aggregation column
        assertThat(query("SELECT orderstatus, min(totalprice) FROM orders WHERE orderstatus = 'F' AND totalprice > 50000 GROUP BY orderstatus")).isFullyPushedDown();
        // GROUP BY with WHERE on grouping column
        assertThat(query("SELECT orderstatus, min(totalprice) FROM orders WHERE orderstatus = 'F' GROUP BY orderstatus")).isFullyPushedDown();
        // GROUP BY with WHERE on aggregation column
        assertThat(query("SELECT orderstatus, min(totalprice) FROM orders WHERE totalprice > 50000 GROUP BY orderstatus")).isFullyPushedDown();
    }
}
