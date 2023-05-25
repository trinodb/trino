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

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.trino.plugin.druid.DruidQueryRunner.copyAndIngestTpchData;
import static io.trino.plugin.druid.DruidTpchTables.SELECT_FROM_ORDERS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public abstract class BaseDruidConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingDruidServer druidServer;

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (druidServer != null) {
            druidServer.close();
            druidServer = null;
        }
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
                return false;
            case SUPPORTS_TRUNCATE:
                return true; // TODO ATM Truncate doesn't work in Druid, but testTruncateTable doesn't fail as CREATE TABLE is not supported

            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
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

    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).matches(getDescribeOrdersResult());
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

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', '__time'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertQuery(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'",
                ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");
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
     * {@link DruidJdbcClient#getColumns(ConnectorSession, JdbcTableHandle)}
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
        assertThat(query("DESCRIBE " + datasourceA)).matches(expectedColumns);

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
        assertThat(query("DESCRIBE " + datasourceB)).matches(expectedColumns);
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
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isNotFullyPushedDown(AggregationNode.class); // global aggregation, LIMIT removed TODO https://github.com/trinodb/trino/pull/4313
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isNotFullyPushedDown(AggregationNode.class); // TODO https://github.com/trinodb/trino/pull/4313

        // distinct limit can be pushed down even without aggregation pushdown
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with aggregation and filter over numeric column
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isNotFullyPushedDown(AggregationNode.class); // TODO https://github.com/trinodb/trino/pull/4313
        // with aggregation and filter over varchar column
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isNotFullyPushedDown(AggregationNode.class); // TODO https://github.com/trinodb/trino/pull/4313

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
        throw new SkipException("Druid connector does not map 'orderdate' column to date type and INSERT statement");
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        throw new SkipException("Druid connector does not map 'orderdate' column to date type");
    }

    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageContaining("Error while executing SQL \"CREATE TABLE druid.char_trailing_space");
        throw new SkipException("Implement test for Druid");
    }

    @Override
    public void testNativeQuerySelectFromTestTable()
    {
        throw new SkipException("cannot create test table for Druid");
    }

    @Override
    public void testNativeQueryCreateStatement()
    {
        // override because Druid fails to prepare statement, while other connectors succeed in preparing statement and then fail because of no metadata available
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .hasMessageContaining("Failed to get table handle for prepared query");
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
    }

    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        throw new SkipException("cannot create test table for Druid");
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

        // Druid doesn't support Aggregation Pushdown
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isNotFullyPushedDown(AggregationNode.class);

        // Druid doesn't support Aggregation Pushdown
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isNotFullyPushedDown(AggregationNode.class);
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

    @Test(dataProvider = "timestampValuesProvider")
    public void testPredicatePushdownForTimestampWithHigherPrecision(String timestamp)
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

    @DataProvider
    public Object[][] timestampValuesProvider()
    {
        return new Object[][] {
                {"1992-01-04 00:00:00.1234"},
                {"1992-01-04 00:00:00.12345"},
                {"1992-01-04 00:00:00.123456"},
                {"1992-01-04 00:00:00.1234567"},
                {"1992-01-04 00:00:00.12345678"},
                {"1992-01-04 00:00:00.123456789"},
                {"1992-01-04 00:00:00.1234567891"},
                {"1992-01-04 00:00:00.12345678912"},
                {"1992-01-04 00:00:00.123456789123"}
        };
    }
}
