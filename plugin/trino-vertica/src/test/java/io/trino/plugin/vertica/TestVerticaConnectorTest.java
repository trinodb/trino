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

package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.vertica.VerticaQueryRunner.createVerticaQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVerticaConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingVerticaServer verticaServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verticaServer = closeAfterClass(new TestingVerticaServer());
        return createVerticaQueryRunner(
                verticaServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomTableSuffix();
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertUpdate("DROP TABLE " + tableName);

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT orderkey, discount FROM lineitem", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

        // Trino creates a table of varchar(1), This unicode occupies 3 chars in Vertica
        /*
        assertCreateTableAsSelect(
                "SELECT '\u2603' unicode",
                "SELECT 1");
         */

        assertCreateTableAsSelect(
                "SELECT * FROM orders WITH DATA",
                "SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer
        assertCreateTableAsSelect(
                "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from " + tableName, "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                this::execute,
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        // shippriority column's type is BIGINT, not INTEGER
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        // shippriority column's type is BIGINT, not INTEGER
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE vertica.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority bigint,\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        // shippriority column's type is BIGINT, not INTEGER
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        assertEquals(actual, expected);
    }

    @Test
    @Override
    public void testDropColumn()
    {
        assertThatThrownBy(super::testDropColumn).hasMessage("This connector does not support dropping columns");
        throw new SkipException("This connector does not support dropping columns");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("varbinary")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("real")
                || typeName.equals("double")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    private void execute(String sql)
    {
        verticaServer.execute(sql);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return verticaServer::execute;
    }
}
