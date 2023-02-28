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

package io.trino.plugin.influxdb;

import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.trino.plugin.influxdb.InfluxQueryRunner.createInfluxQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseInfluxConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingInfluxServer server = closeAfterClass(new TestingInfluxServer());
        return createInfluxQueryRunner(server, REQUIRED_TPCH_TABLES);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA -> false;
            case SUPPORTS_CREATE_TABLE, SUPPORTS_RENAME_TABLE -> false;
            case SUPPORTS_ADD_COLUMN, SUPPORTS_RENAME_COLUMN -> false;
            case SUPPORTS_COMMENT_ON_TABLE, SUPPORTS_COMMENT_ON_COLUMN -> false;
            case SUPPORTS_INSERT, SUPPORTS_UPDATE, SUPPORTS_DELETE -> false;
            case SUPPORTS_ARRAY -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // List columns explicitly, as there's no defined order in InfluxDB, columns will return by natural order
        // to manually order columns, there is an existing issue here. https://github.com/influxdata/influxdb/issues/15957
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo(format("""
                                CREATE TABLE %s.%s.orders (
                                   time timestamp(9),
                                   clerk varchar,
                                   comment varchar,
                                   custkey bigint,
                                   orderdate varchar,
                                   orderkey bigint,
                                   orderpriority varchar,
                                   orderstatus varchar,
                                   shippriority bigint,
                                   totalprice double
                                )""", //"time" is 1st column in influx query result additionally.
                        catalog, schema));
    }

    @Test
    @Override
    public void testShowColumns()
    {
        // List columns explicitly, as there's no defined order in InfluxDB, columns will return by natural order
        // to manually order columns, there is an existing issue here. https://github.com/influxdata/influxdb/issues/15957
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("time", "timestamp(9)", "", "") //"time" is 1st column in influx query result additionally.
                .row("clerk", "varchar", "", "")
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("totalprice", "double", "", "")
                .build();

        assertThat(expected).isEqualTo(actual);
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        // List columns explicitly, as there's no defined order in InfluxDB, columns will return by natural order
        // to manually order columns, there is an existing issue here. https://github.com/influxdata/influxdb/issues/15957
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("time", "timestamp(9)", "", "") //"time" is 1st column in influx query result additionally.
                .row("clerk", "varchar", "", "")
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("totalprice", "double", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertThat(actualColumns).isEqualTo(expectedColumns);
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        // List columns explicitly, as there's no defined order in InfluxDB, columns will return by natural order
        // to manually order columns, there is an existing issue here. https://github.com/influxdata/influxdb/issues/15957
        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'time'), " + //"time" is 1st column in influx query result additionally.
                "('orders', 'clerk'), " +
                "('orders', 'comment')," +
                "('orders', 'custkey'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderkey'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'totalprice')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertThat(query(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'"))
                .skippingTypesCheck()
                .containsAll(ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");

        assertQuery(
                "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1",
                "VALUES " +
                        "('applicable_roles'), " +
                        "('columns'), " +
                        "('enabled_roles'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as there's no defined order in InfluxDB, columns will return by natural order
        // to manually order columns, there is an existing issue here. https://github.com/influxdata/influxdb/issues/15957
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        throw new SkipException("InfluxDB connector does not map 'orderdate' column to date type and INSERT statement");
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        throw new SkipException("InfluxDB connector does not map 'orderdate' column to date type");
    }

    @Test
    @Override
    public void testPredicateReflectedInExplain()
    {
        // The format of the string representation of what gets shown in the table scan is connector-specific.
        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 42",
                "name=nationkey, type=bigint", "[42]");
    }

    @Test
    @Override
    public void testSortItemsReflectedInExplain()
    {
        // The format of the string representation of what gets shown in the table scan is connector-specific.
        assertExplain(
                "EXPLAIN SELECT name FROM nation ORDER BY nationkey DESC NULLS LAST LIMIT 5",
                "TopNPartial\\[count = 5, orderBy = \\[nationkey DESC");
    }
}
