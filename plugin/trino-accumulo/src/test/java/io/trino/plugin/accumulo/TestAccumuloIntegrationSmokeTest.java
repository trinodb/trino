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
package io.trino.plugin.accumulo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestIntegrationSmokeTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAccumuloIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createAccumuloQueryRunner(ImmutableMap.of());
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "Accumulo row ID")
                .row("custkey", "bigint", "", "Accumulo column custkey:custkey. Indexed: false")
                .row("orderstatus", "varchar(1)", "", "Accumulo column orderstatus:orderstatus. Indexed: false")
                .row("totalprice", "double", "", "Accumulo column totalprice:totalprice. Indexed: false")
                .row("orderdate", "date", "", "Accumulo column orderdate:orderdate. Indexed: true")
                .row("orderpriority", "varchar(15)", "", "Accumulo column orderpriority:orderpriority. Indexed: false")
                .row("clerk", "varchar(15)", "", "Accumulo column clerk:clerk. Indexed: false")
                .row("shippriority", "integer", "", "Accumulo column shippriority:shippriority. Indexed: false")
                .row("comment", "varchar(79)", "", "Accumulo column comment:comment. Indexed: false")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE accumulo.tpch.orders (\n" +
                        "   orderkey bigint COMMENT 'Accumulo row ID',\n" +
                        "   custkey bigint COMMENT 'Accumulo column custkey:custkey. Indexed: false',\n" +
                        "   orderstatus varchar(1) COMMENT 'Accumulo column orderstatus:orderstatus. Indexed: false',\n" +
                        "   totalprice double COMMENT 'Accumulo column totalprice:totalprice. Indexed: false',\n" +
                        "   orderdate date COMMENT 'Accumulo column orderdate:orderdate. Indexed: true',\n" +
                        "   orderpriority varchar(15) COMMENT 'Accumulo column orderpriority:orderpriority. Indexed: false',\n" +
                        "   clerk varchar(15) COMMENT 'Accumulo column clerk:clerk. Indexed: false',\n" +
                        "   shippriority integer COMMENT 'Accumulo column shippriority:shippriority. Indexed: false',\n" +
                        "   comment varchar(79) COMMENT 'Accumulo column comment:comment. Indexed: false'\n" +
                        ")");
    }

    @Override
    public void testShowColumns()
    {
        // Override base class because table descriptions for Accumulo connector include comments
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        assertEquals(actual.getMaterializedRows().get(0).getField(0), "orderkey");
        assertEquals(actual.getMaterializedRows().get(0).getField(1), "bigint");
        assertEquals(actual.getMaterializedRows().get(1).getField(0), "custkey");
        assertEquals(actual.getMaterializedRows().get(1).getField(1), "bigint");
        assertEquals(actual.getMaterializedRows().get(2).getField(0), "orderstatus");
        assertEquals(actual.getMaterializedRows().get(2).getField(1), "varchar(1)");
        assertEquals(actual.getMaterializedRows().get(3).getField(0), "totalprice");
        assertEquals(actual.getMaterializedRows().get(3).getField(1), "double");
        assertEquals(actual.getMaterializedRows().get(4).getField(0), "orderdate");
        assertEquals(actual.getMaterializedRows().get(4).getField(1), "date");
        assertEquals(actual.getMaterializedRows().get(5).getField(0), "orderpriority");
        assertEquals(actual.getMaterializedRows().get(5).getField(1), "varchar(15)");
        assertEquals(actual.getMaterializedRows().get(6).getField(0), "clerk");
        assertEquals(actual.getMaterializedRows().get(6).getField(1), "varchar(15)");
        assertEquals(actual.getMaterializedRows().get(7).getField(0), "shippriority");
        assertEquals(actual.getMaterializedRows().get(7).getField(1), "integer");
        assertEquals(actual.getMaterializedRows().get(8).getField(0), "comment");
        assertEquals(actual.getMaterializedRows().get(8).getField(1), "varchar(79)");
    }

    @Override
    public void testScalarSubquery()
    {
        // Override because of extra UUID column in lineitem table, cannot SELECT *

        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // aggregation
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE orderkey = \n"
                + "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE orderkey = \n"
                + "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE \n"
                + "(SELECT orderkey FROM orders WHERE 0=1) "
                + "is null");
        assertQuery("SELECT "
                + "orderkey, partkey, suppkey, linenumber, quantity, "
                + "extendedprice, discount, tax, returnflag, linestatus, "
                + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                + "FROM lineitem WHERE \n"
                + "(SELECT orderkey FROM orders WHERE 0=1) "
                + "is not null");

        // subquery results and an in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(SELECT min(orderkey) FROM orders)" +
                "<" +
                "(SELECT max(orderkey) FROM orders)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE orderkey BETWEEN" +
                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                "   AND" +
                "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        for (String joinType : ImmutableList.of("INNER", "LEFT OUTER")) {
            assertQuery("SELECT l.orderkey, COUNT(*) " +
                    "FROM lineitem l " + joinType + " JOIN orders o ON l.orderkey = o.orderkey " +
                    "WHERE l.orderkey BETWEEN" +
                    "   (SELECT avg(orderkey) FROM orders) - 10 " +
                    "   AND" +
                    "   (SELECT avg(orderkey) FROM orders) + 10 " +
                    "GROUP BY l.orderkey");
        }

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = "Scalar sub-query has returned multiple rows";
        assertQueryFails("SELECT "
                        + "orderkey, partkey, suppkey, linenumber, quantity, "
                        + "extendedprice, discount, tax, returnflag, linestatus, "
                        + "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
                        + "FROM lineitem WHERE orderkey = (\n"
                        + "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");
    }
}
