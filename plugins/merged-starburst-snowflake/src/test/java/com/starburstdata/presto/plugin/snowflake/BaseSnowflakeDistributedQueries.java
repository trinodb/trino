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
package com.starburstdata.presto.plugin.snowflake;

import io.prestosql.Session;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestQueries;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tests.QueryAssertions.assertEqualsIgnoreOrder;

public class BaseSnowflakeDistributedQueries
        extends AbstractTestQueries
{
    BaseSnowflakeDistributedQueries(QueryRunnerSupplier queryRunnerSupplier)
    {
        super(queryRunnerSupplier);
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(38,0)", "", "")
                .row("custkey", "decimal(38,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(38,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testApproxSetBigint()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testApproxSetBigintGroupBy()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testApproxSetWithNulls()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testCustomAdd()
    {
        // custom_add does not support decimal arguments
    }

    @Override
    public void testCustomSum()
    {
        // custom_sum does not support decimal arguments
    }

    @Test
    public void testDescribeInput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT ? FROM nation WHERE nationkey = ? and name < ?")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "unknown")
                .row(1, "decimal")
                .row(2, "varchar")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(38,0)", 16, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(38,0)", 16, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(152)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(38,0)", 16, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' AND table_schema = 'test_schema' LIMIT 1",
                "SELECT 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(38,0)' AND table_schema = 'test_schema' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer'");
    }

    @Override
    public void testMergeHyperLogLog()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeHyperLogLogGroupBy()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeHyperLogLogWithNulls()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeEmptyNonEmptyApproxSet()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testP4ApproxSetBigint()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetBigintGroupBy()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetGroupByWithNulls()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetWithNulls()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testTableSampleBernoulli()
    {
        throw new SkipException("This test takes more than 10 minutes to finish.");
    }
}
