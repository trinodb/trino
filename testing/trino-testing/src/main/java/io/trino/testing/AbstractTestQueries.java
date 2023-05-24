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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.tpch.TpchTable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.operator.scalar.InvokeFunction.INVOKE_FUNCTION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.StatefulSleepingSum.STATEFUL_SLEEPING_SUM;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestQueries
        extends AbstractTestQueryFramework
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION);

    // We can just use the default type registry, since we don't use any parametric types
    protected static final FunctionBundle CUSTOM_FUNCTIONS = InternalFunctionBundle.builder()
            .aggregates(CustomSum.class)
            .window(CustomRank.class)
            .scalars(CustomAdd.class)
            .scalars(CreateHll.class)
            .functions(APPLY_FUNCTION, INVOKE_FUNCTION, STATEFUL_SLEEPING_SUM)
            .build();

    @Test
    public void testAggregationOverUnknown()
    {
        assertQuery("SELECT clerk, min(totalprice), max(totalprice), min(nullvalue), max(nullvalue) " +
                "FROM (SELECT clerk, totalprice, null AS nullvalue FROM orders) " +
                "GROUP BY clerk");
    }

    @Test
    public void testLimitMax()
    {
        // max int
        assertQuery("SELECT orderkey FROM orders LIMIT " + Integer.MAX_VALUE);
        assertQuery("SELECT orderkey FROM orders ORDER BY orderkey LIMIT " + Integer.MAX_VALUE);

        // max long; a connector may attempt a pushdown while remote system may not accept such high limit values
        assertQuery("SELECT nationkey FROM nation LIMIT " + Long.MAX_VALUE, "SELECT nationkey FROM nation");
        // Currently this is not supported but once it's supported, it should be tested with connectors as well
        assertQueryFails("SELECT nationkey FROM nation ORDER BY nationkey LIMIT " + Long.MAX_VALUE, "ORDER BY LIMIT > 2147483647 is not supported");
    }

    @Test
    public void testComplexQuery()
    {
        assertQueryOrdered(
                "SELECT sum(orderkey), row_number() OVER (ORDER BY orderkey) " +
                        "FROM orders " +
                        "WHERE orderkey <= 10 " +
                        "GROUP BY orderkey " +
                        "HAVING sum(orderkey) >= 3 " +
                        "ORDER BY orderkey DESC " +
                        "LIMIT 3",
                "VALUES (7, 5), (6, 4), (5, 3)");
    }

    @Test
    public void testDistinctMultipleFields()
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders");
    }

    @Test
    public void testArithmeticNegation()
    {
        assertQuery("SELECT -custkey FROM orders");
    }

    @Test
    public void testDistinct()
    {
        assertQuery("SELECT DISTINCT custkey FROM orders");
    }

    @Test
    public void testDistinctHaving()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) AS count " +
                "FROM orders " +
                "GROUP BY orderdate " +
                "HAVING COUNT(DISTINCT clerk) > 1");
    }

    @Test
    public void testDistinctLimit()
    {
        assertQuery("" +
                "SELECT DISTINCT orderstatus, custkey " +
                "FROM (SELECT orderstatus, custkey FROM orders ORDER BY orderkey LIMIT 10) " +
                "LIMIT 10");
        assertQuery("SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus, custkey FROM orders LIMIT 10)");
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 2");

        assertQuery("" +
                        "SELECT DISTINCT x " +
                        "FROM (VALUES 1) t(x) JOIN (VALUES 10, 20) u(a) ON t.x < u.a " +
                        "LIMIT 100",
                "SELECT 1");
    }

    @Test
    public void testDistinctWithOrderBy()
    {
        assertQueryOrdered("SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 10");
    }

    @Test
    public void testRepeatedAggregations()
    {
        assertQuery("SELECT SUM(orderkey), SUM(orderkey) FROM orders");
    }

    @Test
    public void testLimit()
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM orders LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);

        actual = computeActual(
                "(SELECT orderkey, custkey FROM orders ORDER BY orderkey) UNION ALL " +
                        "SELECT orderkey, custkey FROM orders WHERE orderstatus = 'F' UNION ALL " +
                        "(SELECT orderkey, custkey FROM orders ORDER BY orderkey LIMIT 20) UNION ALL " +
                        "(SELECT orderkey, custkey FROM orders LIMIT 5) UNION ALL " +
                        "SELECT orderkey, custkey FROM orders LIMIT 10");
        all = computeExpected("SELECT orderkey, custkey FROM orders", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);

        // with ORDER BY
        assertQuery("SELECT name FROM nation ORDER BY nationkey LIMIT 3");
        assertQuery("SELECT name FROM nation ORDER BY regionkey LIMIT 5"); // query is deterministic because first peer group in regionkey order has 5 rows

        // global aggregation, LIMIT should be removed (and connector should not prevent this from happening)
        assertQuery("SELECT max(regionkey) FROM nation LIMIT 5");

        // with aggregation
        assertQuery("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5");

        // with DISTINCT (can be expressed as DistinctLimitNode and handled differently)
        assertQuery("SELECT DISTINCT regionkey FROM nation LIMIT 5");

        // with filter and aggregation
        assertQuery("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3");
    }

    @Test
    public void testLimitWithAggregation()
    {
        MaterializedResult actual = computeActual("SELECT custkey, SUM(orderkey) FROM orders GROUP BY custkey LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(orderkey) FROM orders GROUP BY custkey", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testLimitInInlineView()
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM orders LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testCountAll()
    {
        assertQuery("SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(42) FROM orders", "SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(42 + 42) FROM orders", "SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(null) FROM orders", "SELECT 0");
    }

    @Test
    public void testCountColumn()
    {
        assertQuery("SELECT COUNT(orderkey) FROM orders");
        assertQuery("SELECT COUNT(orderstatus) FROM orders");
        assertQuery("SELECT COUNT(orderdate) FROM orders");
        assertQuery("SELECT COUNT(1) FROM orders");

        assertQuery("SELECT COUNT(NULLIF(orderstatus, 'F')) FROM orders");
        assertQuery("SELECT COUNT(NULL) FROM orders", "VALUES 0");
        assertQuery("SELECT COUNT(CAST(NULL AS BIGINT)) FROM orders", "VALUES 0");
    }

    @Test
    public void testSelectWithComparison()
    {
        assertQuery("SELECT orderkey FROM orders WHERE totalprice < custkey");
    }

    @Test
    public void testIn()
    {
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)", "SELECT orderkey FROM orders LIMIT 0"); // H2 incorrectly matches rows
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2E0, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE totalprice IN (1, 2, 3)");
    }

    @Test(dataProvider = "largeInValuesCount")
    public void testLargeIn(int valuesCount)
    {
        String longValues = range(0, valuesCount)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");
    }

    @DataProvider
    public Object[][] largeInValuesCount()
    {
        return largeInValuesCountData();
    }

    protected Object[][] largeInValuesCountData()
    {
        return new Object[][] {
                {200},
                {500},
                {1000},
                {5000}
        };
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasFrom()
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS FROM %s", getSession().getCatalog().get()));
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasLike()
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS LIKE '%s'", getSession().getSchema().get()));
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(getSession().getSchema().get()));
    }

    @Test
    public void testShowSchemasLikeWithEscape()
    {
        assertQueryFails("SHOW SCHEMAS LIKE 't$_%' ESCAPE ''", "Escape string must be a single character");
        assertQueryFails("SHOW SCHEMAS LIKE 't$_%' ESCAPE '$$'", "Escape string must be a single character");

        // Using eventual assertion because set of schemas may change concurrently.
        assertEventually(() -> {
            Set<Object> allSchemas = computeActual("SHOW SCHEMAS").getOnlyColumnAsSet();
            assertEquals(allSchemas, computeActual("SHOW SCHEMAS LIKE '%_%'").getOnlyColumnAsSet());
            Set<Object> result = computeActual("SHOW SCHEMAS LIKE '%$_%' ESCAPE '$'").getOnlyColumnAsSet();
            verify(allSchemas.stream().anyMatch(schema -> !((String) schema).contains("_")),
                    "This test expects at least one schema without underscore in it's name. Satisfy this assumption or override the test.");
            assertThat(result)
                    .isSubsetOf(allSchemas)
                    .isNotEqualTo(allSchemas);
            assertThat(result).contains("information_schema").allMatch(schemaName -> ((String) schemaName).contains("_"));
        });
    }

    @Test
    public void testShowTables()
    {
        Set<String> expectedTables = REQUIRED_TPCH_TABLES.stream()
                .map(TpchTable::getTableName)
                .collect(toImmutableSet());

        MaterializedResult result = computeActual("SHOW TABLES");
        assertThat(result.getOnlyColumnAsSet()).containsAll(expectedTables);
    }

    @Test
    public void testShowTablesLike()
    {
        assertThat(computeActual("SHOW TABLES LIKE 'or%'").getOnlyColumnAsSet())
                .contains("orders")
                .allMatch(tableName -> ((String) tableName).startsWith("or"));
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertTrue(actual.equals(expectedParametrizedVarchar) || actual.equals(expectedUnparametrizedVarchar),
                format("%s does not match neither of %s and %s", actual, expectedParametrizedVarchar, expectedUnparametrizedVarchar));
    }

    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'bigint' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    @Test
    public void testInformationSchemaUppercaseName()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'LOCAL'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TINY'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'",
                "SELECT '' WHERE false");
    }

    @Test
    public void testTopN()
    {
        assertQueryOrdered("SELECT n.name, r.name FROM nation n LEFT JOIN region r ON n.regionkey = r.regionkey ORDER BY n.name LIMIT 1");

        assertQueryOrdered("SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10");
        assertQueryOrdered("SELECT orderkey FROM orders ORDER BY orderkey DESC LIMIT 10");

        // multiple sort columns with different sort orders
        assertQueryOrdered("SELECT orderpriority, totalprice FROM orders ORDER BY orderpriority DESC, totalprice ASC LIMIT 10");

        // TopN with Filter
        assertQueryOrdered("SELECT orderkey FROM orders WHERE orderkey > 10 ORDER BY orderkey DESC LIMIT 10");

        // TopN over aggregation column
        assertQueryOrdered("SELECT sum(totalprice), clerk FROM orders GROUP BY clerk ORDER BY sum(totalprice) LIMIT 10");

        // TopN over TopN
        assertQueryOrdered("SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders ORDER BY 1, 2 LIMIT 10) ORDER BY 2, 1 LIMIT 5");

        // TopN over complex query
        assertQueryOrdered(
                "SELECT totalprice_sum, clerk " +
                        "FROM (SELECT SUM(totalprice) as totalprice_sum, clerk FROM orders WHERE orderpriority='1-URGENT' GROUP BY clerk ORDER BY totalprice_sum DESC LIMIT 10)" +
                        "ORDER BY clerk DESC LIMIT 5");

        // TopN over aggregation with filter
        assertQueryOrdered(
                "SELECT * " +
                        "FROM (SELECT SUM(totalprice) as sum, custkey AS total FROM orders GROUP BY custkey HAVING COUNT(*) > 3) " +
                        "ORDER BY sum DESC LIMIT 10");
    }

    @Test
    public void testTopNByMultipleFields()
    {
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey ASC, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey ASC, custkey DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey DESC, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey DESC, custkey DESC LIMIT 10");

        // now try with order by fields swapped
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey ASC, orderkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey ASC, orderkey DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey DESC, orderkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey DESC, orderkey DESC LIMIT 10");

        // nulls first
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS FIRST, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS FIRST, custkey ASC LIMIT 10");

        // nulls last
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS LAST, custkey ASC LIMIT 10");

        // assure that default is nulls last
        assertQueryOrdered(
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC, custkey ASC LIMIT 10",
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC LIMIT 10");
    }

    @Test
    public void testPredicate()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey+1 AS a FROM orders WHERE orderstatus = 'F' UNION ALL \n" +
                "  SELECT orderkey FROM orders WHERE orderkey % 2 = 0 UNION ALL \n" +
                "  (SELECT orderkey+custkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                ") \n" +
                "WHERE a < 20 OR a > 100 \n" +
                "ORDER BY a");
    }

    @Test
    public void testTableSampleBernoulliBoundaryValues()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (0)");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", fullSample.getTypes());

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testTableSampleBernoulli()
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        int total = computeExpected("SELECT orderkey FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().size();

        for (int i = 0; i < 100; i++) {
            List<MaterializedRow> values = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (50)").getMaterializedRows();

            assertEquals(values.size(), ImmutableSet.copyOf(values).size(), "TABLESAMPLE produced duplicate rows");
            stats.addValue(values.size() * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.45 && mean < 0.55, format("Expected mean sampling rate to be ~0.5, but was %s", mean));
    }

    @Test
    public void testFilterPushdownWithAggregation()
    {
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders) WHERE 0=1");
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders) WHERE null");
    }

    @Test
    public void testUnionAllAboveBroadcastJoin()
    {
        assertQuery("SELECT COUNT(*) FROM region r JOIN (SELECT nationkey FROM nation UNION ALL SELECT nationkey as key FROM nation) n ON r.regionkey = n.nationkey", "VALUES 10");
    }
}
