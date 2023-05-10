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
package io.trino.tests.product.cache;

import com.google.common.io.Resources;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.TpcTestUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.tests.product.TestGroups.HIVE_CACHE_SUBQUERIES;
import static io.trino.tests.product.TpcTestUtils.assertResults;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveCacheSubqueries
        extends ProductTest
{
    private volatile boolean initialized;

    @BeforeMethodWithContext
    public void init()
    {
        if (initialized) {
            return;
        }
        // uncomment if enable testCachingAgainstTpchDataset or testCachingAgainstTpcdsDataset tests
        //createTpcdsAndTpchDatasets("hive");
        onTrino().executeQuery("create schema if not exists hive.tpch");
        onTrino().executeQuery("create table if not exists hive.tpch.orders_part with (partitioned_by = ARRAY['o_orderpriority']) as select o_orderkey, o_orderpriority from tpch.sf1.orders");
        initialized = true;
    }

    @Test(groups = HIVE_CACHE_SUBQUERIES)
    public void testCacheIsActuallyEnabled()
    {
        onTrino().executeQuery("set session cache_common_subqueries_enabled=true");
        QueryResult result = onTrino().executeQuery(
                """
                        explain analyze
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                        union all
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                    """);
        assertThat((String) result.rows().get(0).get(0)).contains("CacheData");
    }

    @Test(groups = HIVE_CACHE_SUBQUERIES)
    public void testQueryResulIsValidWhenCacheSubqueriesIsEnabled()
    {
        onTrino().executeQuery("set session cache_common_subqueries_enabled=false");
        QueryResult expectedResult = onTrino().executeQuery(
                """
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                        union all
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                    """);
        onTrino().executeQuery("set session cache_common_subqueries_enabled=true");

        // cache data
        onTrino().executeQuery(
                """
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                        union all
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                    """);
        // use cached data
        QueryResult withCacheEnabledResult = onTrino().executeQuery(
                """
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                        union all
                        select count(o_orderkey) from hive.tpch.orders_part where o_orderpriority = '3-MEDIUM'
                    """);

        assertThat((long) withCacheEnabledResult.row(0).get(0)).isEqualTo((long) expectedResult.row(0).get(0));
    }

    @Test(groups = HIVE_CACHE_SUBQUERIES, dataProvider = "tpchQueries", dataProviderClass = TpcTestUtils.class, enabled = false)
    public void testCachingAgainstTpchDataset(String queryId)
            throws IOException
    {
        String query = Resources.toString(getResource("sql-tests/testcases/hive_tpch/q" + queryId + ".sql"), UTF_8);
        List<String> expected = Resources.readLines(getResource("sql-tests/testcases/hive_tpch/q" + queryId + ".result"), UTF_8)
                .stream()
                .filter(line -> !line.startsWith("--"))
                .collect(toImmutableList());
        onTrino().executeQuery("set session cache_common_subqueries_enabled=true");
        onTrino().executeQuery("use hive.tpch");
        // we run it twice to test "warm" and "cold" cache cases
        assertResults(expected, query);
        assertResults(expected, query);
    }

    @Test(groups = HIVE_CACHE_SUBQUERIES, dataProvider = "tpcdsQueries", dataProviderClass = TpcTestUtils.class, enabled = false)
    public void testCachingAgainstTpcdsDataset(String queryId)
            throws IOException
    {
        String query = Resources.toString(getResource("sql-tests/testcases/tpcds/q" + queryId + ".sql"), UTF_8);
        List<String> expected = Resources.readLines(getResource("sql-tests/testcases/tpcds/q" + queryId + ".result"), UTF_8)
                .stream()
                .filter(line -> !line.startsWith("--"))
                .collect(toImmutableList());
        onTrino().executeQuery("set session cache_common_subqueries_enabled=true");
        onTrino().executeQuery("use hive.tpcds");

        // we run it twice to test "warm" and "cold" cache cases
        assertResults(expected, query);
        assertResults(expected, query);
    }
}
