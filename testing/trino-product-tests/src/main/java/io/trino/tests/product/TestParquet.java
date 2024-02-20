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
package io.trino.tests.product;

import com.google.common.io.Resources;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.tests.product.TestGroups.PARQUET;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TpcTestUtils.assertResults;
import static io.trino.tests.product.TpcTestUtils.createTpcdsAndTpchDatasets;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("UnstableApiUsage")
public class TestParquet
        extends ProductTest
{
    private volatile boolean initialized;

    @BeforeMethodWithContext
    public void init()
    {
        if (initialized) {
            return;
        }
        createTpcdsAndTpchDatasets("hive");
        initialized = true;
    }

    @Test(groups = {PARQUET, PROFILE_SPECIFIC_TESTS}, dataProvider = "tpcdsQueries", dataProviderClass = TpcTestUtils.class)
    public void testTpcds(String queryId)
            throws IOException
    {
        String query = Resources.toString(getResource("sql-tests/testcases/tpcds/q" + queryId + ".sql"), UTF_8);
        List<String> expected = Resources.readLines(getResource("sql-tests/testcases/tpcds/q" + queryId + ".result"), UTF_8)
                .stream()
                .filter(line -> !line.startsWith("--"))
                .collect(toImmutableList());
        onTrino().executeQuery("USE hive.tpcds");
        assertResults(expected, query);
    }

    @Test(groups = {PARQUET, PROFILE_SPECIFIC_TESTS}, dataProvider = "tpchQueries", dataProviderClass = TpcTestUtils.class)
    public void testTpch(String queryId)
            throws IOException
    {
        String query = Resources.toString(getResource("sql-tests/testcases/hive_tpch/q" + queryId + ".sql"), UTF_8);
        List<String> expected = Resources.readLines(getResource("sql-tests/testcases/hive_tpch/q" + queryId + ".result"), UTF_8)
                .stream()
                .filter(line -> !line.startsWith("--"))
                .collect(toImmutableList());
        onTrino().executeQuery("USE hive.tpch");
        assertResults(expected, query);
    }
}
