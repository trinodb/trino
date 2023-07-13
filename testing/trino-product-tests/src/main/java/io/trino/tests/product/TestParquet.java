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

import com.google.common.base.CharMatcher;
import com.google.common.io.Resources;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import org.assertj.core.data.Percentage;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.tests.product.TestGroups.PARQUET;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

@SuppressWarnings("UnstableApiUsage")
public class TestParquet
        extends ProductTest
{
    // Do not increase this value without examination and multiple local runs.
    // 1E-10 fails roughly once every 100 - 1000 runs.
    // 1.1E-10 should be orders of magnitude safer.
    private static final Percentage DOUBLE_COMPARISON_ACCURACY = withPercentage(1.1E-10);

    private static final String[] TPCDS_TABLES = {
            "call_center",
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "time_dim",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "web_site"};

    private static final String[] TPCH_TABLES = {
            "nation",
            "region",
            "part",
            "supplier",
            "partsupp",
            "customer",
            "orders",
            "lineitem"};

    private volatile boolean initialized;

    @BeforeMethodWithContext
    public void init()
    {
        if (initialized) {
            return;
        }
        onTrino().executeQuery("CREATE SCHEMA hive.tpch");
        onTrino().executeQuery("CREATE SCHEMA hive.tpcds");

        for (String table : TPCDS_TABLES) {
            onTrino().executeQuery("CREATE TABLE hive.tpcds." + table + " WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1." + table);
        }
        for (String table : TPCH_TABLES) {
            onTrino().executeQuery("CREATE TABLE hive.tpch." + table + " WITH (format='PARQUET') AS SELECT * FROM tpch.sf1." + table);
        }
        initialized = true;
    }

    @Test(groups = {PARQUET, PROFILE_SPECIFIC_TESTS}, dataProvider = "tpcdsQueries")
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

    @Test(groups = {PARQUET, PROFILE_SPECIFIC_TESTS}, dataProvider = "tpchQueries")
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

    /**
     * We use hardcoded results from Trino tpch/tpcds suite stored as plain text.
     * Some minor conversions are needed:
     * Double values may different at last bits due to decimal conversion to text.
     * Big decimal values may have trailing zeros and dots
     */
    private void assertResults(List<String> expected, String query)
    {
        List<List<?>> result = onTrino().executeQuery(query).rows();
        assertThat(result.size()).isEqualTo(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            String expectedRow = expected.get(i);
            String[] expectedValues = expectedRow.split("\\|");
            List<?> resultRow = result.get(i);

            assertThat(expectedValues.length).isEqualTo(resultRow.size());
            for (int j = 0; j < expectedValues.length; j++) {
                String expectedValue = expectedValues[j];
                Object resultValue = resultRow.get(j);
                if (resultValue instanceof Double) {
                    expectedValue = trimIfNeeded(expectedValue);
                    BigDecimal expectedDecimal = new BigDecimal(expectedValue);
                    BigDecimal resultDecimal = BigDecimal.valueOf((Double) resultValue);
                    resultDecimal = resultDecimal.setScale(expectedDecimal.scale(), RoundingMode.HALF_DOWN);

                    assertThat(expectedDecimal).isCloseTo(resultDecimal, DOUBLE_COMPARISON_ACCURACY);
                }
                else if (resultValue instanceof BigDecimal) {
                    assertThat(trimIfNeeded(Objects.toString(resultValue))).isEqualTo(trimIfNeeded(expectedValue));
                }
                else {
                    assertThat(Objects.toString(resultValue)).isEqualTo(expectedValue);
                }
            }
        }
    }

    private String trimIfNeeded(String value)
    {
        // Trim zeros and a trailing dot to match Big Decimals in different formats
        if (value.contains(".")) {
            return CharMatcher.is('.').trimTrailingFrom(
                    CharMatcher.is('0').trimTrailingFrom(value));
        }
        return value;
    }

    @DataProvider
    public static Object[][] tpchQueries()
    {
        return IntStream.range(1, 23)
                .filter(i -> i != 15) // Query creates a view beforehand, not supported here
                .mapToObj(i -> String.format("%02d", i))
                .sorted()
                .map(value -> new Object[] {value})
                .toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] tpcdsQueries()
    {
        return Stream.concat(
                IntStream.range(1, 100)
                        .filter(i -> i != 14) // There are two queries with id 14, 23, 24 and 39
                        .filter(i -> i != 23)
                        .filter(i -> i != 24)
                        .filter(i -> i != 39)
                        .filter(i -> i != 72) // TODO Results for q72 need to be fixed. https://github.com/trinodb/trino/issues/4564
                        .mapToObj(i -> String.format("%02d", i)),
                Stream.of("14_1", "14_2", "23_1", "23_2", "24_2", "39_1", "39_2")) // 24_1 is ignored as it relies on the order of the result
                .sorted()
                .map(value -> new Object[] {value})
                .toArray(Object[][]::new);
    }
}
