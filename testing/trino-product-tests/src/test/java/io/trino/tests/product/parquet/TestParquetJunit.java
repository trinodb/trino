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
package io.trino.tests.product.parquet;

import com.google.common.base.CharMatcher;
import com.google.common.io.Resources;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.trino.tests.product.ConfiguredFeatures.assertDefaultConnectors;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

@ProductTest
@RequiresEnvironment(ParquetEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Parquet
@TestGroup.ProfileSpecificTests
class TestParquetJunit
{
    // Do not increase this value without examination and repeated runs.
    private static final double DOUBLE_COMPARISON_PERCENTAGE = 1.1e-10;

    private static boolean initialized;

    @Test
    void testConfiguredConnectors(ParquetEnvironment environment)
    {
        assertDefaultConnectors(environment, "hive");
    }

    @ParameterizedTest
    @MethodSource("tpcdsQueries")
    void testTpcds(String queryId, ParquetEnvironment env)
            throws IOException
    {
        initializeDatasets(env);
        String query = Resources.toString(getResource("sql-tests/testcases/tpcds/q" + queryId + ".sql"), UTF_8);
        List<String> expected = Resources.readLines(getResource("sql-tests/testcases/tpcds/q" + queryId + ".result"), UTF_8)
                .stream()
                .filter(line -> !line.startsWith("--"))
                .toList();
        env.executeTrinoInSession(session -> {
            session.executeUpdate("USE hive.tpcds");
            assertResults(expected, session.executeQuery(query));
        });
    }

    @ParameterizedTest
    @MethodSource("tpchQueries")
    void testTpch(String queryId, ParquetEnvironment env)
            throws IOException
    {
        initializeDatasets(env);
        String query = Resources.toString(getResource("sql-tests/testcases/hive_tpch/q" + queryId + ".sql"), UTF_8);
        List<String> expected = Resources.readLines(getResource("sql-tests/testcases/hive_tpch/q" + queryId + ".result"), UTF_8)
                .stream()
                .filter(line -> !line.startsWith("--"))
                .toList();
        env.executeTrinoInSession(session -> {
            session.executeUpdate("USE hive.tpch");
            assertResults(expected, session.executeQuery(query));
        });
    }

    private static synchronized void initializeDatasets(ParquetEnvironment env)
    {
        if (initialized) {
            return;
        }

        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.tpch");
        for (String table : tpchTables()) {
            env.executeTrinoUpdate("CREATE TABLE IF NOT EXISTS hive.tpch.%1$s WITH (format='PARQUET') AS SELECT * FROM tpch.sf1.%1$s".formatted(table));
        }

        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.tpcds");
        for (String table : tpcdsTables()) {
            env.executeTrinoUpdate("CREATE TABLE IF NOT EXISTS hive.tpcds." + table + " WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1." + table);
        }

        initialized = true;
    }

    private static void assertResults(List<String> expected, QueryResult actual)
    {
        List<List<Object>> rows = actual.rows();
        assertThat(rows).hasSize(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            String[] expectedValues = expected.get(i).split("\\|");
            List<Object> actualValues = rows.get(i);
            assertThat(actualValues).hasSize(expectedValues.length);

            for (int j = 0; j < expectedValues.length; j++) {
                String expectedValue = expectedValues[j];
                Object actualValue = actualValues.get(j);
                if (actualValue instanceof Double doubleValue) {
                    expectedValue = trimIfNeeded(expectedValue);
                    BigDecimal expectedDecimal = new BigDecimal(expectedValue);
                    BigDecimal actualDecimal = BigDecimal.valueOf(doubleValue)
                            .setScale(expectedDecimal.scale(), RoundingMode.HALF_DOWN);
                    assertThat(expectedDecimal).isCloseTo(actualDecimal, withPercentage(DOUBLE_COMPARISON_PERCENTAGE));
                }
                else if (actualValue instanceof BigDecimal) {
                    assertThat(trimIfNeeded(Objects.toString(actualValue))).isEqualTo(trimIfNeeded(expectedValue));
                }
                else {
                    assertThat(Objects.toString(actualValue)).isEqualTo(expectedValue);
                }
            }
        }
    }

    private static String trimIfNeeded(String value)
    {
        if (value.contains(".")) {
            return CharMatcher.is('.').trimTrailingFrom(CharMatcher.is('0').trimTrailingFrom(value));
        }
        return value;
    }

    private static Stream<String> tpchQueries()
    {
        return IntStream.range(1, 23)
                .filter(i -> i != 15)
                .mapToObj(i -> "%02d".formatted(i))
                .sorted();
    }

    private static Stream<String> tpcdsQueries()
    {
        return Stream.concat(
                        IntStream.range(1, 100)
                                .filter(i -> i != 14)
                                .filter(i -> i != 23)
                                .filter(i -> i != 24)
                                .filter(i -> i != 39)
                                .filter(i -> i != 72)
                                .mapToObj(i -> "%02d".formatted(i)),
                        Stream.of("14_1", "14_2", "23_1", "23_2", "24_2", "39_1", "39_2"))
                .sorted();
    }

    private static List<String> tpchTables()
    {
        return List.of("nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem");
    }

    private static List<String> tpcdsTables()
    {
        return List.of(
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
                "web_site");
    }
}
