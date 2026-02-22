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
    private static final double DOUBLE_COMPARISON_PERCENTAGE = 1.1E-10;

    private static volatile boolean initialized;

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
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.tpch." + table);
            env.executeTrinoUpdate(createTpchTableSql(table));
        }

        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.tpcds");
        for (String table : tpcdsTables()) {
            env.executeTrinoUpdate("CREATE TABLE IF NOT EXISTS hive.tpcds." + table + " WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1." + table);
        }

        initialized = true;
    }

    private static String createTpchTableSql(String table)
    {
        return switch (table) {
            case "nation" -> "CREATE TABLE hive.tpch.nation WITH (format='PARQUET') AS SELECT nationkey AS n_nationkey, name AS n_name, regionkey AS n_regionkey, comment AS n_comment FROM tpch.sf1.nation";
            case "region" -> "CREATE TABLE hive.tpch.region WITH (format='PARQUET') AS SELECT regionkey AS r_regionkey, name AS r_name, comment AS r_comment FROM tpch.sf1.region";
            case "part" -> "CREATE TABLE hive.tpch.part WITH (format='PARQUET') AS SELECT partkey AS p_partkey, name AS p_name, mfgr AS p_mfgr, brand AS p_brand, type AS p_type, size AS p_size, container AS p_container, retailprice AS p_retailprice, comment AS p_comment FROM tpch.sf1.part";
            case "supplier" -> "CREATE TABLE hive.tpch.supplier WITH (format='PARQUET') AS SELECT suppkey AS s_suppkey, name AS s_name, address AS s_address, nationkey AS s_nationkey, phone AS s_phone, acctbal AS s_acctbal, comment AS s_comment FROM tpch.sf1.supplier";
            case "partsupp" -> "CREATE TABLE hive.tpch.partsupp WITH (format='PARQUET') AS SELECT partkey AS ps_partkey, suppkey AS ps_suppkey, availqty AS ps_availqty, supplycost AS ps_supplycost, comment AS ps_comment FROM tpch.sf1.partsupp";
            case "customer" -> "CREATE TABLE hive.tpch.customer WITH (format='PARQUET') AS SELECT custkey AS c_custkey, name AS c_name, address AS c_address, nationkey AS c_nationkey, phone AS c_phone, acctbal AS c_acctbal, mktsegment AS c_mktsegment, comment AS c_comment FROM tpch.sf1.customer";
            case "orders" -> "CREATE TABLE hive.tpch.orders WITH (format='PARQUET') AS SELECT orderkey AS o_orderkey, custkey AS o_custkey, orderstatus AS o_orderstatus, totalprice AS o_totalprice, orderdate AS o_orderdate, orderpriority AS o_orderpriority, clerk AS o_clerk, shippriority AS o_shippriority, comment AS o_comment FROM tpch.sf1.orders";
            case "lineitem" -> "CREATE TABLE hive.tpch.lineitem WITH (format='PARQUET') AS SELECT orderkey AS l_orderkey, partkey AS l_partkey, suppkey AS l_suppkey, linenumber AS l_linenumber, quantity AS l_quantity, extendedprice AS l_extendedprice, discount AS l_discount, tax AS l_tax, returnflag AS l_returnflag, linestatus AS l_linestatus, shipdate AS l_shipdate, commitdate AS l_commitdate, receiptdate AS l_receiptdate, shipinstruct AS l_shipinstruct, shipmode AS l_shipmode, comment AS l_comment FROM tpch.sf1.lineitem";
            default -> throw new IllegalArgumentException("Unknown TPCH table: " + table);
        };
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
                .mapToObj(i -> String.format("%02d", i))
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
                                .mapToObj(i -> String.format("%02d", i)),
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
