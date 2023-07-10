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
package io.trino.plugin.hive.metastore.thrift;

import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.plugin.hive.metastore.thrift.ThriftSparkMetastoreUtil.fromMetastoreColumnStatistics;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.testng.Assert.assertEquals;

public class TestThriftSparkMetastoreUtil
{
    @Test
    public void testSparkLongStatsToColumnStatistics()
    {
        Map<String, String> columnStatistics = Map.of("spark.sql.statistics.colStats.c_long.min", "1",
                "spark.sql.statistics.colStats.c_long.distinctCount", "4",
                "spark.sql.statistics.colStats.c_long.maxLen", "4",
                "spark.sql.statistics.colStats.c_long.avgLen", "4",
                "spark.sql.statistics.colStats.c_long.nullCount", "0",
                "spark.sql.statistics.colStats.c_long.version", "2",
                "spark.sql.statistics.colStats.c_long.max", "4");
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics(new FieldSchema("c_long_not_exists", BIGINT_TYPE_NAME, null), columnStatistics, 4);
        assertEquals(
                actualNotExists,
                HiveColumnStatistics.builder()
                        .setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty()))
                        .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics(new FieldSchema("c_long", BIGINT_TYPE_NAME, null), columnStatistics, 4);
        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(4)))
                        .setNullsCount(0)
                        .setDistinctValuesCount(4)
                        .build());
    }

    @Test
    public void testSparkDoubleStatsToColumnStatistics()
    {
        Map<String, String> columnStatistics = Map.of("spark.sql.statistics.colStats.c_double.min", "0.3",
                "spark.sql.statistics.colStats.c_double.distinctCount", "10",
                "spark.sql.statistics.colStats.c_double.maxLen", "4",
                "spark.sql.statistics.colStats.c_double.avgLen", "4",
                "spark.sql.statistics.colStats.c_double.nullCount", "1",
                "spark.sql.statistics.colStats.c_double.version", "2",
                "spark.sql.statistics.colStats.c_double.max", "3.3");
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics(new FieldSchema("c_double_not_exists", DOUBLE_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actualNotExists,
                HiveColumnStatistics.builder()
                        .setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()))
                        .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics(new FieldSchema("c_double", DOUBLE_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0.3), OptionalDouble.of(3.3)))
                        .setNullsCount(1)
                        .setDistinctValuesCount(9)
                        .build());
    }

    @Test
    public void testSparkDecimalStatsToColumnStatistics()
    {
        Map<String, String> columnStatistics = Map.of("spark.sql.statistics.colStats.c_decimal.min", "0.3",
                "spark.sql.statistics.colStats.c_decimal.distinctCount", "10",
                "spark.sql.statistics.colStats.c_decimal.maxLen", "4",
                "spark.sql.statistics.colStats.c_decimal.avgLen", "4",
                "spark.sql.statistics.colStats.c_decimal.nullCount", "1",
                "spark.sql.statistics.colStats.c_decimal.version", "2",
                "spark.sql.statistics.colStats.c_decimal.max", "3.3");
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics(new FieldSchema("c_decimal_not_exists", DECIMAL_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actualNotExists,
                HiveColumnStatistics.builder()
                        .setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty()))
                        .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics(new FieldSchema("c_decimal", DECIMAL_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDecimalStatistics(new DecimalStatistics(Optional.of(new BigDecimal("0.3")), Optional.of(new BigDecimal("3.3"))))
                        .setNullsCount(1)
                        .setDistinctValuesCount(9)
                        .build());
    }

    @Test
    public void testSparkBooleanStatsToColumnStatistics()
    {
        Map<String, String> columnStatistics = Map.of("spark.sql.statistics.colStats.c_bool.min", "false",
                "spark.sql.statistics.colStats.c_bool.distinctCount", "2",
                "spark.sql.statistics.colStats.c_bool.maxLen", "1",
                "spark.sql.statistics.colStats.c_bool.avgLen", "1",
                "spark.sql.statistics.colStats.c_bool.nullCount", "1",
                "spark.sql.statistics.colStats.c_bool.version", "2",
                "spark.sql.statistics.colStats.c_bool.max", "true");
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics(new FieldSchema("c_bool_not_exists", BOOLEAN_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actualNotExists,
                HiveColumnStatistics.builder()
                        .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                        .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics(new FieldSchema("c_bool", BOOLEAN_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                        .setNullsCount(1)
                        .build());
    }

    @Test
    public void testSparkDateStatsToColumnStatistics()
    {
        Map<String, String> columnStatistics = Map.of("spark.sql.statistics.colStats.c_date.min", "2000-01-01",
                "spark.sql.statistics.colStats.c_date.distinctCount", "10",
                "spark.sql.statistics.colStats.c_date.maxLen", "4",
                "spark.sql.statistics.colStats.c_date.avgLen", "4",
                "spark.sql.statistics.colStats.c_date.nullCount", "3",
                "spark.sql.statistics.colStats.c_date.version", "2",
                "spark.sql.statistics.colStats.c_date.max", "2030-12-31");
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics(new FieldSchema("c_date_not_exists", DATE_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actualNotExists,
                HiveColumnStatistics.builder()
                        .setDateStatistics((new DateStatistics(Optional.empty(), Optional.empty())))
                        .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics(new FieldSchema("c_date", DATE_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDateStatistics((new DateStatistics(Optional.of(LocalDate.of(2000, 1, 1)), Optional.of(LocalDate.of(2030, 12, 31)))))
                        .setNullsCount(3)
                        .setDistinctValuesCount(7)
                        .build());
    }

    @Test
    public void testSparkStringStatsToColumnStatistics()
    {
        Map<String, String> columnStatistics = Map.of(
                "spark.sql.statistics.colStats.c_char.distinctCount", "3",
                "spark.sql.statistics.colStats.c_char.avgLen", "10",
                "spark.sql.statistics.colStats.char_col.maxLen", "10",
                "spark.sql.statistics.colStats.c_char.nullCount", "7",
                "spark.sql.statistics.colStats.c_char.version", "2");
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics(new FieldSchema("c_char_not_exists", STRING_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(actualNotExists, HiveColumnStatistics.builder().build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics(new FieldSchema("c_char", STRING_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setNullsCount(7)
                        .setDistinctValuesCount(3)
                        .setTotalSizeInBytes(30)
                        .build());
    }

    @Test
    public void testSparkBinaryStatsToColumnStatistics()
    {
        Map<String, String> columnStatistics = Map.of(
                "spark.sql.statistics.colStats.c_bin.distinctCount", "7",
                "spark.sql.statistics.colStats.c_bin.avgLen", "10",
                "spark.sql.statistics.colStats.c_bin.maxLen", "10",
                "spark.sql.statistics.colStats.c_bin.nullCount", "3",
                "spark.sql.statistics.colStats.c_bin.version", "2");
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics(new FieldSchema("c_bin_not_exists", BINARY_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(actualNotExists, HiveColumnStatistics.builder().build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics(new FieldSchema("c_bin", BINARY_TYPE_NAME, null), columnStatistics, 10);
        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setNullsCount(3)
                        .setTotalSizeInBytes(70)
                        .setMaxValueSizeInBytes(10)
                        .build());
    }
}
