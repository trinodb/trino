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
package io.trino.plugin.hive.metastore;

import io.trino.plugin.hive.HiveType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.plugin.hive.metastore.SparkMetastoreUtil.fromMetastoreColumnStatistics;
import static io.trino.plugin.hive.util.SerdeConstants.DECIMAL_TYPE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSparkMetastoreUtil
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
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics("c_long_not_exists", HiveType.HIVE_LONG, columnStatistics, 4);
        assertThat(actualNotExists).isEqualTo(HiveColumnStatistics.builder()
                .setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty()))
                .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics("c_long", HiveType.HIVE_LONG, columnStatistics, 4);
        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(4)))
                .setNullsCount(0)
                .setDistinctValuesWithNullCount(4)
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
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics("c_double_not_exists", HiveType.HIVE_DOUBLE, columnStatistics, 10);
        assertThat(actualNotExists).isEqualTo(HiveColumnStatistics.builder()
                .setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()))
                .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics("c_double", HiveType.HIVE_DOUBLE, columnStatistics, 10);
        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0.3), OptionalDouble.of(3.3)))
                .setNullsCount(1)
                .setDistinctValuesWithNullCount(10)
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
        HiveType decimalType = HiveType.valueOf(DECIMAL_TYPE_NAME);
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics("c_decimal_not_exists", decimalType, columnStatistics, 10);
        assertThat(actualNotExists).isEqualTo(HiveColumnStatistics.builder()
                .setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty()))
                .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics("c_decimal", decimalType, columnStatistics, 10);
        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDecimalStatistics(new DecimalStatistics(Optional.of(new BigDecimal("0.3")), Optional.of(new BigDecimal("3.3"))))
                .setNullsCount(1)
                .setDistinctValuesWithNullCount(10)
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
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics("c_bool_not_exists", HiveType.HIVE_BOOLEAN, columnStatistics, 10);
        assertThat(actualNotExists).isEqualTo(HiveColumnStatistics.builder()
                .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics("c_bool", HiveType.HIVE_BOOLEAN, columnStatistics, 10);
        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
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
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics("c_date_not_exists", HiveType.HIVE_DATE, columnStatistics, 10);
        assertThat(actualNotExists).isEqualTo(HiveColumnStatistics.builder()
                .setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty()))
                .build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics("c_date", HiveType.HIVE_DATE, columnStatistics, 10);
        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDateStatistics(new DateStatistics(Optional.of(LocalDate.of(2000, 1, 1)), Optional.of(LocalDate.of(2030, 12, 31))))
                .setNullsCount(3)
                .setDistinctValuesWithNullCount(10)
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
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics("c_char_not_exists", HiveType.HIVE_STRING, columnStatistics, 10);
        assertThat(actualNotExists).isEqualTo(HiveColumnStatistics.builder().build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics("c_char", HiveType.HIVE_STRING, columnStatistics, 10);
        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setNullsCount(7)
                .setDistinctValuesWithNullCount(3)
                .setAverageColumnLength(10)
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
        HiveColumnStatistics actualNotExists = fromMetastoreColumnStatistics("c_bin_not_exists", HiveType.HIVE_BINARY, columnStatistics, 10);
        assertThat(actualNotExists).isEqualTo(HiveColumnStatistics.builder().build());

        HiveColumnStatistics actual = fromMetastoreColumnStatistics("c_bin", HiveType.HIVE_BINARY, columnStatistics, 10);
        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setNullsCount(3)
                .setAverageColumnLength(10)
                .setMaxValueSizeInBytes(10)
                .build());
    }
}
