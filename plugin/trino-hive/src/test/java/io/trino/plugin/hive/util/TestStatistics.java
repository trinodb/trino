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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import io.trino.spi.block.Block;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.Function;

import static io.trino.plugin.hive.HiveBasicStatistics.createEmptyStatistics;
import static io.trino.plugin.hive.HiveBasicStatistics.createZeroStatistics;
import static io.trino.plugin.hive.HiveColumnStatisticType.MAX_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.MIN_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.plugin.hive.util.Statistics.ReduceOperator.ADD;
import static io.trino.plugin.hive.util.Statistics.ReduceOperator.SUBTRACT;
import static io.trino.plugin.hive.util.Statistics.createHiveColumnStatistics;
import static io.trino.plugin.hive.util.Statistics.merge;
import static io.trino.plugin.hive.util.Statistics.reduce;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatistics
{
    @Test
    public void testCreateRealHiveColumnStatistics()
    {
        HiveColumnStatistics statistics;

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(-2391f)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(42f))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-2391d), OptionalDouble.of(42)));

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.NEGATIVE_INFINITY)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.POSITIVE_INFINITY))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.NaN)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.NaN))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(-15f)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(-0f))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-15d), OptionalDouble.of(-0d))); // TODO should we distinguish between -0 and 0?
    }

    private static HiveColumnStatistics createRealColumnStatistics(Map<HiveColumnStatisticType, Block> computedStatistics)
    {
        return createHiveColumnStatistics(computedStatistics, REAL, 1);
    }

    @Test
    public void testCreateDoubleHiveColumnStatistics()
    {
        HiveColumnStatistics statistics;

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, -2391d),
                MAX_VALUE, nativeValueToBlock(DOUBLE, 42d)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-2391d), OptionalDouble.of(42)));

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, Double.NEGATIVE_INFINITY),
                MAX_VALUE, nativeValueToBlock(DOUBLE, Double.POSITIVE_INFINITY)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, Double.NaN),
                MAX_VALUE, nativeValueToBlock(DOUBLE, Double.NaN)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, -15d),
                MAX_VALUE, nativeValueToBlock(DOUBLE, -0d)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-15d), OptionalDouble.of(-0d))); // TODO should we distinguish between -0 and 0?
    }

    private static HiveColumnStatistics createDoubleColumnStatistics(Map<HiveColumnStatisticType, Block> computedStatistics)
    {
        return createHiveColumnStatistics(computedStatistics, DOUBLE, 1);
    }

    @Test
    public void testReduce()
    {
        assertThat(reduce(createEmptyStatistics(), createEmptyStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createZeroStatistics(), createEmptyStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createZeroStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createEmptyStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createZeroStatistics(), createEmptyStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createZeroStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4), ADD))
                .isEqualTo(new HiveBasicStatistics(12, 11, 10, 9));
        assertThat(reduce(
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4), SUBTRACT))
                .isEqualTo(new HiveBasicStatistics(10, 7, 4, 1));
    }

    @Test
    public void testMergeEmptyColumnStatistics()
    {
        assertMergeHiveColumnStatistics(HiveColumnStatistics.empty(), HiveColumnStatistics.empty(), HiveColumnStatistics.empty());
    }

    @Test
    public void testMergeIntegerColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(3))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(3))).build());
    }

    @Test
    public void testMergeDoubleColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(3))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(3))).build());
    }

    @Test
    public void testMergeDecimalColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(0)), Optional.of(BigDecimal.valueOf(3)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(0)), Optional.of(BigDecimal.valueOf(3)))).build());
    }

    @Test
    public void testMergeDateColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(0)), Optional.of(LocalDate.ofEpochDay(3)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(0)), Optional.of(LocalDate.ofEpochDay(3)))).build());
    }

    @Test
    public void testMergeBooleanColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(2), OptionalLong.of(3))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(3), OptionalLong.of(5))).build());
    }

    @Test
    public void testMergeStringColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(1)).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(3)).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(3)).build());

        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.of(1)).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.of(3)).build(),
                HiveColumnStatistics.builder().setTotalSizeInBytes(OptionalLong.of(5)).build());
    }

    @Test
    public void testMergeGenericColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(2)).build());

        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(3)).build());
    }

    @Test
    public void testMergeHiveColumnStatisticsMap()
    {
        Map<String, HiveColumnStatistics> first = ImmutableMap.of(
                "column1", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)),
                "column2", HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(2), OptionalDouble.of(3), OptionalLong.of(4), OptionalLong.of(5)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(5), OptionalLong.of(5), OptionalLong.of(10)),
                "column4", createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3)));
        Map<String, HiveColumnStatistics> second = ImmutableMap.of(
                "column5", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)),
                "column2", HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(4), OptionalLong.of(4), OptionalLong.of(6)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(6), OptionalLong.of(5), OptionalLong.of(10)),
                "column6", createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3)));
        Map<String, HiveColumnStatistics> expected = ImmutableMap.of(
                "column2", HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(4), OptionalLong.of(8), OptionalLong.of(6)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(6), OptionalLong.of(10), OptionalLong.of(20)));
        assertThat(merge(first, second)).isEqualTo(expected);
        assertThat(merge(ImmutableMap.of(), ImmutableMap.of())).isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testFromComputedStatistics()
    {
        Function<Integer, Block> singleIntegerValueBlock = value ->
                BigintType.BIGINT.createBlockBuilder(null, 1).writeLong(value).build();

        ComputedStatistics statistics = ComputedStatistics.builder(ImmutableList.of(), ImmutableList.of())
                .addTableStatistic(TableStatisticType.ROW_COUNT, singleIntegerValueBlock.apply(5))
                .addColumnStatistic(MIN_VALUE.createColumnStatisticMetadata("a_column"), singleIntegerValueBlock.apply(1))
                .addColumnStatistic(MAX_VALUE.createColumnStatisticMetadata("a_column"), singleIntegerValueBlock.apply(5))
                .addColumnStatistic(NUMBER_OF_DISTINCT_VALUES.createColumnStatisticMetadata("a_column"), singleIntegerValueBlock.apply(5))
                .addColumnStatistic(NUMBER_OF_NON_NULL_VALUES.createColumnStatisticMetadata("a_column"), singleIntegerValueBlock.apply(5))
                .addColumnStatistic(NUMBER_OF_NON_NULL_VALUES.createColumnStatisticMetadata("b_column"), singleIntegerValueBlock.apply(4))
                .build();

        Map<String, Type> columnTypes = ImmutableMap.of("a_column", INTEGER, "b_column", VARCHAR);

        Map<String, HiveColumnStatistics> columnStatistics = Statistics.fromComputedStatistics(statistics.getColumnStatistics(), columnTypes, 5);

        assertThat(columnStatistics).hasSize(2);
        assertThat(columnStatistics.keySet()).contains("a_column", "b_column");
        assertThat(columnStatistics.get("a_column")).isEqualTo(
                HiveColumnStatistics.builder()
                        .setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(5)))
                        .setNullsCount(0)
                        .setDistinctValuesCount(5)
                        .build());
        assertThat(columnStatistics.get("b_column")).isEqualTo(
                HiveColumnStatistics.builder()
                        .setNullsCount(1)
                        .build());
    }

    private static void assertMergeHiveColumnStatistics(HiveColumnStatistics first, HiveColumnStatistics second, HiveColumnStatistics expected)
    {
        assertThat(merge(first, second)).isEqualTo(expected);
        assertThat(merge(second, first)).isEqualTo(expected);
    }
}
