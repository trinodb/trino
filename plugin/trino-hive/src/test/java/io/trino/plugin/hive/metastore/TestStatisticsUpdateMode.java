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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.util.Statistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.plugin.hive.HiveBasicStatistics.createEmptyStatistics;
import static io.trino.plugin.hive.HiveBasicStatistics.createZeroStatistics;
import static io.trino.plugin.hive.HiveColumnStatisticType.MAX_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.MIN_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.plugin.hive.metastore.StatisticsUpdateMode.CLEAR_ALL;
import static io.trino.plugin.hive.metastore.StatisticsUpdateMode.MERGE_INCREMENTAL;
import static io.trino.plugin.hive.metastore.StatisticsUpdateMode.OVERWRITE_ALL;
import static io.trino.plugin.hive.metastore.StatisticsUpdateMode.OVERWRITE_SOME_COLUMNS;
import static io.trino.plugin.hive.metastore.StatisticsUpdateMode.UNDO_MERGE_INCREMENTAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestStatisticsUpdateMode
{
    private static final HiveBasicStatistics ONE_ROW = new HiveBasicStatistics(1, 1, 0, 0);

    @Test
    void testOverwriteAll()
    {
        // overwrite all should always return the second argument unmodified
        PartitionStatistics x = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        PartitionStatistics y = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        assertThat(OVERWRITE_ALL.updatePartitionStatistics(x, y)).isSameAs(y);
        assertThat(OVERWRITE_ALL.updatePartitionStatistics(y, x)).isSameAs(x);

        x = new PartitionStatistics(ONE_ROW, ImmutableMap.of());
        y = new PartitionStatistics(ONE_ROW, ImmutableMap.of());
        assertThat(OVERWRITE_ALL.updatePartitionStatistics(x, y)).isSameAs(y);
        assertThat(OVERWRITE_ALL.updatePartitionStatistics(y, x)).isSameAs(x);
    }

    @Test
    void testClearAll()
    {
        // overwrite all should always return the second argument unmodified
        PartitionStatistics partitionStatistics = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        assertThat(CLEAR_ALL.updatePartitionStatistics(partitionStatistics, partitionStatistics)).isSameAs(PartitionStatistics.empty());

        partitionStatistics = new PartitionStatistics(ONE_ROW, ImmutableMap.of());
        assertThat(CLEAR_ALL.updatePartitionStatistics(partitionStatistics, partitionStatistics)).isSameAs(PartitionStatistics.empty());
    }

    @Test
    void testOverwriteSomeBasicStats()
    {
        // overwrite some should always return the basic statistics from second argument unmodified
        HiveBasicStatistics basicStatistics = new HiveBasicStatistics(1, 2, 3, 4);
        assertThat(merge(OVERWRITE_SOME_COLUMNS, createEmptyStatistics(), basicStatistics)).isSameAs(basicStatistics);
        assertThat(merge(OVERWRITE_SOME_COLUMNS, ONE_ROW, basicStatistics)).isSameAs(basicStatistics);
    }

    @Test
    void testOverwriteSomeColumnStats()
    {
        assertThat(merge(OVERWRITE_SOME_COLUMNS, ImmutableMap.of(), ImmutableMap.of())).isEqualTo(ImmutableMap.of());

        // overwrite some returns all columns from botha arguments, favoring the second argument
        var first = ImmutableMap.<String, HiveColumnStatistics>builder()
                .put("a", createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3)))
                .put("shared", createBooleanColumnStatistics(OptionalLong.of(100), OptionalLong.of(200), OptionalLong.of(300)))
                .buildOrThrow();
        assertThat(merge(OVERWRITE_SOME_COLUMNS, first, ImmutableMap.of())).isEqualTo(first);
        assertThat(merge(OVERWRITE_SOME_COLUMNS, ImmutableMap.of(), first)).isEqualTo(first);

        var second = ImmutableMap.<String, HiveColumnStatistics>builder()
                .put("x", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)))
                .put("shared", createIntegerColumnStatistics(OptionalLong.of(100), OptionalLong.of(200), OptionalLong.of(300), OptionalLong.of(400)))
                .buildOrThrow();
        var expected = ImmutableMap.<String, HiveColumnStatistics>builder()
                .put("a", first.get("a"))
                .put("x", second.get("x"))
                .put("shared", second.get("shared"))
                .buildOrThrow();
        assertThat(merge(OVERWRITE_SOME_COLUMNS, first, second)).isEqualTo(expected);
    }

    @Test
    void testMergeIncrementalBasicStats()
    {
        // if either argument is empty, the result is empty
        assertThat(merge(MERGE_INCREMENTAL, createEmptyStatistics(), createEmptyStatistics())).isEqualTo(createEmptyStatistics());
        assertThat(merge(MERGE_INCREMENTAL, ONE_ROW, createEmptyStatistics())).isEqualTo(createEmptyStatistics());
        assertThat(merge(MERGE_INCREMENTAL, createEmptyStatistics(), ONE_ROW)).isEqualTo(createEmptyStatistics());

        // if one argument has zero rows, the result is the other argument
        assertThat(merge(MERGE_INCREMENTAL, createZeroStatistics(), ONE_ROW)).isSameAs(ONE_ROW);
        assertThat(merge(MERGE_INCREMENTAL, ONE_ROW, createZeroStatistics())).isSameAs(ONE_ROW);

        assertThat(merge(
                MERGE_INCREMENTAL,
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4)))
                .isEqualTo(new HiveBasicStatistics(12, 11, 10, 9));
    }

    @Test
    void testUndoMergeIncrementalBasicStats()
    {
        assertThat(merge(UNDO_MERGE_INCREMENTAL, createEmptyStatistics(), createEmptyStatistics())).isEqualTo(createEmptyStatistics());
        assertThat(merge(UNDO_MERGE_INCREMENTAL, ONE_ROW, createEmptyStatistics())).isEqualTo(createEmptyStatistics());
        assertThat(merge(UNDO_MERGE_INCREMENTAL, createEmptyStatistics(), ONE_ROW)).isEqualTo(createEmptyStatistics());
        assertThat(merge(
                UNDO_MERGE_INCREMENTAL,
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4)))
                .isEqualTo(new HiveBasicStatistics(10, 7, 4, 1));
    }

    @Test
    void testMergeEmptyColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(HiveColumnStatistics.empty(), HiveColumnStatistics.empty(), HiveColumnStatistics.empty());
    }

    @Test
    void testMergeIntegerColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(3))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(3))).build());
    }

    @Test
    void testMergeDoubleColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(3))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(3))).build());
    }

    @Test
    void testMergeDecimalColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(0)), Optional.of(BigDecimal.valueOf(3)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(0)), Optional.of(BigDecimal.valueOf(3)))).build());
    }

    @Test
    void testMergeDateColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(0)), Optional.of(LocalDate.ofEpochDay(3)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(0)), Optional.of(LocalDate.ofEpochDay(3)))).build());
    }

    @Test
    void testMergeBooleanColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(2), OptionalLong.of(3))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(3), OptionalLong.of(5))).build());
    }

    @Test
    void testMergeStringColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(1)).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(3)).build(),
                HiveColumnStatistics.builder().setMaxValueSizeInBytes(OptionalLong.of(3)).build());

        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(1)).build(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(2)).build(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(3)).build(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(2.5)).build());

        PartitionStatistics firstStats = new PartitionStatistics(
                new HiveBasicStatistics(1, 10, 0, 0),
                Map.of("column_name", HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(2)).build()));
        PartitionStatistics secondStats = new PartitionStatistics(
                new HiveBasicStatistics(1, 1, 0, 0),
                Map.of("column_name", HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(13)).build()));
        assertThat(MERGE_INCREMENTAL.updatePartitionStatistics(firstStats, secondStats).columnStatistics().get("column_name").getAverageColumnLength()).hasValue(3.0);
    }

    @Test
    void testMergeGenericColumnStatistics()
    {
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.empty()).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.empty()).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setDistinctValuesWithNullCount(OptionalLong.of(2)).build());

        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build());
        assertMergeIncrementalColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(3)).build());
    }

    private static void assertMergeIncrementalColumnStatistics(HiveColumnStatistics first, HiveColumnStatistics second, HiveColumnStatistics expected)
    {
        assertThat(merge(MERGE_INCREMENTAL, first, second)).isEqualTo(expected);
        assertThat(merge(MERGE_INCREMENTAL, second, first)).isEqualTo(expected);
    }

    @Test
    void testMergeHiveColumnStatisticsMap()
    {
        Map<String, HiveColumnStatistics> first = ImmutableMap.of(
                "column1", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)),
                "column2", HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(2), OptionalDouble.of(3), OptionalLong.of(4), OptionalLong.of(5)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(5), OptionalDouble.of(5), OptionalLong.of(10)),
                "column4", createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3)));
        Map<String, HiveColumnStatistics> second = ImmutableMap.of(
                "column5", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)),
                "column2", HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(4), OptionalLong.of(4), OptionalLong.of(6)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(6), OptionalDouble.of(5), OptionalLong.of(10)),
                "column6", createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3)));
        Map<String, HiveColumnStatistics> expected = ImmutableMap.of(
                "column2", HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(4), OptionalLong.of(8), OptionalLong.of(6)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(6), OptionalDouble.of(5), OptionalLong.of(20)));
        assertThat(merge(MERGE_INCREMENTAL, first, second)).isEqualTo(expected);
        assertThat(merge(MERGE_INCREMENTAL, ImmutableMap.of(), ImmutableMap.of())).isEqualTo(ImmutableMap.of());
    }

    @Test
    void testFromComputedStatistics()
    {
        ComputedStatistics statistics = ComputedStatistics.builder(ImmutableList.of(), ImmutableList.of())
                .addTableStatistic(TableStatisticType.ROW_COUNT, writeNativeValue(BIGINT, 5L))
                .addColumnStatistic(MIN_VALUE.createColumnStatisticMetadata("a_column"), writeNativeValue(INTEGER, 1L))
                .addColumnStatistic(MAX_VALUE.createColumnStatisticMetadata("a_column"), writeNativeValue(INTEGER, 5L))
                .addColumnStatistic(NUMBER_OF_DISTINCT_VALUES.createColumnStatisticMetadata("a_column"), writeNativeValue(BIGINT, 5L))
                .addColumnStatistic(NUMBER_OF_NON_NULL_VALUES.createColumnStatisticMetadata("a_column"), writeNativeValue(BIGINT, 5L))
                .addColumnStatistic(NUMBER_OF_NON_NULL_VALUES.createColumnStatisticMetadata("b_column"), writeNativeValue(BIGINT, 4L))
                .build();

        Map<String, Type> columnTypes = ImmutableMap.of("a_column", INTEGER, "b_column", VARCHAR);

        Map<String, HiveColumnStatistics> columnStatistics = Statistics.fromComputedStatistics(statistics.getColumnStatistics(), columnTypes, 5);

        assertThat(columnStatistics).hasSize(2);
        assertThat(columnStatistics.keySet()).contains("a_column", "b_column");
        assertThat(columnStatistics).containsEntry("a_column", HiveColumnStatistics.builder()
                        .setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(5)))
                        .setNullsCount(0)
                        .setDistinctValuesWithNullCount(5)
                        .build());
        assertThat(columnStatistics).containsEntry("b_column", HiveColumnStatistics.builder()
                        .setNullsCount(1)
                        .build());
    }

    private static HiveBasicStatistics merge(StatisticsUpdateMode mode, HiveBasicStatistics first, HiveBasicStatistics second)
    {
        return mode.updatePartitionStatistics(new PartitionStatistics(first, ImmutableMap.of()), new PartitionStatistics(second, ImmutableMap.of()))
                .basicStatistics();
    }

    private static HiveColumnStatistics merge(StatisticsUpdateMode mode, HiveColumnStatistics first, HiveColumnStatistics second)
    {
        return merge(mode, Map.of("column_name", first), Map.of("column_name", second))
                .get("column_name");
    }

    private static Map<String, HiveColumnStatistics> merge(StatisticsUpdateMode mode, Map<String, HiveColumnStatistics> first, Map<String, HiveColumnStatistics> second)
    {
        return mode.updatePartitionStatistics(new PartitionStatistics(ONE_ROW, first), new PartitionStatistics(ONE_ROW, second))
                .columnStatistics();
    }
}
