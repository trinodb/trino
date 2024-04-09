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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HiveBasicStatistics.createZeroStatistics;
import static io.trino.plugin.hive.HiveColumnStatisticType.MAX_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static io.trino.plugin.hive.HiveColumnStatisticType.MIN_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_COLUMN_STATISTIC_TYPE;
import static io.trino.plugin.hive.util.HiveWriteUtils.createPartitionValues;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class Statistics
{
    private Statistics() {}

    public static PartitionStatistics createEmptyPartitionStatistics(Map<String, Type> columnTypes, Map<String, Set<HiveColumnStatisticType>> columnStatisticsMetadataTypes)
    {
        Map<String, HiveColumnStatistics> columnStatistics = columnStatisticsMetadataTypes.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> createColumnStatisticsForEmptyPartition(columnTypes.get(entry.getKey()), entry.getValue())));
        return new PartitionStatistics(createZeroStatistics(), columnStatistics);
    }

    private static HiveColumnStatistics createColumnStatisticsForEmptyPartition(Type columnType, Set<HiveColumnStatisticType> columnStatisticTypes)
    {
        requireNonNull(columnType, "columnType is null");
        HiveColumnStatistics.Builder result = HiveColumnStatistics.builder();
        for (HiveColumnStatisticType columnStatisticType : columnStatisticTypes) {
            setColumnStatisticsForEmptyPartition(columnType, result, columnStatisticType);
        }
        return result.build();
    }

    private static void setColumnStatisticsForEmptyPartition(Type columnType, HiveColumnStatistics.Builder result, HiveColumnStatisticType columnStatisticType)
    {
        switch (columnStatisticType) {
            case MAX_VALUE_SIZE_IN_BYTES:
                result.setMaxValueSizeInBytes(0);
                return;
            case TOTAL_SIZE_IN_BYTES:
                result.setAverageColumnLength(0);
                return;
            case NUMBER_OF_DISTINCT_VALUES:
                result.setDistinctValuesWithNullCount(0);
                return;
            case NUMBER_OF_NON_NULL_VALUES:
                result.setNullsCount(0);
                return;
            case NUMBER_OF_TRUE_VALUES:
                result.setBooleanStatistics(new BooleanStatistics(OptionalLong.of(0L), OptionalLong.of(0L)));
                return;
            case MIN_VALUE:
            case MAX_VALUE:
                setMinMaxForEmptyPartition(columnType, result);
                return;
        }
        throw new TrinoException(HIVE_UNKNOWN_COLUMN_STATISTIC_TYPE, "Unknown column statistics type: " + columnStatisticType.name());
    }

    private static void setMinMaxForEmptyPartition(Type type, HiveColumnStatistics.Builder result)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            result.setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty()));
        }
        else if (type.equals(DOUBLE) || type.equals(REAL)) {
            result.setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));
        }
        else if (type.equals(DATE)) {
            result.setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty()));
        }
        else if (type instanceof DecimalType) {
            result.setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty()));
        }
        // TODO (https://github.com/trinodb/trino/issues/5859) Add support for timestamp
        else {
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    public static Map<List<String>, ComputedStatistics> createComputedStatisticsToPartitionMap(
            Collection<ComputedStatistics> computedStatistics,
            List<String> partitionColumns,
            Map<String, Type> columnTypes)
    {
        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(columnTypes::get)
                .collect(toImmutableList());

        return computedStatistics.stream()
                .collect(toImmutableMap(statistics -> getPartitionValues(statistics, partitionColumns, partitionColumnTypes), Function.identity()));
    }

    private static List<String> getPartitionValues(ComputedStatistics statistics, List<String> partitionColumns, List<Type> partitionColumnTypes)
    {
        checkArgument(statistics.getGroupingColumns().equals(partitionColumns),
                "Unexpected grouping. Partition columns: %s. Grouping columns: %s", partitionColumns, statistics.getGroupingColumns());
        Page partitionColumnsPage = new Page(1, statistics.getGroupingValues().toArray(new Block[] {}));
        return createPartitionValues(partitionColumnTypes, partitionColumnsPage, 0);
    }

    public static Map<String, HiveColumnStatistics> fromComputedStatistics(
            Map<ColumnStatisticMetadata, Block> computedStatistics,
            Map<String, Type> columnTypes,
            long rowCount)
    {
        return createColumnToComputedStatisticsMap(computedStatistics).entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> createHiveColumnStatistics(entry.getValue(), columnTypes.get(entry.getKey()), rowCount)));
    }

    private static Map<String, Map<HiveColumnStatisticType, Block>> createColumnToComputedStatisticsMap(Map<ColumnStatisticMetadata, Block> computedStatistics)
    {
        Map<String, Map<HiveColumnStatisticType, Block>> result = new HashMap<>();
        computedStatistics.forEach((metadata, block) -> {
            Map<HiveColumnStatisticType, Block> columnStatistics = result.computeIfAbsent(metadata.getColumnName(), key -> new HashMap<>());
            columnStatistics.put(HiveColumnStatisticType.from(metadata), block);
        });
        return result.entrySet()
                .stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
    }

    @VisibleForTesting
    public static HiveColumnStatistics createHiveColumnStatistics(
            Map<HiveColumnStatisticType, Block> computedStatistics,
            Type columnType,
            long rowCount)
    {
        HiveColumnStatistics.Builder result = HiveColumnStatistics.builder();

        // MIN_VALUE, MAX_VALUE
        // We ask the engine to compute either both or neither
        verify(computedStatistics.containsKey(MIN_VALUE) == computedStatistics.containsKey(MAX_VALUE));
        if (computedStatistics.containsKey(MIN_VALUE)) {
            setMinMax(columnType, computedStatistics.get(MIN_VALUE), computedStatistics.get(MAX_VALUE), result);
        }

        // MAX_VALUE_SIZE_IN_BYTES
        if (computedStatistics.containsKey(MAX_VALUE_SIZE_IN_BYTES)) {
            result.setMaxValueSizeInBytes(getIntegerValue(BIGINT, computedStatistics.get(MAX_VALUE_SIZE_IN_BYTES)));
        }

        // TOTAL_VALUES_SIZE_IN_BYTES
        if (computedStatistics.containsKey(TOTAL_SIZE_IN_BYTES)) {
            OptionalLong totalSizeInBytes = getIntegerValue(BIGINT, computedStatistics.get(TOTAL_SIZE_IN_BYTES));
            OptionalLong numNonNullValues = getIntegerValue(BIGINT, computedStatistics.get(NUMBER_OF_NON_NULL_VALUES));
            result.setAverageColumnLength(getAverageColumnLength(totalSizeInBytes, numNonNullValues));
        }

        // NUMBER OF NULLS
        if (computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            result.setNullsCount(rowCount - BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0));
        }

        // NDV
        if (computedStatistics.containsKey(NUMBER_OF_DISTINCT_VALUES) && computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            // number of distinct value is estimated using HLL, and can be higher than the number of non null values
            long numberOfNonNullValues = BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0);
            long numberOfDistinctValues = BIGINT.getLong(computedStatistics.get(NUMBER_OF_DISTINCT_VALUES), 0);
            // Hive expects NDV to be one greater when column has a null
            result.setDistinctValuesWithNullCount(Math.min(numberOfDistinctValues, numberOfNonNullValues) + (rowCount > numberOfNonNullValues ? 1 : 0));
        }

        // NUMBER OF FALSE, NUMBER OF TRUE
        if (computedStatistics.containsKey(NUMBER_OF_TRUE_VALUES) && computedStatistics.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            long numberOfTrue = BIGINT.getLong(computedStatistics.get(NUMBER_OF_TRUE_VALUES), 0);
            long numberOfNonNullValues = BIGINT.getLong(computedStatistics.get(NUMBER_OF_NON_NULL_VALUES), 0);
            result.setBooleanStatistics(new BooleanStatistics(OptionalLong.of(numberOfTrue), OptionalLong.of(numberOfNonNullValues - numberOfTrue)));
        }
        return result.build();
    }

    private static void setMinMax(Type type, Block min, Block max, HiveColumnStatistics.Builder result)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            result.setIntegerStatistics(new IntegerStatistics(getIntegerValue(type, min), getIntegerValue(type, max)));
        }
        else if (type.equals(DOUBLE) || type.equals(REAL)) {
            result.setDoubleStatistics(new DoubleStatistics(getDoubleValue(type, min), getDoubleValue(type, max)));
        }
        else if (type.equals(DATE)) {
            result.setDateStatistics(new DateStatistics(getDateValue(type, min), getDateValue(type, max)));
        }
        else if (type instanceof DecimalType) {
            result.setDecimalStatistics(new DecimalStatistics(getDecimalValue(type, min), getDecimalValue(type, max)));
        }
        // TODO (https://github.com/trinodb/trino/issues/5859) Add support for timestamp
        else {
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static OptionalLong getIntegerValue(Type type, Block block)
    {
        verify(type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(type.getLong(block, 0));
    }

    private static OptionalDouble getDoubleValue(Type type, Block block)
    {
        verify(type == DOUBLE || type == REAL, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return OptionalDouble.empty();
        }
        double value;
        if (type == DOUBLE) {
            value = type.getDouble(block, 0);
        }
        else {
            verify(type == REAL);
            value = intBitsToFloat(toIntExact(type.getLong(block, 0)));
        }
        if (!Double.isFinite(value)) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(value);
    }

    private static Optional<LocalDate> getDateValue(Type type, Block block)
    {
        verify(type == DATE, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return Optional.empty();
        }
        int days = toIntExact(type.getLong(block, 0));
        return Optional.of(LocalDate.ofEpochDay(days));
    }

    private static Optional<BigDecimal> getDecimalValue(Type type, Block block)
    {
        verify(type instanceof DecimalType, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return Optional.empty();
        }
        return Optional.of(Decimals.readBigDecimal((DecimalType) type, block, 0));
    }

    private static OptionalDouble getAverageColumnLength(OptionalLong totalSizeInBytes, OptionalLong numNonNullValues)
    {
        if (totalSizeInBytes.isEmpty() || numNonNullValues.isEmpty()) {
            return OptionalDouble.empty();
        }

        long nonNullsCount = numNonNullValues.getAsLong();
        if (nonNullsCount <= 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(((double) totalSizeInBytes.getAsLong()) / nonNullsCount);
    }
}
