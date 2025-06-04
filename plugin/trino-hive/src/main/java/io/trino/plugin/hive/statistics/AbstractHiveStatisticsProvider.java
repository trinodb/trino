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

package io.trino.plugin.hive.statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.hash.HashFunction;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.metastore.DateStatistics;
import io.trino.metastore.DecimalStatistics;
import io.trino.metastore.DoubleStatistics;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HivePartition;
import io.trino.metastore.IntegerStatistics;
import io.trino.metastore.PartitionStatistics;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.DoubleStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.hash.Hashing.murmur3_128;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS;
import static io.trino.plugin.hive.HiveSessionProperties.getPartitionStatisticsSampleSize;
import static io.trino.plugin.hive.HiveSessionProperties.isIgnoreCorruptedStatistics;
import static io.trino.plugin.hive.HiveSessionProperties.isStatisticsEnabled;
import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

public abstract class AbstractHiveStatisticsProvider
        implements HiveStatisticsProvider
{
    private static final Logger log = Logger.get(AbstractHiveStatisticsProvider.class);

    @Override
    public TableStatistics getTableStatistics(
            ConnectorSession session,
            SchemaTableName table,
            Map<String, ColumnHandle> columns,
            Map<String, Type> columnTypes,
            List<HivePartition> partitions)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }
        if (partitions.isEmpty()) {
            return createZeroStatistics(columns, columnTypes);
        }
        int sampleSize = getPartitionStatisticsSampleSize(session);
        List<HivePartition> partitionsSample = getPartitionsSample(partitions, sampleSize);
        try {
            Map<String, PartitionStatistics> statisticsSample = getPartitionsStatistics(session, table, partitionsSample, columns.keySet());
            validatePartitionStatistics(table, statisticsSample);
            return getTableStatistics(columns, columnTypes, partitions, statisticsSample);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(HIVE_CORRUPTED_COLUMN_STATISTICS.toErrorCode()) && isIgnoreCorruptedStatistics(session)) {
                log.error(e);
                return TableStatistics.empty();
            }
            throw e;
        }
    }

    protected abstract Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns);

    private TableStatistics createZeroStatistics(Map<String, ColumnHandle> columns, Map<String, Type> columnTypes)
    {
        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(0));
        columns.forEach((columnName, columnHandle) -> {
            Type columnType = columnTypes.get(columnName);
            verifyNotNull(columnType, "columnType is missing for column: %s", columnName);
            ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
            columnStatistics.setNullsFraction(Estimate.of(0));
            columnStatistics.setDistinctValuesCount(Estimate.of(0));
            if (hasDataSize(columnType)) {
                columnStatistics.setDataSize(Estimate.of(0));
            }
            result.setColumnStatistics(columnHandle, columnStatistics.build());
        });
        return result.build();
    }

    @VisibleForTesting
    static List<HivePartition> getPartitionsSample(List<HivePartition> partitions, int sampleSize)
    {
        checkArgument(sampleSize > 0, "sampleSize is expected to be greater than zero");

        if (partitions.size() <= sampleSize) {
            return partitions;
        }

        List<HivePartition> result = new ArrayList<>();

        int samplesLeft = sampleSize;

        HivePartition min = partitions.get(0);
        HivePartition max = partitions.get(0);
        for (HivePartition partition : partitions) {
            if (partition.getPartitionId().compareTo(min.getPartitionId()) < 0) {
                min = partition;
            }
            else if (partition.getPartitionId().compareTo(max.getPartitionId()) > 0) {
                max = partition;
            }
        }

        result.add(min);
        samplesLeft--;
        if (samplesLeft > 0) {
            result.add(max);
            samplesLeft--;
        }

        if (samplesLeft > 0) {
            HashFunction hashFunction = murmur3_128();
            Comparator<Map.Entry<HivePartition, Long>> hashComparator = Comparator
                    .<Map.Entry<HivePartition, Long>, Long>comparing(Map.Entry::getValue)
                    .thenComparing(entry -> entry.getKey().getPartitionId());
            partitions.stream()
                    .filter(partition -> !result.contains(partition))
                    .map(partition -> immutableEntry(partition, hashFunction.hashUnencodedChars(partition.getPartitionId()).asLong()))
                    .sorted(hashComparator)
                    .limit(samplesLeft)
                    .forEachOrdered(entry -> result.add(entry.getKey()));
        }

        return unmodifiableList(result);
    }

    @VisibleForTesting
    static void validatePartitionStatistics(SchemaTableName table, Map<String, PartitionStatistics> partitionStatistics)
    {
        partitionStatistics.forEach((partition, statistics) -> {
            HiveBasicStatistics basicStatistics = statistics.basicStatistics();
            OptionalLong rowCount = basicStatistics.getRowCount();
            rowCount.ifPresent(count -> checkStatistics(count >= 0, table, partition, "rowCount must be greater than or equal to zero: %s", count));
            basicStatistics.getFileCount().ifPresent(count -> checkStatistics(count >= 0, table, partition, "fileCount must be greater than or equal to zero: %s", count));
            basicStatistics.getInMemoryDataSizeInBytes().ifPresent(size -> checkStatistics(size >= 0, table, partition, "inMemoryDataSizeInBytes must be greater than or equal to zero: %s", size));
            basicStatistics.getOnDiskDataSizeInBytes().ifPresent(size -> checkStatistics(size >= 0, table, partition, "onDiskDataSizeInBytes must be greater than or equal to zero: %s", size));
            statistics.columnStatistics().forEach((column, columnStatistics) -> validateColumnStatistics(table, partition, column, rowCount, columnStatistics));
        });
    }

    private static void validateColumnStatistics(SchemaTableName table, String partition, String column, OptionalLong rowCount, HiveColumnStatistics columnStatistics)
    {
        columnStatistics.getMaxValueSizeInBytes().ifPresent(maxValueSizeInBytes ->
                checkStatistics(maxValueSizeInBytes >= 0, table, partition, column, "maxValueSizeInBytes must be greater than or equal to zero: %s", maxValueSizeInBytes));
        columnStatistics.getAverageColumnLength().ifPresent(averageColumnLength ->
                checkStatistics(averageColumnLength >= 0, table, partition, column, "averageColumnLength must be greater than or equal to zero: %s", averageColumnLength));
        columnStatistics.getNullsCount().ifPresent(nullsCount -> {
            checkStatistics(nullsCount >= 0, table, partition, column, "nullsCount must be greater than or equal to zero: %s", nullsCount);
            if (rowCount.isPresent()) {
                checkStatistics(
                        nullsCount <= rowCount.getAsLong(),
                        table,
                        partition,
                        column,
                        "nullsCount must be less than or equal to rowCount. nullsCount: %s. rowCount: %s.",
                        nullsCount,
                        rowCount.getAsLong());
            }
        });
        columnStatistics.getDistinctValuesWithNullCount().ifPresent(distinctValuesCount -> {
            checkStatistics(distinctValuesCount >= 0, table, partition, column, "distinctValuesCount must be greater than or equal to zero: %s", distinctValuesCount);
        });

        columnStatistics.getIntegerStatistics().ifPresent(integerStatistics -> {
            OptionalLong min = integerStatistics.getMin();
            OptionalLong max = integerStatistics.getMax();
            if (min.isPresent() && max.isPresent()) {
                checkStatistics(
                        min.getAsLong() <= max.getAsLong(),
                        table,
                        partition,
                        column,
                        "integerStatistics.min must be less than or equal to integerStatistics.max. integerStatistics.min: %s. integerStatistics.max: %s.",
                        min.getAsLong(),
                        max.getAsLong());
            }
        });
        columnStatistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
            OptionalDouble min = doubleStatistics.getMin();
            OptionalDouble max = doubleStatistics.getMax();
            if (min.isPresent() && max.isPresent() && !isNaN(min.getAsDouble()) && !isNaN(max.getAsDouble())) {
                checkStatistics(
                        min.getAsDouble() <= max.getAsDouble(),
                        table,
                        partition,
                        column,
                        "doubleStatistics.min must be less than or equal to doubleStatistics.max. doubleStatistics.min: %s. doubleStatistics.max: %s.",
                        min.getAsDouble(),
                        max.getAsDouble());
            }
        });
        columnStatistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
            Optional<BigDecimal> min = decimalStatistics.getMin();
            Optional<BigDecimal> max = decimalStatistics.getMax();
            if (min.isPresent() && max.isPresent()) {
                checkStatistics(
                        min.get().compareTo(max.get()) <= 0,
                        table,
                        partition,
                        column,
                        "decimalStatistics.min must be less than or equal to decimalStatistics.max. decimalStatistics.min: %s. decimalStatistics.max: %s.",
                        min.get(),
                        max.get());
            }
        });
        columnStatistics.getDateStatistics().ifPresent(dateStatistics -> {
            Optional<LocalDate> min = dateStatistics.getMin();
            Optional<LocalDate> max = dateStatistics.getMax();
            if (min.isPresent() && max.isPresent()) {
                checkStatistics(
                        min.get().compareTo(max.get()) <= 0,
                        table,
                        partition,
                        column,
                        "dateStatistics.min must be less than or equal to dateStatistics.max. dateStatistics.min: %s. dateStatistics.max: %s.",
                        min.get(),
                        max.get());
            }
        });
        columnStatistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
            OptionalLong falseCount = booleanStatistics.getFalseCount();
            OptionalLong trueCount = booleanStatistics.getTrueCount();
            falseCount.ifPresent(count ->
                    checkStatistics(count >= 0, table, partition, column, "falseCount must be greater than or equal to zero: %s", count));
            trueCount.ifPresent(count ->
                    checkStatistics(count >= 0, table, partition, column, "trueCount must be greater than or equal to zero: %s", count));
            if (rowCount.isPresent() && falseCount.isPresent()) {
                checkStatistics(
                        falseCount.getAsLong() <= rowCount.getAsLong(),
                        table,
                        partition,
                        column,
                        "booleanStatistics.falseCount must be less than or equal to rowCount. booleanStatistics.falseCount: %s. rowCount: %s.",
                        falseCount.getAsLong(),
                        rowCount.getAsLong());
            }
            if (rowCount.isPresent() && trueCount.isPresent()) {
                checkStatistics(
                        trueCount.getAsLong() <= rowCount.getAsLong(),
                        table,
                        partition,
                        column,
                        "booleanStatistics.trueCount must be less than or equal to rowCount. booleanStatistics.trueCount: %s. rowCount: %s.",
                        trueCount.getAsLong(),
                        rowCount.getAsLong());
            }
        });
    }

    @FormatMethod
    private static void checkStatistics(boolean expression, SchemaTableName table, String partition, String column, @FormatString String message, Object... args)
    {
        if (!expression) {
            throw new TrinoException(
                    HIVE_CORRUPTED_COLUMN_STATISTICS,
                    format("Corrupted partition statistics (Table: %s Partition: [%s] Column: %s): %s", table, partition, column, format(message, args)));
        }
    }

    @FormatMethod
    private static void checkStatistics(boolean expression, SchemaTableName table, String partition, @FormatString String message, Object... args)
    {
        if (!expression) {
            throw new TrinoException(
                    HIVE_CORRUPTED_COLUMN_STATISTICS,
                    format("Corrupted partition statistics (Table: %s Partition: [%s]): %s", table, partition, format(message, args)));
        }
    }

    private static TableStatistics getTableStatistics(
            Map<String, ColumnHandle> columns,
            Map<String, Type> columnTypes,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics)
    {
        if (statistics.isEmpty()) {
            return createEmptyTableStatisticsWithPartitionColumnStatistics(columns, columnTypes, partitions);
        }

        checkArgument(!partitions.isEmpty(), "partitions is empty");

        Optional<PartitionsRowCount> optionalRowCount = calculatePartitionsRowCount(statistics.values(), partitions.size());
        if (optionalRowCount.isEmpty()) {
            return createEmptyTableStatisticsWithPartitionColumnStatistics(columns, columnTypes, partitions);
        }
        double rowCount = optionalRowCount.get().getRowCount();

        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(rowCount));
        for (Map.Entry<String, ColumnHandle> column : columns.entrySet()) {
            String columnName = column.getKey();
            HiveColumnHandle columnHandle = (HiveColumnHandle) column.getValue();
            Type columnType = columnTypes.get(columnName);
            ColumnStatistics columnStatistics;
            if (columnHandle.isPartitionKey()) {
                double averageRowsPerPartition = optionalRowCount.get().getAverageRowsPerPartition();
                columnStatistics = createPartitionColumnStatistics(columnHandle, columnType, partitions, statistics, averageRowsPerPartition, rowCount);
            }
            else {
                columnStatistics = createDataColumnStatistics(columnName, columnType, rowCount, statistics.values());
            }
            result.setColumnStatistics(columnHandle, columnStatistics);
        }
        return result.build();
    }

    private static TableStatistics createEmptyTableStatisticsWithPartitionColumnStatistics(
            Map<String, ColumnHandle> columns,
            Map<String, Type> columnTypes,
            List<HivePartition> partitions)
    {
        TableStatistics.Builder result = TableStatistics.builder();
        // Estimate stats for partitioned columns even when row count is unavailable. This will help us use
        // ndv stats in rules like "ApplyPreferredTableWriterPartitioning".
        for (Map.Entry<String, ColumnHandle> column : columns.entrySet()) {
            HiveColumnHandle columnHandle = (HiveColumnHandle) column.getValue();
            if (columnHandle.isPartitionKey()) {
                result.setColumnStatistics(
                        columnHandle,
                        createPartitionColumnStatisticsWithoutRowCount(columnHandle, columnTypes.get(column.getKey()), partitions));
            }
        }
        return result.build();
    }

    @VisibleForTesting
    static Optional<PartitionsRowCount> calculatePartitionsRowCount(Collection<PartitionStatistics> statistics, int queriedPartitionsCount)
    {
        long[] rowCounts = statistics.stream()
                .map(PartitionStatistics::basicStatistics)
                .map(HiveBasicStatistics::getRowCount)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .peek(count -> verify(count >= 0, "count must be greater than or equal to zero"))
                .toArray();
        int sampleSize = statistics.size();
        // Sample contains all the queried partitions, estimate avg normally
        if (rowCounts.length <= 2 || queriedPartitionsCount == sampleSize) {
            OptionalDouble averageRowsPerPartitionOptional = Arrays.stream(rowCounts).average();
            if (averageRowsPerPartitionOptional.isEmpty()) {
                return Optional.empty();
            }
            double averageRowsPerPartition = averageRowsPerPartitionOptional.getAsDouble();
            return Optional.of(new PartitionsRowCount(averageRowsPerPartition, averageRowsPerPartition * queriedPartitionsCount));
        }

        // Some partitions (e.g. __HIVE_DEFAULT_PARTITION__) may be outliers in terms of row count.
        // Excluding the min and max rowCount values from averageRowsPerPartition calculation helps to reduce the
        // possibility of errors in the extrapolated rowCount due to a couple of outliers.
        int minIndex = 0;
        int maxIndex = 0;
        long rowCountSum = rowCounts[0];
        for (int index = 1; index < rowCounts.length; index++) {
            if (rowCounts[index] < rowCounts[minIndex]) {
                minIndex = index;
            }
            else if (rowCounts[index] > rowCounts[maxIndex]) {
                maxIndex = index;
            }
            rowCountSum += rowCounts[index];
        }
        double averageWithoutOutliers = ((double) (rowCountSum - rowCounts[minIndex] - rowCounts[maxIndex])) / (rowCounts.length - 2);
        double rowCount = (averageWithoutOutliers * (queriedPartitionsCount - 2)) + rowCounts[minIndex] + rowCounts[maxIndex];
        return Optional.of(new PartitionsRowCount(averageWithoutOutliers, rowCount));
    }

    @VisibleForTesting
    static class PartitionsRowCount
    {
        private final double averageRowsPerPartition;
        private final double rowCount;

        PartitionsRowCount(double averageRowsPerPartition, double rowCount)
        {
            verify(averageRowsPerPartition >= 0, "averageRowsPerPartition must be greater than or equal to zero");
            verify(rowCount >= 0, "rowCount must be greater than or equal to zero");
            this.averageRowsPerPartition = averageRowsPerPartition;
            this.rowCount = rowCount;
        }

        private double getAverageRowsPerPartition()
        {
            return averageRowsPerPartition;
        }

        private double getRowCount()
        {
            return rowCount;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionsRowCount that = (PartitionsRowCount) o;
            return Double.compare(that.averageRowsPerPartition, averageRowsPerPartition) == 0
                    && Double.compare(that.rowCount, rowCount) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(averageRowsPerPartition, rowCount);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("averageRowsPerPartition", averageRowsPerPartition)
                    .add("rowCount", rowCount)
                    .toString();
        }
    }

    private static ColumnStatistics createPartitionColumnStatistics(
            HiveColumnHandle column,
            Type type,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics,
            double averageRowsPerPartition,
            double rowCount)
    {
        List<HivePartition> nonEmptyPartitions = partitions.stream()
                .filter(partition -> getPartitionRowCount(partition.getPartitionId(), statistics).orElse(averageRowsPerPartition) != 0)
                .collect(toImmutableList());

        return ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(calculateDistinctPartitionKeys(column, nonEmptyPartitions)))
                .setNullsFraction(Estimate.of(calculateNullsFractionForPartitioningKey(column, partitions, statistics, averageRowsPerPartition, rowCount)))
                .setRange(calculateRangeForPartitioningKey(column, type, nonEmptyPartitions))
                .setDataSize(calculateDataSizeForPartitioningKey(column, type, partitions, statistics, averageRowsPerPartition))
                .build();
    }

    private static ColumnStatistics createPartitionColumnStatisticsWithoutRowCount(HiveColumnHandle column, Type type, List<HivePartition> partitions)
    {
        if (partitions.isEmpty()) {
            return ColumnStatistics.empty();
        }

        // Since we don't know the row count for each partition, we are taking an assumption here that all partitions
        // are non-empty and contains exactly same amount of data. This will help us estimate ndv stats for partitioned
        // columns which can be useful for certain optimizer rules.
        double estimatedNullsCount = partitions.stream()
                .filter(partition -> partition.getKeys().get(column).isNull())
                .count();

        return ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(calculateDistinctPartitionKeys(column, partitions)))
                .setNullsFraction(Estimate.of(normalizeFraction(estimatedNullsCount / partitions.size())))
                .setRange(calculateRangeForPartitioningKey(column, type, partitions))
                .build();
    }

    @VisibleForTesting
    static long calculateDistinctPartitionKeys(
            HiveColumnHandle column,
            List<HivePartition> partitions)
    {
        return partitions.stream()
                .map(partition -> partition.getKeys().get(column))
                .filter(value -> !value.isNull())
                .distinct()
                .count();
    }

    @VisibleForTesting
    static double calculateNullsFractionForPartitioningKey(
            HiveColumnHandle column,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics,
            double averageRowsPerPartition,
            double rowCount)
    {
        if (rowCount == 0) {
            return 0;
        }
        double estimatedNullsCount = partitions.stream()
                .filter(partition -> partition.getKeys().get(column).isNull())
                .map(HivePartition::getPartitionId)
                .mapToDouble(partitionName -> getPartitionRowCount(partitionName, statistics).orElse(averageRowsPerPartition))
                .sum();
        return normalizeFraction(estimatedNullsCount / rowCount);
    }

    private static double normalizeFraction(double fraction)
    {
        checkArgument(!isNaN(fraction), "fraction is NaN");
        checkArgument(isFinite(fraction), "fraction must be finite");
        if (fraction < 0) {
            return 0;
        }
        if (fraction > 1) {
            return 1;
        }
        return fraction;
    }

    @VisibleForTesting
    static Estimate calculateDataSizeForPartitioningKey(
            HiveColumnHandle column,
            Type type,
            List<HivePartition> partitions,
            Map<String, PartitionStatistics> statistics,
            double averageRowsPerPartition)
    {
        if (!hasDataSize(type)) {
            return Estimate.unknown();
        }
        double dataSize = 0;
        for (HivePartition partition : partitions) {
            int length = getSize(partition.getKeys().get(column));
            double rowCount = getPartitionRowCount(partition.getPartitionId(), statistics).orElse(averageRowsPerPartition);
            dataSize += length * rowCount;
        }
        return Estimate.of(dataSize);
    }

    private static boolean hasDataSize(Type type)
    {
        return type instanceof VarcharType || type instanceof CharType;
    }

    private static int getSize(NullableValue nullableValue)
    {
        if (nullableValue.isNull()) {
            return 0;
        }
        Object value = nullableValue.getValue();
        checkArgument(value instanceof Slice, "value is expected to be of Slice type");
        return ((Slice) value).length();
    }

    private static OptionalDouble getPartitionRowCount(String partitionName, Map<String, PartitionStatistics> statistics)
    {
        PartitionStatistics partitionStatistics = statistics.get(partitionName);
        if (partitionStatistics == null) {
            return OptionalDouble.empty();
        }
        OptionalLong rowCount = partitionStatistics.basicStatistics().getRowCount();
        if (rowCount.isPresent()) {
            verify(rowCount.getAsLong() >= 0, "rowCount must be greater than or equal to zero");
            return OptionalDouble.of(rowCount.getAsLong());
        }
        return OptionalDouble.empty();
    }

    @VisibleForTesting
    static Optional<DoubleRange> calculateRangeForPartitioningKey(HiveColumnHandle column, Type type, List<HivePartition> partitions)
    {
        List<OptionalDouble> convertedValues = partitions.stream()
                .map(HivePartition::getKeys)
                .map(keys -> keys.get(column))
                .filter(value -> !value.isNull())
                .map(NullableValue::getValue)
                .map(value -> convertPartitionValueToDouble(type, value))
                .collect(toImmutableList());

        if (convertedValues.stream().noneMatch(OptionalDouble::isPresent)) {
            return Optional.empty();
        }
        double[] values = convertedValues.stream()
                .peek(convertedValue -> checkState(convertedValue.isPresent(), "convertedValue is missing"))
                .mapToDouble(OptionalDouble::getAsDouble)
                .toArray();
        verify(values.length != 0, "No values");

        if (DoubleStream.of(values).anyMatch(Double::isNaN)) {
            return Optional.empty();
        }

        double min = DoubleStream.of(values).min().orElseThrow();
        double max = DoubleStream.of(values).max().orElseThrow();
        return Optional.of(new DoubleRange(min, max));
    }

    @VisibleForTesting
    static OptionalDouble convertPartitionValueToDouble(Type type, Object value)
    {
        return toStatsRepresentation(type, value);
    }

    @VisibleForTesting
    static ColumnStatistics createDataColumnStatistics(String column, Type type, double rowsCount, Collection<PartitionStatistics> partitionStatistics)
    {
        List<HiveColumnStatistics> columnStatistics = partitionStatistics.stream()
                .map(PartitionStatistics::columnStatistics)
                .map(statistics -> statistics.get(column))
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (columnStatistics.isEmpty()) {
            return ColumnStatistics.empty();
        }

        return ColumnStatistics.builder()
                .setDistinctValuesCount(calculateDistinctValuesCount(column, partitionStatistics))
                .setNullsFraction(calculateNullsFraction(column, partitionStatistics))
                .setDataSize(calculateDataSize(column, partitionStatistics, rowsCount))
                .setRange(calculateRange(type, columnStatistics))
                .build();
    }

    @VisibleForTesting
    static Estimate calculateDistinctValuesCount(String column, Collection<PartitionStatistics> partitionStatistics)
    {
        return partitionStatistics.stream()
                .map(statistics -> getDistinctValuesCount(column, statistics))
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong)
                .peek(distinctValuesCount -> verify(distinctValuesCount >= 0, "distinctValuesCount must be greater than or equal to zero"))
                .max(Long::compare)
                .map(Estimate::of)
                .orElse(Estimate.unknown());
    }

    @VisibleForTesting
    static OptionalLong getDistinctValuesCount(String column, PartitionStatistics partitionStatistics)
    {
        HiveColumnStatistics statistics = partitionStatistics.columnStatistics().get(column);
        if (statistics == null) {
            return OptionalLong.empty();
        }

        if (statistics.getBooleanStatistics().isPresent() &&
                statistics.getBooleanStatistics().get().getFalseCount().isPresent() &&
                statistics.getBooleanStatistics().get().getTrueCount().isPresent()) {
            long falseCount = statistics.getBooleanStatistics().get().getFalseCount().getAsLong();
            long trueCount = statistics.getBooleanStatistics().get().getTrueCount().getAsLong();
            return OptionalLong.of((falseCount > 0 ? 1 : 0) + (trueCount > 0 ? 1 : 0));
        }

        if (statistics.getDistinctValuesWithNullCount().isEmpty()) {
            return OptionalLong.empty();
        }

        long distinctValuesCount = statistics.getDistinctValuesWithNullCount().getAsLong();

        // Hive includes nulls in the distinct values count, but Trino does not
        long nullsCount = statistics.getNullsCount().orElse(0);
        if (distinctValuesCount > 0 && nullsCount > 0) {
            distinctValuesCount--;
        }

        // if there is non-null row the distinct values count should be at least 1
        if (distinctValuesCount == 0 && nullsCount < partitionStatistics.basicStatistics().getRowCount().orElse(0)) {
            distinctValuesCount = 1;
        }

        // Hive can produce distinct values that are much larger than the actual number of rows in the partition
        distinctValuesCount = min(distinctValuesCount, partitionStatistics.basicStatistics().getRowCount().orElse(Long.MAX_VALUE) - nullsCount);
        return OptionalLong.of(distinctValuesCount);
    }

    @VisibleForTesting
    static Estimate calculateNullsFraction(String column, Collection<PartitionStatistics> partitionStatistics)
    {
        List<PartitionStatistics> statisticsWithKnownRowCountAndNullsCount = partitionStatistics.stream()
                .filter(statistics -> {
                    if (statistics.basicStatistics().getRowCount().isEmpty()) {
                        return false;
                    }
                    HiveColumnStatistics columnStatistics = statistics.columnStatistics().get(column);
                    if (columnStatistics == null) {
                        return false;
                    }
                    return columnStatistics.getNullsCount().isPresent();
                })
                .collect(toImmutableList());

        if (statisticsWithKnownRowCountAndNullsCount.isEmpty()) {
            return Estimate.unknown();
        }

        long totalNullsCount = 0;
        long totalRowCount = 0;
        for (PartitionStatistics statistics : statisticsWithKnownRowCountAndNullsCount) {
            long rowCount = statistics.basicStatistics().getRowCount().orElseThrow(() -> new VerifyException("rowCount is not present"));
            verify(rowCount >= 0, "rowCount must be greater than or equal to zero");
            HiveColumnStatistics columnStatistics = statistics.columnStatistics().get(column);
            verifyNotNull(columnStatistics, "columnStatistics is null");
            long nullsCount = columnStatistics.getNullsCount().orElseThrow(() -> new VerifyException("nullsCount is not present"));
            verify(nullsCount >= 0, "nullsCount must be greater than or equal to zero");
            verify(nullsCount <= rowCount, "nullsCount must be less than or equal to rowCount. nullsCount: %s. rowCount: %s.", nullsCount, rowCount);
            totalNullsCount += nullsCount;
            totalRowCount += rowCount;
        }

        if (totalRowCount == 0) {
            return Estimate.zero();
        }

        verify(
                totalNullsCount <= totalRowCount,
                "totalNullsCount must be less than or equal to totalRowCount. totalNullsCount: %s. totalRowCount: %s.",
                totalNullsCount,
                totalRowCount);
        return Estimate.of(((double) totalNullsCount) / totalRowCount);
    }

    @VisibleForTesting
    static Estimate calculateDataSize(String column, Collection<PartitionStatistics> partitionStatistics, double totalRowCount)
    {
        List<PartitionStatistics> statisticsWithKnownRowCountAndDataSize = partitionStatistics.stream()
                .filter(statistics -> {
                    if (statistics.basicStatistics().getRowCount().isEmpty()) {
                        return false;
                    }
                    HiveColumnStatistics columnStatistics = statistics.columnStatistics().get(column);
                    if (columnStatistics == null) {
                        return false;
                    }
                    return columnStatistics.getAverageColumnLength().isPresent();
                })
                .collect(toImmutableList());

        if (statisticsWithKnownRowCountAndDataSize.isEmpty()) {
            return Estimate.unknown();
        }

        long knownRowCount = 0;
        double knownDataSize = 0;
        for (PartitionStatistics statistics : statisticsWithKnownRowCountAndDataSize) {
            long rowCount = statistics.basicStatistics().getRowCount().orElseThrow(() -> new VerifyException("rowCount is not present"));
            verify(rowCount >= 0, "rowCount must be greater than or equal to zero");

            HiveColumnStatistics columnStatistics = statistics.columnStatistics().get(column);
            verifyNotNull(columnStatistics, "columnStatistics is null");

            long nullCount = columnStatistics.getNullsCount().orElse(0);
            verify(nullCount >= 0, "nullCount must be greater than or equal to zero");
            long nonNullRowCount = max(rowCount - nullCount, 0);

            double averageColumnLength = columnStatistics.getAverageColumnLength().orElseThrow(() -> new VerifyException("averageColumnLength is not present"));
            verify(averageColumnLength >= 0, "averageColumnLength must be greater than or equal to zero");
            knownRowCount += rowCount;
            knownDataSize += averageColumnLength * nonNullRowCount;
        }

        if (totalRowCount <= 0) {
            return Estimate.zero();
        }

        if (knownRowCount <= 0) {
            return Estimate.unknown();
        }

        double averageValueDataSizeInBytes = knownDataSize / knownRowCount;
        return Estimate.of(averageValueDataSizeInBytes * totalRowCount);
    }

    @VisibleForTesting
    static Optional<DoubleRange> calculateRange(Type type, List<HiveColumnStatistics> columnStatistics)
    {
        return columnStatistics.stream()
                .map(statistics -> createRange(type, statistics))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce(DoubleRange::union);
    }

    private static Optional<DoubleRange> createRange(Type type, HiveColumnStatistics statistics)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return statistics.getIntegerStatistics().flatMap(integerStatistics -> createIntegerRange(type, integerStatistics));
        }
        if (type.equals(DOUBLE) || type.equals(REAL)) {
            return statistics.getDoubleStatistics().flatMap(AbstractHiveStatisticsProvider::createDoubleRange);
        }
        if (type.equals(DATE)) {
            return statistics.getDateStatistics().flatMap(AbstractHiveStatisticsProvider::createDateRange);
        }
        if (type instanceof DecimalType) {
            return statistics.getDecimalStatistics().flatMap(AbstractHiveStatisticsProvider::createDecimalRange);
        }
        return Optional.empty();
    }

    private static Optional<DoubleRange> createIntegerRange(Type type, IntegerStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent()) {
            return Optional.of(createIntegerRange(type, statistics.getMin().getAsLong(), statistics.getMax().getAsLong()));
        }
        return Optional.empty();
    }

    private static DoubleRange createIntegerRange(Type type, long min, long max)
    {
        return new DoubleRange(normalizeIntegerValue(type, min), normalizeIntegerValue(type, max));
    }

    private static long normalizeIntegerValue(Type type, long value)
    {
        if (type.equals(BIGINT)) {
            return value;
        }
        if (type.equals(INTEGER)) {
            return Ints.saturatedCast(value);
        }
        if (type.equals(SMALLINT)) {
            return Shorts.saturatedCast(value);
        }
        if (type.equals(TINYINT)) {
            return SignedBytes.saturatedCast(value);
        }
        throw new IllegalArgumentException("Unexpected type: " + type);
    }

    private static Optional<DoubleRange> createDoubleRange(DoubleStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent() && !isNaN(statistics.getMin().getAsDouble()) && !isNaN(statistics.getMax().getAsDouble())) {
            return Optional.of(new DoubleRange(statistics.getMin().getAsDouble(), statistics.getMax().getAsDouble()));
        }
        return Optional.empty();
    }

    private static Optional<DoubleRange> createDateRange(DateStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent()) {
            return Optional.of(new DoubleRange(statistics.getMin().get().toEpochDay(), statistics.getMax().get().toEpochDay()));
        }
        return Optional.empty();
    }

    private static Optional<DoubleRange> createDecimalRange(DecimalStatistics statistics)
    {
        if (statistics.getMin().isPresent() && statistics.getMax().isPresent()) {
            return Optional.of(new DoubleRange(statistics.getMin().get().doubleValue(), statistics.getMax().get().doubleValue()));
        }
        return Optional.empty();
    }
}
