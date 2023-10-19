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
package io.trino.plugin.deltalake.transactionlog;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.spi.block.Block;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.toJsonValue;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public final class DeltaLakeComputedStatistics
{
    private DeltaLakeComputedStatistics() {}

    public static DeltaLakeJsonFileStatistics toDeltaLakeJsonFileStatistics(ComputedStatistics stats, Map</* lowercase */ String, DeltaLakeColumnHandle> lowercaseToColumnsHandles)
    {
        Optional<Long> rowCount = getLongValue(stats.getTableStatistics().get(ROW_COUNT)).stream().boxed().findFirst();

        Optional<Map<String, Object>> minValues = Optional.of(getColumnStatistics(stats, MIN_VALUE, lowercaseToColumnsHandles));
        Optional<Map<String, Object>> maxValues = Optional.of(getColumnStatistics(stats, MAX_VALUE, lowercaseToColumnsHandles));

        Optional<Map<String, Object>> nullCount = Optional.empty();
        if (rowCount.isPresent()) {
            nullCount = Optional.of(getNullCount(stats, rowCount.get(), lowercaseToColumnsHandles));
        }

        return new DeltaLakeJsonFileStatistics(rowCount, minValues, maxValues, nullCount);
    }

    private static Map<String, Object> getNullCount(ComputedStatistics statistics, long rowCount, Map</* lowercase */ String, DeltaLakeColumnHandle> lowercaseToColumnsHandles)
    {
        return statistics.getColumnStatistics().entrySet().stream()
                .filter(stats -> stats.getKey().getStatisticType() == NUMBER_OF_NON_NULL_VALUES
                        && lowercaseToColumnsHandles.containsKey(stats.getKey().getColumnName()))
                .map(stats -> Map.entry(lowercaseToColumnsHandles.get(stats.getKey().getColumnName()).getBasePhysicalColumnName(), getLongValue(stats.getValue())))
                .filter(stats -> stats.getValue().isPresent())
                .map(nonNullCount -> Map.entry(nonNullCount.getKey(), rowCount - nonNullCount.getValue().getAsLong()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Object> getColumnStatistics(ComputedStatistics statistics, ColumnStatisticType statisticType, Map</* lowercase */ String, DeltaLakeColumnHandle> lowercaseToColumnsHandles)
    {
        return statistics.getColumnStatistics().entrySet().stream()
                .filter(stats -> stats.getKey().getStatisticType().equals(statisticType)
                        && lowercaseToColumnsHandles.containsKey(stats.getKey().getColumnName()))
                .map(stats -> mapSingleStatisticsValueToJsonRepresentation(stats, lowercaseToColumnsHandles))
                .filter(Optional::isPresent)
                .flatMap(Optional::stream)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Optional<Map.Entry<String, Object>> mapSingleStatisticsValueToJsonRepresentation(Map.Entry<ColumnStatisticMetadata, Block> statistics, Map</* lowercase */ String, DeltaLakeColumnHandle> lowercaseToColumnsHandles)
    {
        Type columnType = lowercaseToColumnsHandles.get(statistics.getKey().getColumnName()).getBasePhysicalType();
        String physicalName = lowercaseToColumnsHandles.get(statistics.getKey().getColumnName()).getBasePhysicalColumnName();
        if (columnType.equals(BOOLEAN) || columnType.equals(VARBINARY)) {
            return Optional.empty();
        }

        Object value = readNativeValue(columnType, statistics.getValue(), 0);
        Object jsonValue = toJsonValue(columnType, value);
        if (jsonValue != null) {
            return Optional.of(Map.entry(physicalName, jsonValue));
        }
        return Optional.empty();
    }

    private static OptionalLong getLongValue(Block block)
    {
        if (block == null || block.isNull(0)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(block.getLong(0, 0));
    }
}
