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

package io.trino.plugin.tpcds.statistics;

import io.trino.plugin.tpcds.TpcdsColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.tpcds.Table;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Double.parseDouble;

public class TpcdsTableStatisticsFactory
{
    private final TableStatisticsDataRepository statisticsDataRepository = new TableStatisticsDataRepository();

    public TableStatistics create(String schemaName, Table table, Map<String, ColumnHandle> columnHandles)
    {
        Optional<TableStatisticsData> statisticsDataOptional = statisticsDataRepository.load(schemaName, table);
        return statisticsDataOptional.map(statisticsData -> toTableStatistics(columnHandles, statisticsData))
                .orElse(TableStatistics.empty());
    }

    private TableStatistics toTableStatistics(Map<String, ColumnHandle> columnHandles, TableStatisticsData statisticsData)
    {
        long rowCount = statisticsData.rowCount();
        TableStatistics.Builder tableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount));

        if (rowCount > 0) {
            Map<String, ColumnStatisticsData> columnsData = statisticsData.columns();
            for (Map.Entry<String, ColumnHandle> entry : columnHandles.entrySet()) {
                TpcdsColumnHandle columnHandle = (TpcdsColumnHandle) entry.getValue();
                tableStatistics.setColumnStatistics(entry.getValue(), toColumnStatistics(columnsData.get(entry.getKey()), columnHandle.type(), rowCount));
            }
        }

        return tableStatistics.build();
    }

    private ColumnStatistics toColumnStatistics(ColumnStatisticsData columnStatisticsData, Type type, long rowCount)
    {
        ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
        long nullCount = columnStatisticsData.nullsCount();
        columnStatistics.setNullsFraction(Estimate.of((double) nullCount / rowCount));
        columnStatistics.setRange(toRange(columnStatisticsData.min(), columnStatisticsData.max(), type));
        columnStatistics.setDistinctValuesCount(Estimate.of(columnStatisticsData.distinctValuesCount()));
        columnStatistics.setDataSize(columnStatisticsData.dataSize().map(Estimate::of).orElse(Estimate.unknown()));
        return columnStatistics.build();
    }

    private static Optional<DoubleRange> toRange(Optional<Object> min, Optional<Object> max, Type columnType)
    {
        if (columnType instanceof VarcharType || columnType instanceof CharType || columnType instanceof TimeType) {
            return Optional.empty();
        }
        if (min.isEmpty() || max.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DoubleRange(toDouble(min.get(), columnType), toDouble(max.get(), columnType)));
    }

    private static double toDouble(Object value, Type type)
    {
        if (value instanceof String string && type.equals(DATE)) {
            return LocalDate.parse(string).toEpochDay();
        }
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(DATE)) {
            return ((Number) value).doubleValue();
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return parseDouble(Decimals.toString(((Number) value).longValue(), decimalType.getScale()));
            }
            return parseDouble(Decimals.toString((Int128) value, decimalType.getScale()));
        }
        if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        throw new IllegalArgumentException("unsupported column type " + type);
    }
}
