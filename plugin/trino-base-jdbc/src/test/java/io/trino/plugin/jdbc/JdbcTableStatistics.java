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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.Block;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.base.statistics.Estimates.toEstimate;
import static io.trino.plugin.base.statistics.ReduceOperator.ADD;
import static io.trino.plugin.base.statistics.Statistics.createColumnToComputedStatisticsMap;
import static io.trino.plugin.base.statistics.Statistics.getIntegerValue;
import static io.trino.plugin.base.statistics.Statistics.reduce;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public final class JdbcTableStatistics
{
    private final OptionalLong rowsCount;
    private final Map<String, JdbcColumnStatistics> columnStatistics;

    private JdbcTableStatistics(OptionalLong rowsCount, Map<String, JdbcColumnStatistics> columnStatistics)
    {
        this.rowsCount = requireNonNull(rowsCount, "rowsCount is null");
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
    }

    public Map<String, JdbcColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    public Estimate getRowsCount()
    {
        return toEstimate(rowsCount);
    }

    public static JdbcTableStatistics from(ComputedStatistics computedStatistics, Map<String, Type> types)
    {
        OptionalLong rowsCount = getIntegerValue(BIGINT, computedStatistics.getTableStatistics().get(ROW_COUNT));
        Map<String, Map<ColumnStatisticType, Block>> columnStatistics = createColumnToComputedStatisticsMap(computedStatistics.getColumnStatistics());

        return new JdbcTableStatistics(
                rowsCount,
                columnStatistics.keySet().stream()
                        .collect(toImmutableMap(
                                Function.identity(),
                                columnName -> {
                                    return JdbcColumnStatistics.from(columnStatistics.get(columnName), types.get(columnName));
                                })));
    }

    public static JdbcTableStatistics merge(JdbcTableStatistics first, JdbcTableStatistics second)
    {
        Map<String, JdbcColumnStatistics> firstColumnStatistics = first.getColumnStatistics();
        Map<String, JdbcColumnStatistics> secondColumnStatistics = second.getColumnStatistics();
        checkArgument(firstColumnStatistics.keySet().equals(secondColumnStatistics.keySet()));

        return new JdbcTableStatistics(
                reduce(first.rowsCount, second.rowsCount, ADD, true),
                firstColumnStatistics.keySet().stream()
                        .collect(toImmutableMap(
                                Function.identity(),
                                columnName -> JdbcColumnStatistics.merge(firstColumnStatistics.get(columnName), secondColumnStatistics.get(columnName)))));
    }
}
