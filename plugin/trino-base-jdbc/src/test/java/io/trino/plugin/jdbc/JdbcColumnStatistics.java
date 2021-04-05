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

import io.trino.plugin.base.statistics.IntegerStatistics;
import io.trino.spi.block.Block;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.base.statistics.Estimates.toEstimate;
import static io.trino.plugin.base.statistics.ReduceOperator.ADD;
import static io.trino.plugin.base.statistics.Statistics.getIntegerValue;
import static io.trino.plugin.base.statistics.Statistics.mergeIntegerStatistics;
import static io.trino.plugin.base.statistics.Statistics.reduce;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class JdbcColumnStatistics
{
    private final Optional<IntegerStatistics> integerStatistics;
    private final OptionalLong nonNullsCount;
    private final OptionalLong distinctValuesCount;

    private JdbcColumnStatistics(
            Optional<IntegerStatistics> integerStatistics,
            OptionalLong nonNullsCount,
            OptionalLong distinctValuesCount)
    {
        this.integerStatistics = requireNonNull(integerStatistics, "integerStatistics is null");
        this.nonNullsCount = requireNonNull(nonNullsCount, "nullsCount is null");
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
    }

    public Optional<IntegerStatistics> getIntegerStatistics()
    {
        return integerStatistics;
    }

    public Estimate getDistinctValuesCount()
    {
        return toEstimate(distinctValuesCount);
    }

    public Estimate getNonNullsCount()
    {
        return toEstimate(nonNullsCount);
    }

    public static JdbcColumnStatistics from(Map<ColumnStatisticType, Block> statistics, Type type)
    {
        requireNonNull(statistics, "statistics is null");
        requireNonNull(type, "type is null");

        Optional<IntegerStatistics> integerStatistics = Optional.empty();
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            integerStatistics = Optional.of(new IntegerStatistics(getIntegerValue(type, statistics.get(MIN_VALUE)), getIntegerValue(type, statistics.get(MAX_VALUE))));
        }
        return new JdbcColumnStatistics(
                integerStatistics,
                getIntegerValue(BIGINT, statistics.get(NUMBER_OF_NON_NULL_VALUES)),
                getIntegerValue(BIGINT, statistics.get(NUMBER_OF_DISTINCT_VALUES)));
    }

    public static JdbcColumnStatistics merge(JdbcColumnStatistics first, JdbcColumnStatistics second)
    {
        return new JdbcColumnStatistics(
                mergeIntegerStatistics(first.integerStatistics, second.integerStatistics),
                reduce(first.nonNullsCount, second.nonNullsCount, ADD, true),
                reduce(first.distinctValuesCount, second.distinctValuesCount, ADD, true));
    }
}
