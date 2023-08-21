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
package io.trino.orc.metadata.statistics;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

public class TimeMicrosStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    private long nonNullValueCount;
    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;
    private long sum;
    private boolean overflow;

    private final BloomFilterBuilder bloomFilterBuilder;

    public TimeMicrosStatisticsBuilder(BloomFilterBuilder bloomFilterBuilder)
    {
        this.bloomFilterBuilder = requireNonNull(bloomFilterBuilder, "bloomFilterBuilder is null");
    }

    @Override
    public long getValueFromBlock(Type type, Block block, int position)
    {
        return type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND;
    }

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        minimum = Math.min(value, minimum);
        maximum = Math.max(value, maximum);

        if (!overflow) {
            try {
                sum = addExact(sum, value);
            }
            catch (ArithmeticException e) {
                overflow = true;
            }
        }
        bloomFilterBuilder.addLong(value);
    }

    private Optional<IntegerStatistics> buildIntegerStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new IntegerStatistics(minimum, maximum, overflow ? null : sum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<IntegerStatistics> integerStatistics = buildIntegerStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                integerStatistics.map(s -> INTEGER_VALUE_BYTES).orElse(0L),
                null,
                integerStatistics.orElse(null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                bloomFilterBuilder.buildBloomFilter());
    }
}
