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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import it.unimi.dsi.fastutil.longs.AbstractLongSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.util.FastutilSetHelper.toFastutilHashSet;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFastutilSetHelper
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void testCreateTupleDomainFilters()
    {
        for (TestInput testInput : testInputs()) {
            for (Block inputValues : testInput.getInputs()) {
                Type type = testInput.getType();
                List<Long> values = IntStream.range(0, inputValues.getPositionCount())
                        .mapToLong(index -> type.getLong(inputValues, index))
                        .boxed()
                        .collect(toImmutableList());
                @SuppressWarnings("unchecked")
                Set<Long> filter = (Set<Long>) toFastutilHashSet(
                        ImmutableSet.copyOf(values),
                        type,
                        TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL)),
                        TYPE_OPERATORS.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)));
                assertThat(filter).isInstanceOf(AbstractLongSet.class);
                for (Object value : values) {
                    assertThat(filter.contains((long) value)).isTrue();
                }

                long minValue = ((Number) type.getRange().map(Type.Range::getMin).orElse(Long.MIN_VALUE)).longValue();
                long maxValue = ((Number) type.getRange().map(Type.Range::getMax).orElse(Long.MAX_VALUE)).longValue();
                assertThat(filter.contains(minValue)).isFalse();
                assertThat(filter.contains(maxValue)).isFalse();

                long min = values.get(0);
                long max = min;
                for (int i = 1; i < values.size(); i++) {
                    min = Math.min(min, values.get(i));
                    max = Math.max(max, values.get(i));
                }
                assertThat(filter.contains(min - 1)).isFalse();
                assertThat(filter.contains(max + 1)).isFalse();
            }
        }
    }

    private static List<TestInput> testInputs()
    {
        return ImmutableList.of(
                new TestInput(INTEGER, longInputValues()),
                new TestInput(BIGINT, longInputValues()),
                new TestInput(SMALLINT, longInputValues()),
                new TestInput(TINYINT, ImmutableList.of(ImmutableList.of(-100L, 100L, 120L), ImmutableList.of(100L, 102L, 104L, 105L))),
                new TestInput(REAL, floatInputValues()),
                new TestInput(TIMESTAMP_SECONDS, timestampInputValues(0)),
                new TestInput(TIMESTAMP_MILLIS, timestampInputValues(3)),
                new TestInput(TIMESTAMP_MICROS, timestampInputValues(6)),
                new TestInput(TIME_SECONDS, timeInputValues(0)),
                new TestInput(TIME_MILLIS, timeInputValues(3)),
                new TestInput(TIME_MICROS, timeInputValues(6)),
                new TestInput(TIME_NANOS, timeInputValues(9)),
                new TestInput(TIME_PICOS, timeInputValues(12)),
                new TestInput(DATE, longInputValues()),
                new TestInput(createDecimalType(5, 0), longInputValues()),
                new TestInput(createDecimalType(7, 2), longInputValues()));
    }

    private static List<List<?>> longInputValues()
    {
        return ImmutableList.of(
                ImmutableList.of(-100L, 0L, 1L, 3L, 500L, 7000L),
                ImmutableList.of(1L, 3L, 7L, 12L, 14L));
    }

    private static List<List<?>> floatInputValues()
    {
        return ImmutableList.of(
                ImmutableList.of((long) floatToRawIntBits(-123122.03f), (long) floatToRawIntBits(3424.4f), (long) floatToRawIntBits(989.998f)),
                ImmutableList.of((long) floatToRawIntBits(1.2342f), (long) floatToRawIntBits(3.56f), (long) floatToRawIntBits(7.4f), (long) floatToRawIntBits(12.1f), (long) floatToRawIntBits(14.0f)));
    }

    private static List<List<?>> timeInputValues(int precision)
    {
        return ImmutableList.of(
                        ImmutableList.of(1000_000_000_000L, 1000_000_003_000L, 1000_004_000_000L, 1005_000_000_000L, 1060_000_000_000L),
                        ImmutableList.of(1000_000_000_001L, 1000_000_000_003L, 1000_000_000_007L, 1000_000_000_012L, 1000_000_000_014L))
                .stream()
                .map(inputs -> inputs.stream()
                        .map(value -> SqlTime.newInstance(12, value).roundTo(precision).getPicos())
                        .collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private static List<List<?>> timestampInputValues(int precision)
    {
        return ImmutableList.of(
                        ImmutableList.of(1000_000_000_000L, 1000_000_003_000L, 1000_004_000_000L, 1005_000_000_000L, 1060_000_000_000L),
                        ImmutableList.of(1000_000_000_001L, 1000_000_000_003L, 1000_000_000_007L, 1000_000_000_012L, 1000_000_000_014L))
                .stream()
                .map(inputs -> inputs.stream()
                        .map(value -> SqlTimestamp.newInstance(12, value, 0).roundTo(precision).getEpochMicros())
                        .collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private static class TestInput
    {
        private final Type type;
        private final List<Block> inputs;

        private TestInput(Type type, List<List<?>> inputs)
        {
            this.type = type;
            ImmutableList.Builder<Block> builder = ImmutableList.builder();
            for (List<?> values : inputs) {
                BlockBuilder blockBuilder = type.createBlockBuilder(null, inputs.size());
                for (Object value : values) {
                    writeNativeValue(type, blockBuilder, value);
                }
                builder.add(blockBuilder.build());
            }
            this.inputs = builder.build();
        }

        public Type getType()
        {
            return type;
        }

        public List<Block> getInputs()
        {
            return inputs;
        }
    }
}
