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
package io.trino.operator.aggregation;

import io.airlift.stats.QuantileDigest;
import io.trino.operator.aggregation.state.QuantileDigestState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.QuantileDigestType;
import io.trino.spi.type.Type;

import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static io.trino.operator.scalar.QuantileDigestFunctions.DEFAULT_ACCURACY;
import static io.trino.operator.scalar.QuantileDigestFunctions.DEFAULT_WEIGHT;
import static io.trino.operator.scalar.QuantileDigestFunctions.verifyAccuracy;
import static io.trino.operator.scalar.QuantileDigestFunctions.verifyWeight;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;

public final class QuantileDigestAggregationFunction
{
    @AggregationFunction(value = "qdigest_agg", isOrderSensitive = true)
    @Description("Returns a qdigest from the set of doubles")
    public static final class DoubleQuantileDigestAggregationFunction
    {
        private static final QuantileDigestType OUTPUT_TYPE = new QuantileDigestType(DOUBLE);

        private DoubleQuantileDigestAggregationFunction() {}

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("DOUBLE") double value)
        {
            input(state, value, DEFAULT_WEIGHT, DEFAULT_ACCURACY);
        }

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("DOUBLE") double value,
                @SqlType("BIGINT") long weight)
        {
            input(state, value, weight, DEFAULT_ACCURACY);
        }

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("DOUBLE") double value,
                @SqlType("BIGINT") long weight,
                @SqlType("DOUBLE") double accuracy)
        {
            internalInput(state, doubleToSortableLong(value), weight, accuracy);
        }

        @CombineFunction
        public static void combine(@AggregationState QuantileDigestState state, @AggregationState QuantileDigestState otherState)
        {
            internalCombine(state, otherState);
        }

        @OutputFunction("qdigest(DOUBLE)")
        public static void output(@AggregationState QuantileDigestState state, BlockBuilder out)
        {
            internalOutput(OUTPUT_TYPE, state, out);
        }
    }

    @AggregationFunction(value = "qdigest_agg", isOrderSensitive = true)
    @Description("Returns a qdigest from the set of reals")
    public static final class RealQuantileDigestAggregationFunction
    {
        private static final QuantileDigestType OUTPUT_TYPE = new QuantileDigestType(REAL);

        private RealQuantileDigestAggregationFunction() {}

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("REAL") long value)
        {
            input(state, value, DEFAULT_WEIGHT, DEFAULT_ACCURACY);
        }

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("REAL") long value,
                @SqlType("BIGINT") long weight)
        {
            input(state, value, weight, DEFAULT_ACCURACY);
        }

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("REAL") long value,
                @SqlType("BIGINT") long weight,
                @SqlType("DOUBLE") double accuracy)
        {
            internalInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, accuracy);
        }

        @CombineFunction
        public static void combine(@AggregationState QuantileDigestState state, @AggregationState QuantileDigestState otherState)
        {
            internalCombine(state, otherState);
        }

        @OutputFunction("qdigest(REAL)")
        public static void output(@AggregationState QuantileDigestState state, BlockBuilder out)
        {
            internalOutput(OUTPUT_TYPE, state, out);
        }
    }

    @AggregationFunction(value = "qdigest_agg", isOrderSensitive = true)
    @Description("Returns a qdigest from the set of bigints")
    public static final class BigintQuantileDigestAggregationFunction
    {
        private static final QuantileDigestType OUTPUT_TYPE = new QuantileDigestType(BIGINT);

        private BigintQuantileDigestAggregationFunction() {}

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("BIGINT") long value)
        {
            input(state, value, DEFAULT_WEIGHT, DEFAULT_ACCURACY);
        }

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("BIGINT") long value,
                @SqlType("BIGINT") long weight)
        {
            input(state, value, weight, DEFAULT_ACCURACY);
        }

        @InputFunction
        public static void input(
                @AggregationState QuantileDigestState state,
                @SqlType("BIGINT") long value,
                @SqlType("BIGINT") long weight,
                @SqlType("DOUBLE") double accuracy)
        {
            internalInput(state, value, weight, accuracy);
        }

        @CombineFunction
        public static void combine(@AggregationState QuantileDigestState state, @AggregationState QuantileDigestState otherState)
        {
            internalCombine(state, otherState);
        }

        @OutputFunction("qdigest(BIGINT)")
        public static void output(@AggregationState QuantileDigestState state, BlockBuilder out)
        {
            internalOutput(OUTPUT_TYPE, state, out);
        }
    }

    private static void internalInput(
            QuantileDigestState state,
            long value,
            long weight,
            double accuracy)
    {
        QuantileDigest qdigest = getOrCreateQuantileDigest(state, verifyAccuracy(accuracy));
        state.addMemoryUsage(-qdigest.estimatedInMemorySizeInBytes());
        qdigest.add(value, verifyWeight(weight));
        state.addMemoryUsage(qdigest.estimatedInMemorySizeInBytes());
    }

    private static QuantileDigest getOrCreateQuantileDigest(QuantileDigestState state, double accuracy)
    {
        QuantileDigest qdigest = state.getQuantileDigest();
        if (qdigest == null) {
            qdigest = new QuantileDigest(accuracy);
            state.setQuantileDigest(qdigest);
            state.addMemoryUsage(qdigest.estimatedInMemorySizeInBytes());
        }
        return qdigest;
    }

    private static void internalCombine(QuantileDigestState state, QuantileDigestState otherState)
    {
        QuantileDigest input = otherState.getQuantileDigest();

        QuantileDigest previous = state.getQuantileDigest();
        if (previous == null) {
            state.setQuantileDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    private static void internalOutput(Type type, QuantileDigestState state, BlockBuilder out)
    {
        if (state.getQuantileDigest() == null) {
            out.appendNull();
        }
        else {
            type.writeSlice(out, state.getQuantileDigest().serialize());
        }
    }
}
