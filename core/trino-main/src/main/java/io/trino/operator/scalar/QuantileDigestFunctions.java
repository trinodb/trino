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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

public final class QuantileDigestFunctions
{
    public static final double DEFAULT_ACCURACY = 0.01;
    public static final long DEFAULT_WEIGHT = 1L;

    private QuantileDigestFunctions() {}

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType(StandardTypes.DOUBLE)
    public static double valueAtQuantileDouble(@SqlType("qdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return sortableLongToDouble(valueAtQuantileBigint(input, quantile));
    }

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType(StandardTypes.REAL)
    public static long valueAtQuantileReal(@SqlType("qdigest(real)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return floatToRawIntBits(sortableIntToFloat((int) valueAtQuantileBigint(input, quantile)));
    }

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType(StandardTypes.BIGINT)
    public static long valueAtQuantileBigint(@SqlType("qdigest(bigint)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return new QuantileDigest(input).getQuantile(quantile);
    }

    @ScalarFunction("quantile_at_value")
    @Description("Given an input x between min/max values of qdigest, find which quantile is represented by that value")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double quantileAtValueDouble(@SqlType("qdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return quantileAtValueBigint(input, doubleToSortableLong(value));
    }

    @ScalarFunction("quantile_at_value")
    @Description("Given an input x between min/max values of qdigest, find which quantile is represented by that value")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double quantileAtValueReal(@SqlType("qdigest(real)") Slice input, @SqlType(StandardTypes.REAL) long value)
    {
        return quantileAtValueBigint(input, floatToSortableInt(intBitsToFloat((int) value)));
    }

    @ScalarFunction("quantile_at_value")
    @Description("Given an input x between min/max values of qdigest, find which quantile is represented by that value")
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double quantileAtValueBigint(@SqlType("qdigest(bigint)") Slice input, @SqlType(StandardTypes.BIGINT) long value)
    {
        QuantileDigest digest = new QuantileDigest(input);
        if (digest.getCount() == 0 || value > digest.getMax() || value < digest.getMin()) {
            return null;
        }
        double bucketCount = getOnlyElement(digest.getHistogram(ImmutableList.of(value))).getCount();
        return bucketCount / digest.getCount();
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType("array(double)")
    public static Block valuesAtQuantilesDouble(@SqlType("qdigest(double)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        QuantileDigest digest = new QuantileDigest(input);
        BlockBuilder output = DOUBLE.createFixedSizeBlockBuilder(percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            DOUBLE.writeDouble(output, sortableLongToDouble(digest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i))));
        }
        return output.build();
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType("array(real)")
    public static Block valuesAtQuantilesReal(@SqlType("qdigest(real)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        QuantileDigest digest = new QuantileDigest(input);
        BlockBuilder output = REAL.createFixedSizeBlockBuilder(percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            REAL.writeLong(output, floatToRawIntBits(sortableIntToFloat((int) digest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i)))));
        }
        return output.build();
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType("array(bigint)")
    public static Block valuesAtQuantilesBigint(@SqlType("qdigest(bigint)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        QuantileDigest digest = new QuantileDigest(input);
        BlockBuilder output = BIGINT.createFixedSizeBlockBuilder(percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            BIGINT.writeLong(output, digest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i)));
        }
        return output.build();
    }

    public static double verifyAccuracy(double accuracy)
    {
        checkCondition(accuracy > 0 && accuracy < 1, INVALID_FUNCTION_ARGUMENT, () -> String.format("Percentile accuracy must be exclusively between 0 and 1, was %s", accuracy));
        return accuracy;
    }

    public static long verifyWeight(long weight)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, () -> String.format("Percentile weight must be > 0, was %s", weight));
        return weight;
    }
}
