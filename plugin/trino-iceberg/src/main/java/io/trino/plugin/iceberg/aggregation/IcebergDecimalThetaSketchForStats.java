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
package io.trino.plugin.iceberg.aggregation;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;

import static io.trino.plugin.iceberg.aggregation.IcebergThetaSketchForStats.getOrCreateUpdateSketch;
import static java.lang.Long.numberOfLeadingZeros;

/**
 * The decimal variant of {@link IcebergThetaSketchForStats}. It is a separate class, because
 * an aggregation class can have input functions for a single parametric signature only.
 */
@AggregationFunction(value = IcebergThetaSketchForStats.NAME, hidden = true)
public final class IcebergDecimalThetaSketchForStats
{
    private IcebergDecimalThetaSketchForStats() {}

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputShortDecimal(@AggregationState DataSketchState state, @SqlType("decimal(p,s)") long value)
    {
        getOrCreateUpdateSketch(state).update(unscaledValueBytes(value));
    }

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputLongDecimal(@AggregationState DataSketchState state, @SqlType("decimal(p,s)") Int128 value)
    {
        getOrCreateUpdateSketch(state).update(value.toBigInteger().toByteArray());
    }

    // Minimal big-endian two's complement representation, same as BigInteger#toByteArray
    private static byte[] unscaledValueBytes(long unscaledValue)
    {
        int bitLength = Long.SIZE - numberOfLeadingZeros(unscaledValue < 0 ? ~unscaledValue : unscaledValue);
        byte[] bytes = new byte[bitLength / Byte.SIZE + 1];
        for (int i = bytes.length - 1; i >= 0; i--) {
            bytes[i] = (byte) unscaledValue;
            unscaledValue >>= Byte.SIZE;
        }
        return bytes;
    }

    @CombineFunction
    public static void combine(@AggregationState DataSketchState state, @AggregationState DataSketchState otherState)
    {
        IcebergThetaSketchForStats.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState DataSketchState state, BlockBuilder out)
    {
        IcebergThetaSketchForStats.output(state, out);
    }
}
