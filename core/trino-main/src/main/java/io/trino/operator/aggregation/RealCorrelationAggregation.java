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

import io.trino.operator.aggregation.state.CorrelationState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("corr")
public final class RealCorrelationAggregation
{
    private RealCorrelationAggregation() {}

    @InputFunction
    public static void input(@AggregationState CorrelationState state, @SqlType(StandardTypes.REAL) long dependentValue, @SqlType(StandardTypes.REAL) long independentValue)
    {
        DoubleCorrelationAggregation.input(state, intBitsToFloat((int) dependentValue), intBitsToFloat((int) independentValue));
    }

    @CombineFunction
    public static void combine(@AggregationState CorrelationState state, @AggregationState CorrelationState otherState)
    {
        DoubleCorrelationAggregation.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.REAL)
    public static void corr(@AggregationState CorrelationState state, BlockBuilder out)
    {
        double result = state.getCorrelation();
        if (Double.isFinite(result)) {
            long resultBits = floatToRawIntBits((float) result);
            REAL.writeLong(out, resultBits);
        }
        else {
            out.appendNull();
        }
    }
}
