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

import io.trino.operator.aggregation.state.RegressionState;
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

@AggregationFunction
public final class RealRegressionAggregation
{
    private RealRegressionAggregation() {}

    @InputFunction
    public static void input(@AggregationState RegressionState state, @SqlType(StandardTypes.REAL) long dependentValue, @SqlType(StandardTypes.REAL) long independentValue)
    {
        DoubleRegressionAggregation.input(state, intBitsToFloat((int) dependentValue), intBitsToFloat((int) independentValue));
    }

    @CombineFunction
    public static void combine(@AggregationState RegressionState state, @AggregationState RegressionState otherState)
    {
        DoubleRegressionAggregation.combine(state, otherState);
    }

    @AggregationFunction("regr_slope")
    @OutputFunction(StandardTypes.REAL)
    public static void regrSlope(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionSlope();
        if (Double.isFinite(result)) {
            REAL.writeLong(out, floatToRawIntBits((float) result));
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_intercept")
    @OutputFunction(StandardTypes.REAL)
    public static void regrIntercept(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionIntercept();
        if (Double.isFinite(result)) {
            REAL.writeLong(out, floatToRawIntBits((float) result));
        }
        else {
            out.appendNull();
        }
    }
}
