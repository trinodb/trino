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

import static io.trino.spi.type.DoubleType.DOUBLE;

@AggregationFunction
public final class DoubleRegressionAggregation
{
    private DoubleRegressionAggregation() {}

    @InputFunction
    public static void input(@AggregationState RegressionState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        state.update(independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(@AggregationState RegressionState state, @AggregationState RegressionState otherState)
    {
        state.merge(otherState);
    }

    @AggregationFunction("regr_slope")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrSlope(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionSlope();
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction("regr_intercept")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void regrIntercept(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionIntercept();
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }
}
