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

import io.trino.operator.aggregation.state.CovarianceState;
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
public final class DoubleCovarianceAggregation
{
    private DoubleCovarianceAggregation() {}

    @InputFunction
    public static void input(@AggregationState CovarianceState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        state.update(independentValue, dependentValue);
    }

    @CombineFunction
    public static void combine(@AggregationState CovarianceState state, @AggregationState CovarianceState otherState)
    {
        state.merge(otherState);
    }

    @AggregationFunction("covar_samp")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void covarSamp(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() <= 1) {
            out.appendNull();
        }
        else {
            double result = state.getCovarianceSample();
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction("covar_pop")
    @OutputFunction(StandardTypes.DOUBLE)
    public static void covarPop(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            double result = state.getCovariancePopulation();
            DOUBLE.writeDouble(out, result);
        }
    }
}
