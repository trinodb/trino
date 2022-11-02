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
package io.trino.plugin.ml;

import io.airlift.slice.Slices;
import io.trino.plugin.ml.type.RegressorType;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.DOUBLE;

@AggregationFunction(value = "learn_regressor", decomposable = false)
public final class LearnRegressorAggregation
{
    private LearnRegressorAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState LearnState state,
            @SqlType(BIGINT) long label,
            @SqlType("map(bigint,double)") Block features)
    {
        input(state, (double) label, features);
    }

    @InputFunction
    public static void input(
            @AggregationState LearnState state,
            @SqlType(DOUBLE) double label,
            @SqlType("map(bigint,double)") Block features)
    {
        LearnLibSvmRegressorAggregation.input(state, label, features, Slices.utf8Slice(""));
    }

    @OutputFunction(RegressorType.NAME)
    public static void output(@AggregationState LearnState state, BlockBuilder out)
    {
        LearnLibSvmRegressorAggregation.output(state, out);
    }
}
