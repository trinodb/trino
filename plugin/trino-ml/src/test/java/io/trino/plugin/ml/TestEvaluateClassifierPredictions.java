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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.trino.RowPageBuilder;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateClassifierPredictions
{
    @Test
    public void testEvaluateClassifierPredictions()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution(extractFunctions(new MLPlugin().getFunctions()));
        TestingAggregationFunction aggregation = functionResolution.getAggregateFunction("evaluate_classifier_predictions", fromTypes(BIGINT, BIGINT));
        Aggregator aggregator = aggregation.createAggregatorFactory(SINGLE, ImmutableList.of(0, 1), OptionalInt.empty()).createAggregator(new AggregationMetrics());
        aggregator.processPage(getPage());
        BlockBuilder finalOut = VARCHAR.createBlockBuilder(null, 1);
        aggregator.evaluate(finalOut);
        Block block = finalOut.build();

        String output = VARCHAR.getSlice(block, 0).toStringUtf8();
        List<String> parts = ImmutableList.copyOf(Splitter.on('\n').omitEmptyStrings().split(output));
        assertThat(parts.size())
                .describedAs(output)
                .isEqualTo(7);
        assertThat(parts.get(0)).isEqualTo("Accuracy: 1/2 (50.00%)");
    }

    private static Page getPage()
    {
        return RowPageBuilder.rowPageBuilder(BIGINT, BIGINT)
                .row(1L, 1L)
                .row(1L, 0L)
                .build();
    }
}
