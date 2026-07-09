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

import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.junit.jupiter.api.Test;

import static io.trino.operator.aggregation.AggregationFromAnnotationsParser.parseFunctionDefinitions;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAggregationValidation
{
    @Test
    public void testBoxedInputParameterRejected()
    {
        assertThatThrownBy(() -> parseFunctionDefinitions(BoxedInputParameter.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching(".*contains an input parameter with boxed primitive type Long; " +
                        "a non-nullable aggregation input parameter must use the corresponding primitive type");
    }

    @Test
    public void testPrimitiveInputParameterAccepted()
    {
        // The equivalent function with a primitive input parameter parses cleanly.
        parseFunctionDefinitions(PrimitiveInputParameter.class);
    }

    @AggregationFunction("boxed_input_parameter")
    protected static final class BoxedInputParameter
    {
        private BoxedInputParameter() {}

        @InputFunction
        public static void input(@AggregationState NullableLongState state, @SqlType(StandardTypes.BIGINT) Long value)
        {
            // noop, only for annotation validation
        }

        @CombineFunction
        public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState other)
        {
            // noop, only for annotation validation
        }

        @OutputFunction(StandardTypes.BIGINT)
        public static void output(@AggregationState NullableLongState state, BlockBuilder out)
        {
            // noop, only for annotation validation
        }
    }

    @AggregationFunction("primitive_input_parameter")
    protected static final class PrimitiveInputParameter
    {
        private PrimitiveInputParameter() {}

        @InputFunction
        public static void input(@AggregationState NullableLongState state, @SqlType(StandardTypes.BIGINT) long value)
        {
            // noop, only for annotation validation
        }

        @CombineFunction
        public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState other)
        {
            // noop, only for annotation validation
        }

        @OutputFunction(StandardTypes.BIGINT)
        public static void output(@AggregationState NullableLongState state, BlockBuilder out)
        {
            // noop, only for annotation validation
        }
    }
}
