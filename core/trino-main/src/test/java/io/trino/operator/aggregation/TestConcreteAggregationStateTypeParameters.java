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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import org.junit.jupiter.api.Test;

import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConcreteAggregationStateTypeParameters
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION =
            new TestingFunctionResolution(extractFunctions(FixedMapAggregation.class));

    @Test
    public void testConcreteMapAggregationStateTypeParameters()
    {
        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction("test_fixed_map_agg", fromTypes(BIGINT, BIGINT));
        assertThat(function.getFinalType().getTypeDescriptor().toString()).isEqualTo("map(varchar,bigint)");
        assertThat(function.getIntermediateType().getTypeDescriptor().toString()).isEqualTo("map(varchar,bigint)");

        assertAggregation(
                FUNCTION_RESOLUTION,
                "test_fixed_map_agg",
                fromTypes(BIGINT, BIGINT),
                ImmutableMap.of("11", 1L, "22", 2L, "33", 3L),
                createLongsBlock(11L, 22L, 33L),
                createLongsBlock(1L, 2L, 3L));
    }

    @AggregationFunction("test_fixed_map_agg")
    public static final class FixedMapAggregation
    {
        private FixedMapAggregation() {}

        @InputFunction
        public static void input(
                @AggregationState({"varchar", "bigint"}) MapAggregationState state,
                @SqlType("bigint") long key,
                @BlockPosition @SqlType("bigint") ValueBlock value,
                @BlockIndex int valuePosition)
        {
            BlockBuilder keyBlockBuilder = VARCHAR.createBlockBuilder(null, 1);
            VARCHAR.writeString(keyBlockBuilder, Long.toString(key));
            state.add(keyBlockBuilder.buildValueBlock(), 0, value, valuePosition);
        }

        @CombineFunction
        public static void combine(
                @AggregationState({"varchar", "bigint"}) MapAggregationState state,
                @AggregationState({"varchar", "bigint"}) MapAggregationState otherState)
        {
            state.merge(otherState);
        }

        @OutputFunction("map(varchar, bigint)")
        public static void output(
                @AggregationState({"varchar", "bigint"}) MapAggregationState state,
                BlockBuilder out)
        {
            state.writeAll((MapBlockBuilder) out);
        }
    }
}
