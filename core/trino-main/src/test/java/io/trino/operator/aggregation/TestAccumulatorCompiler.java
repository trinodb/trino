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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.TimestampType;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.util.Reflection.methodHandle;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAccumulatorCompiler
{
    @Test
    public void testAccumulatorCompilerForTypeSpecificObjectParameter()
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(TestAccumulatorCompiler.class.getClassLoader());

        TimestampType parameterType = TimestampType.TIMESTAMP_NANOS;
        assertThat(parameterType.getJavaType()).isEqualTo(LongTimestamp.class);

        Class<? extends AccumulatorState> stateInterface = LongTimestampAggregation.State.class;
        AccumulatorStateSerializer<?> stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateInterface, classLoader);

        MethodHandle inputFunction = methodHandle(LongTimestampAggregation.class, "input", LongTimestampAggregation.State.class, LongTimestamp.class);
        MethodHandle combineFunction = methodHandle(LongTimestampAggregation.class, "combine", LongTimestampAggregation.State.class, LongTimestampAggregation.State.class);
        MethodHandle outputFunction = methodHandle(LongTimestampAggregation.class, "output", LongTimestampAggregation.State.class, BlockBuilder.class);
        AggregationMetadata metadata = new AggregationMetadata(
                "longTimestampAggregation",
                ImmutableList.of(
                        new AggregationMetadata.ParameterMetadata(STATE),
                        new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, parameterType)),
                inputFunction,
                Optional.empty(),
                combineFunction,
                outputFunction,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                RealType.REAL);

        // test if we can compile aggregation
        assertThat(AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader)).isNotNull();

        // TODO test if aggregation actually works...
    }

    public static class LongTimestampAggregation
    {
        public interface State
                extends AccumulatorState {}

        public static void input(State state, LongTimestamp value) {}

        public static void combine(State stateA, State stateB) {}

        public static void output(State state, BlockBuilder blockBuilder) {}
    }
}
