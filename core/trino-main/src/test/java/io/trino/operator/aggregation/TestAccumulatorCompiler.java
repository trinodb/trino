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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.BoundSignature;
import io.trino.operator.aggregation.TestAccumulatorCompiler.LongTimestampAggregation.State;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.STATE;
import static io.trino.util.Reflection.methodHandle;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAccumulatorCompiler
{
    @Test
    public void testAccumulatorCompilerForTypeSpecificObjectParameter()
    {
        TimestampType parameterType = TimestampType.TIMESTAMP_NANOS;
        assertThat(parameterType.getJavaType()).isEqualTo(LongTimestamp.class);

        Class<State> stateInterface = State.class;
        AccumulatorStateSerializer<State> stateSerializer = StateCompiler.generateStateSerializer(stateInterface);
        AccumulatorStateFactory<State> stateFactory = StateCompiler.generateStateFactory(stateInterface);

        MethodHandle inputFunction = methodHandle(LongTimestampAggregation.class, "input", State.class, LongTimestamp.class);
        MethodHandle combineFunction = methodHandle(LongTimestampAggregation.class, "combine", State.class, State.class);
        MethodHandle outputFunction = methodHandle(LongTimestampAggregation.class, "output", State.class, BlockBuilder.class);
        AggregationMetadata metadata = new AggregationMetadata(
                ImmutableList.of(STATE, INPUT_CHANNEL),
                inputFunction,
                Optional.empty(),
                combineFunction,
                outputFunction,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor<>(
                        stateInterface,
                        stateSerializer,
                        stateFactory)));
        BoundSignature signature = new BoundSignature("longTimestampAggregation", RealType.REAL, ImmutableList.of(TimestampType.TIMESTAMP_PICOS));

        // test if we can compile aggregation
        assertThat(AccumulatorCompiler.generateAccumulatorFactoryBinder(signature, metadata)).isNotNull();

        // TODO test if aggregation actually works...
    }

    public static final class LongTimestampAggregation
    {
        private LongTimestampAggregation() {}

        public interface State
                extends AccumulatorState {}

        public static void input(State state, LongTimestamp value) {}

        public static void combine(State stateA, State stateB) {}

        public static void output(State state, BlockBuilder blockBuilder) {}
    }
}
