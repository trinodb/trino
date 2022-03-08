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
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionNullability;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.server.PluginManager;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import io.trino.sql.gen.IsolatedClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.normalizeInputMethod;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.util.Reflection.methodHandle;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAccumulatorCompiler
{
    @Test
    public void testAccumulatorCompilerForTypeSpecificObjectParameter()
    {
        TimestampType parameterType = TimestampType.TIMESTAMP_NANOS;
        assertThat(parameterType.getJavaType()).isEqualTo(LongTimestamp.class);
        assertGenerateAccumulator(LongTimestampAggregation.class, LongTimestampAggregationState.class);
    }

    @Test
    public void testAccumulatorCompilerForTypeSpecificObjectParameterSeparateClassLoader()
            throws Exception
    {
        TimestampType parameterType = TimestampType.TIMESTAMP_NANOS;
        assertThat(parameterType.getJavaType()).isEqualTo(LongTimestamp.class);

        ClassLoader pluginClassLoader = PluginManager.createClassLoader("test", ImmutableList.of());
        DynamicClassLoader classLoader = new DynamicClassLoader(pluginClassLoader);
        Class<? extends AccumulatorState> stateInterface = IsolatedClass.isolateClass(
                classLoader,
                AccumulatorState.class,
                LongTimestampAggregationState.class,
                LongTimestampAggregation.class);
        assertThat(stateInterface.getCanonicalName()).isEqualTo(LongTimestampAggregationState.class.getCanonicalName());
        assertThat(stateInterface).isNotSameAs(LongTimestampAggregationState.class);
        Class<?> aggregation = classLoader.loadClass(LongTimestampAggregation.class.getCanonicalName());
        assertThat(aggregation.getCanonicalName()).isEqualTo(LongTimestampAggregation.class.getCanonicalName());
        assertThat(aggregation).isNotSameAs(LongTimestampAggregation.class);

        assertGenerateAccumulator(aggregation, stateInterface);
    }

    private static <S extends AccumulatorState, A> void assertGenerateAccumulator(Class<A> aggregation, Class<S> stateInterface)
    {
        AccumulatorStateSerializer<S> stateSerializer = StateCompiler.generateStateSerializer(stateInterface);
        AccumulatorStateFactory<S> stateFactory = StateCompiler.generateStateFactory(stateInterface);

        BoundSignature signature = new BoundSignature("longTimestampAggregation", RealType.REAL, ImmutableList.of(TIMESTAMP_PICOS));
        MethodHandle inputFunction = methodHandle(aggregation, "input", stateInterface, LongTimestamp.class);
        inputFunction = normalizeInputMethod(inputFunction, signature, STATE, INPUT_CHANNEL);
        MethodHandle combineFunction = methodHandle(aggregation, "combine", stateInterface, stateInterface);
        MethodHandle outputFunction = methodHandle(aggregation, "output", stateInterface, BlockBuilder.class);
        AggregationMetadata metadata = new AggregationMetadata(
                inputFunction,
                Optional.empty(),
                Optional.of(combineFunction),
                outputFunction,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor<>(
                        stateInterface,
                        stateSerializer,
                        stateFactory)));
        FunctionNullability functionNullability = new FunctionNullability(false, ImmutableList.of(false));

        // test if we can compile aggregation
        AccumulatorFactory accumulatorFactory = AccumulatorCompiler.generateAccumulatorFactory(signature, metadata, functionNullability);
        assertThat(accumulatorFactory).isNotNull();
        assertThat(AccumulatorCompiler.generateWindowAccumulatorClass(signature, metadata, functionNullability)).isNotNull();

        TestingAggregationFunction aggregationFunction = new TestingAggregationFunction(
                ImmutableList.of(TIMESTAMP_PICOS),
                ImmutableList.of(BIGINT),
                BIGINT,
                accumulatorFactory);
        assertThat(AggregationTestUtils.aggregation(aggregationFunction, createPage(1234))).isEqualTo(1234L);
    }

    private static Page createPage(int count)
    {
        Block timestampSequenceBlock = createTimestampSequenceBlock(count);
        return new Page(timestampSequenceBlock.getPositionCount(), timestampSequenceBlock);
    }

    private static Block createTimestampSequenceBlock(int count)
    {
        BlockBuilder builder = TIMESTAMP_PICOS.createFixedSizeBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            TIMESTAMP_PICOS.writeObject(builder, new LongTimestamp(i, i));
        }
        return builder.build();
    }
}
