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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.operator.window.InternalWindowIndex;
import io.trino.server.PluginManager;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import io.trino.sql.gen.IsolatedClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Constructor;
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
        AggregationImplementation implementation = AggregationImplementation.builder()
                .inputFunction(inputFunction)
                .combineFunction(combineFunction)
                .outputFunction(outputFunction)
                .accumulatorStateDescriptor(stateInterface, stateSerializer, stateFactory)
                .build();
        FunctionNullability functionNullability = new FunctionNullability(false, ImmutableList.of(false));

        // test if we can compile aggregation
        AccumulatorFactory accumulatorFactory = AccumulatorCompiler.generateAccumulatorFactory(signature, implementation, functionNullability);
        assertThat(accumulatorFactory).isNotNull();

        // compile window aggregation
        Constructor<? extends WindowAccumulator> actual = AccumulatorCompiler.generateWindowAccumulatorClass(signature, implementation, functionNullability);
        assertThat(actual).isNotNull();
        WindowAccumulator windowAccumulator;
        try {
            windowAccumulator = actual.newInstance(ImmutableList.of());
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        // call the functions to ensure that the code does not reference the wrong state
        windowAccumulator.addInput(new TestWindowIndex(), 0, 5);
        windowAccumulator.evaluateFinal(new LongArrayBlockBuilder(null, 1));

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

    private static class TestWindowIndex
            implements InternalWindowIndex
    {
        @Override
        public int size()
        {
            return 10;
        }

        @Override
        public boolean isNull(int channel, int position)
        {
            return false;
        }

        @Override
        public boolean getBoolean(int channel, int position)
        {
            return false;
        }

        @Override
        public long getLong(int channel, int position)
        {
            return 0;
        }

        @Override
        public double getDouble(int channel, int position)
        {
            return 0;
        }

        @Override
        public Slice getSlice(int channel, int position)
        {
            return Slices.EMPTY_SLICE;
        }

        @Override
        public Block getSingleValueBlock(int channel, int position)
        {
            return null;
        }

        @Override
        public Object getObject(int channel, int position)
        {
            return null;
        }

        @Override
        public void appendTo(int channel, int position, BlockBuilder output)
        {
            output.appendNull();
        }

        @Override
        public Block getRawBlock(int channel, int position)
        {
            return new Int96ArrayBlock(1, Optional.empty(), new long[] {0}, new int[] {0});
        }

        @Override
        public int getRawBlockPosition(int position)
        {
            return 0;
        }
    }
}
