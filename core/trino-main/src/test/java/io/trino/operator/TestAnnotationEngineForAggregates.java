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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InternalFunctionDependencies;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.ParametricAggregation;
import io.trino.operator.aggregation.ParametricAggregationImplementation;
import io.trino.operator.aggregation.state.LongState;
import io.trino.operator.aggregation.state.NullableDoubleState;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.TriStateBooleanState;
import io.trino.operator.aggregation.state.VarianceState;
import io.trino.operator.annotations.LiteralImplementationDependency;
import io.trino.operator.annotations.OperatorImplementationDependency;
import io.trino.operator.annotations.TypeImplementationDependency;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.TypeParameterSpecialization;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.Constraint;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.aggregation.AggregationFromAnnotationsParser.parseFunctionDefinitions;
import static io.trino.operator.aggregation.AggregationFromAnnotationsParser.toAccumulatorStateDetails;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.invoke.MethodType.methodType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAnnotationEngineForAggregates
        extends TestAnnotationEngine
{
    private static final MetadataManager METADATA = createTestMetadataManager();
    private static final FunctionManager FUNCTION_MANAGER = createTestingFunctionManager();
    private static final FunctionDependencies NO_FUNCTION_DEPENDENCIES = new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of());

    @AggregationFunction("simple_exact_aggregate")
    @Description("Simple exact aggregate description")
    public static final class ExactAggregationFunction
    {
        @InputFunction
        public static void input(@AggregationState NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testSimpleExactAggregationParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("simple_exact_aggregate")
                .returnType(DoubleType.DOUBLE)
                .argumentType(DoubleType.DOUBLE)
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(ExactAggregationFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple exact aggregate description");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();
        assertImplementationCount(implementations, 1, 0, 0);
        ParametricAggregationImplementation implementation = getOnlyElement(implementations.getExactImplementations().values());
        assertEquals(implementation.getDefinitionClass(), ExactAggregationFunction.class);
        assertDependencyCount(implementation, 0, 0, 0);
        assertFalse(implementation.hasSpecializedTypeParameters());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(expectedSignature.getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("input_parameters_wrong_order")
    @Description("AggregationState must be the first input parameter")
    public static final class InputParametersWrongOrder
    {
        @InputFunction
        public static void input(@SqlType(DOUBLE) double value, @AggregationState NullableDoubleState state)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testInputParameterOrderEnforced()
    {
        assertThatThrownBy(() -> parseFunctionDefinitions(InputParametersWrongOrder.class))
                .hasMessage("Expected input function non-dependency parameters to begin with state types [NullableDoubleState]: " +
                        "public static void io.trino.operator.TestAnnotationEngineForAggregates$InputParametersWrongOrder.input(double,io.trino.operator.aggregation.state.NullableDoubleState)");
    }

    @AggregationFunction("output_parameters_wrong_order")
    @Description("AggregationState must be the first output parameter")
    public static final class OutputParametersWrongOrder
    {
        @InputFunction
        public static void input(@AggregationState NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(BlockBuilder out, @AggregationState NullableDoubleState state)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testOutputParameterOrderEnforced()
    {
        assertThatThrownBy(() -> parseFunctionDefinitions(OutputParametersWrongOrder.class))
                .hasMessage("Expected output function non-dependency parameters to be [NullableDoubleState, BlockBuilder]: " +
                        "public static void io.trino.operator.TestAnnotationEngineForAggregates$OutputParametersWrongOrder.output(io.trino.spi.block.BlockBuilder,io.trino.operator.aggregation.state.NullableDoubleState)");
    }

    @AggregationFunction("no_aggregation_state_aggregate")
    @Description("Aggregate with no @AggregationState annotations")
    public static final class NotAnnotatedAggregateStateAggregationFunction
    {
        @InputFunction
        public static void input(NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(NullableDoubleState combine1, NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testNotAnnotatedAggregateStateAggregationParse()
    {
        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(NotAnnotatedAggregateStateAggregationFunction.class));

        ParametricAggregationImplementation implementation = getOnlyElement(aggregation.getImplementations().getExactImplementations().values());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction(value = "custom_decomposable_aggregate", decomposable = false)
    @Description("Aggregate with Decomposable=false")
    public static final class NotDecomposableAggregationFunction
    {
        @InputFunction
        public static void input(
                @AggregationState NullableDoubleState state,
                @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testNotDecomposableAggregationParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("custom_decomposable_aggregate")
                .returnType(DoubleType.DOUBLE)
                .argumentType(DoubleType.DOUBLE)
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(NotDecomposableAggregationFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Aggregate with Decomposable=false");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertTrue(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("simple_generic_implementations")
    @Description("Simple aggregate with two generic implementations")
    public static final class GenericAggregationFunction
    {
        @InputFunction
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableLongState state,
                @SqlType("T") double value)
        {
            // noop this is only for annotation testing puproses
        }

        @InputFunction
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableLongState state,
                @SqlType("T") long value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableLongState state,
                @AggregationState NullableLongState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableLongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testSimpleGenericAggregationFunctionParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("simple_generic_implementations")
                .typeVariable("T")
                .returnType(new TypeSignature("T"))
                .argumentType(new TypeSignature("T"))
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(GenericAggregationFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple aggregate with two generic implementations");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        assertEquals(aggregation.getStateDetails(), ImmutableList.of(toAccumulatorStateDetails(NullableLongState.class, ImmutableList.of())));
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();
        assertImplementationCount(implementations, 0, 0, 2);
        ParametricAggregationImplementation implementationDouble = implementations.getGenericImplementations().stream()
                .filter(impl -> impl.getInputFunction().type().equals(methodType(void.class, NullableLongState.class, double.class)))
                .collect(toImmutableList())
                .get(0);
        assertEquals(implementationDouble.getDefinitionClass(), GenericAggregationFunction.class);
        assertDependencyCount(implementationDouble, 0, 0, 0);
        assertFalse(implementationDouble.hasSpecializedTypeParameters());
        assertEquals(implementationDouble.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        ParametricAggregationImplementation implementationLong = implementations.getGenericImplementations().stream()
                .filter(impl -> impl.getInputFunction().type().equals(methodType(void.class, NullableLongState.class, long.class)))
                .collect(toImmutableList())
                .get(0);
        assertEquals(implementationLong.getDefinitionClass(), GenericAggregationFunction.class);
        assertDependencyCount(implementationLong, 0, 0, 0);
        assertFalse(implementationLong.hasSpecializedTypeParameters());
        assertEquals(implementationLong.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("block_input_aggregate")
    @Description("Simple aggregate with @BlockPosition usage")
    public static final class BlockInputAggregationFunction
    {
        @InputFunction
        public static void input(
                @AggregationState NullableDoubleState state,
                @BlockPosition @SqlType(DOUBLE) Block value,
                @BlockIndex int id)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableDoubleState combine1,
                @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testSimpleBlockInputAggregationParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("block_input_aggregate")
                .returnType(DoubleType.DOUBLE)
                .argumentType(DoubleType.DOUBLE)
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(BlockInputAggregationFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple aggregate with @BlockPosition usage");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();
        assertImplementationCount(implementations, 1, 0, 0);
        ParametricAggregationImplementation implementation = getOnlyElement(implementations.getExactImplementations().values());
        assertEquals(implementation.getDefinitionClass(), BlockInputAggregationFunction.class);
        assertDependencyCount(implementation, 0, 0, 0);
        assertFalse(implementation.hasSpecializedTypeParameters());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, BLOCK_INPUT_CHANNEL, BLOCK_INDEX));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("implicit_specialized_aggregate")
    @Description("Simple implicit specialized aggregate")
    public static final class ImplicitSpecializedAggregationFunction
    {
        @InputFunction
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableDoubleState state,
                @SqlType("array(T)") Block arrayBlock, @SqlType("T") double additionalValue)
        {
            // noop this is only for annotation testing puproses
        }

        @InputFunction
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableLongState state,
                @SqlType("array(T)") Block arrayBlock, @SqlType("T") long additionalValue)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableLongState state,
                @AggregationState NullableLongState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableDoubleState state,
                @AggregationState NullableDoubleState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableLongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test(enabled = false) // TODO this is not yet supported
    public void testSimpleImplicitSpecializedAggregationParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("implicit_specialized_aggregate")
                .typeVariable("T")
                .returnType(new TypeSignature("T"))
                .argumentType(arrayType(new TypeSignature("T")))
                .argumentType(new TypeSignature("T"))
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(ImplicitSpecializedAggregationFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple implicit specialized aggregate");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();
        assertImplementationCount(implementations, 0, 0, 2);

        ParametricAggregationImplementation implementation1 = implementations.getSpecializedImplementations().get(0);
        assertTrue(implementation1.hasSpecializedTypeParameters());
        assertFalse(implementation1.hasSpecializedTypeParameters());
        assertEquals(implementation1.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL, INPUT_CHANNEL));

        ParametricAggregationImplementation implementation2 = implementations.getSpecializedImplementations().get(1);
        assertTrue(implementation2.hasSpecializedTypeParameters());
        assertFalse(implementation2.hasSpecializedTypeParameters());
        assertEquals(implementation2.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(new ArrayType(DoubleType.DOUBLE)));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("explicit_specialized_aggregate")
    @Description("Simple explicit specialized aggregate")
    public static final class ExplicitSpecializedAggregationFunction
    {
        @InputFunction
        @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableDoubleState state,
                @SqlType("array(T)") Block arrayBlock)
        {
            // noop this is only for annotation testing puproses
        }

        @InputFunction
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableLongState state,
                @SqlType("array(T)") Block arrayBlock)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableLongState state,
                @AggregationState NullableLongState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableDoubleState state,
                @AggregationState NullableDoubleState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableLongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test(enabled = false) // TODO this is not yet supported
    public void testSimpleExplicitSpecializedAggregationParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("explicit_specialized_aggregate")
                .typeVariable("T")
                .returnType(new TypeSignature("T"))
                .argumentType(arrayType(new TypeSignature("T")))
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(ExplicitSpecializedAggregationFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple explicit specialized aggregate");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();
        assertImplementationCount(implementations, 0, 1, 1);
        ParametricAggregationImplementation implementation1 = implementations.getSpecializedImplementations().get(0);
        assertTrue(implementation1.hasSpecializedTypeParameters());
        assertFalse(implementation1.hasSpecializedTypeParameters());
        assertEquals(implementation1.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        ParametricAggregationImplementation implementation2 = implementations.getSpecializedImplementations().get(1);
        assertTrue(implementation2.hasSpecializedTypeParameters());
        assertFalse(implementation2.hasSpecializedTypeParameters());
        assertEquals(implementation2.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(new ArrayType(DoubleType.DOUBLE)));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("multi_output_aggregate")
    @Description("Simple multi output function aggregate generic description")
    public static final class MultiOutputAggregationFunction
    {
        @InputFunction
        public static void input(
                @AggregationState NullableDoubleState state,
                @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableDoubleState combine1,
                @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationFunction("multi_output_aggregate_1")
        @Description("Simple multi output function aggregate specialized description")
        @OutputFunction(DOUBLE)
        public static void output1(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationFunction("multi_output_aggregate_2")
        @OutputFunction(DOUBLE)
        public static void output2(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testMultiOutputAggregationParse()
    {
        Signature expectedSignature1 = Signature.builder()
                .name("multi_output_aggregate_1")
                .returnType(DoubleType.DOUBLE)
                .argumentType(DoubleType.DOUBLE)
                .build();

        Signature expectedSignature2 = Signature.builder()
                .name("multi_output_aggregate_2")
                .returnType(DoubleType.DOUBLE)
                .argumentType(DoubleType.DOUBLE)
                .build();

        List<ParametricAggregation> aggregations = parseFunctionDefinitions(MultiOutputAggregationFunction.class);
        assertEquals(aggregations.size(), 2);

        ParametricAggregation aggregation1 = aggregations.stream().filter(aggregate -> aggregate.getFunctionMetadata().getSignature().getName().equals("multi_output_aggregate_1")).collect(toImmutableList()).get(0);
        assertEquals(aggregation1.getFunctionMetadata().getSignature(), expectedSignature1);
        assertEquals(aggregation1.getFunctionMetadata().getDescription(), "Simple multi output function aggregate specialized description");

        ParametricAggregation aggregation2 = aggregations.stream().filter(aggregate -> aggregate.getFunctionMetadata().getSignature().getName().equals("multi_output_aggregate_2")).collect(toImmutableList()).get(0);
        assertEquals(aggregation2.getFunctionMetadata().getSignature(), expectedSignature2);
        assertEquals(aggregation2.getFunctionMetadata().getDescription(), "Simple multi output function aggregate generic description");

        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations1 = aggregation1.getImplementations();
        assertImplementationCount(implementations1, 1, 0, 0);

        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations2 = aggregation2.getImplementations();
        assertImplementationCount(implementations2, 1, 0, 0);

        ParametricAggregationImplementation implementation = getOnlyElement(implementations1.getExactImplementations().values());
        assertEquals(implementation.getDefinitionClass(), MultiOutputAggregationFunction.class);
        assertDependencyCount(implementation, 0, 0, 0);
        assertFalse(implementation.hasSpecializedTypeParameters());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation1.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        AggregationFunctionMetadata aggregationMetadata = aggregation1.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation1.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("inject_operator_aggregate")
    @Description("Simple aggregate with operator injected")
    public static final class InjectOperatorAggregateFunction
    {
        @InputFunction
        public static void input(
                @OperatorDependency(
                        operator = LESS_THAN,
                        argumentTypes = {DOUBLE, DOUBLE},
                        convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL))
                        MethodHandle methodHandle,
                @AggregationState NullableDoubleState state,
                @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @OperatorDependency(
                        operator = LESS_THAN,
                        argumentTypes = {DOUBLE, DOUBLE},
                        convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL))
                        MethodHandle methodHandle,
                @AggregationState NullableDoubleState combine1,
                @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(
                @OperatorDependency(
                        operator = LESS_THAN,
                        argumentTypes = {DOUBLE, DOUBLE},
                        convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL))
                        MethodHandle methodHandle,
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testInjectOperatorAggregateParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("inject_operator_aggregate")
                .returnType(DoubleType.DOUBLE)
                .argumentType(DoubleType.DOUBLE)
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(InjectOperatorAggregateFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple aggregate with operator injected");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();

        ParametricAggregationImplementation implementation = getOnlyElement(implementations.getExactImplementations().values());
        assertEquals(implementation.getDefinitionClass(), InjectOperatorAggregateFunction.class);
        assertDependencyCount(implementation, 1, 1, 1);

        assertTrue(implementation.getInputDependencies().get(0) instanceof OperatorImplementationDependency);
        assertTrue(implementation.getCombineDependencies().get(0) instanceof OperatorImplementationDependency);
        assertTrue(implementation.getOutputDependencies().get(0) instanceof OperatorImplementationDependency);

        assertFalse(implementation.hasSpecializedTypeParameters());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        specializeAggregationFunction(boundSignature, aggregation);
    }

    @AggregationFunction("inject_type_aggregate")
    @Description("Simple aggregate with type injected")
    public static final class InjectTypeAggregateFunction
    {
        @InputFunction
        @TypeParameter("T")
        public static void input(
                @TypeParameter("T") Type type,
                @AggregationState NullableDoubleState state,
                @SqlType("T") double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @TypeParameter("T") Type type,
                @AggregationState NullableDoubleState combine1,
                @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @TypeParameter("T") Type type,
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testInjectTypeAggregateParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("inject_type_aggregate")
                .typeVariable("T")
                .returnType(new TypeSignature("T"))
                .argumentType(new TypeSignature("T"))
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(InjectTypeAggregateFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple aggregate with type injected");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();

        assertEquals(implementations.getGenericImplementations().size(), 1);
        ParametricAggregationImplementation implementation = implementations.getGenericImplementations().get(0);
        assertEquals(implementation.getDefinitionClass(), InjectTypeAggregateFunction.class);
        assertDependencyCount(implementation, 1, 1, 1);

        assertTrue(implementation.getInputDependencies().get(0) instanceof TypeImplementationDependency);
        assertTrue(implementation.getCombineDependencies().get(0) instanceof TypeImplementationDependency);
        assertTrue(implementation.getOutputDependencies().get(0) instanceof TypeImplementationDependency);

        assertFalse(implementation.hasSpecializedTypeParameters());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE));
        specializeAggregationFunction(boundSignature, aggregation);
    }

    @AggregationFunction("inject_literal_aggregate")
    @Description("Simple aggregate with type literal")
    public static final class InjectLiteralAggregateFunction
    {
        @InputFunction
        @LiteralParameters("x")
        public static void input(
                @LiteralParameter("x") Long varcharSize,
                @AggregationState LongState state,
                @SqlType("varchar(x)") Slice slice)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @LiteralParameter("x") Long varcharSize,
                @AggregationState LongState combine1,
                @AggregationState LongState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("varchar(x)")
        public static void output(
                @LiteralParameter("x") Long varcharSize,
                @AggregationState LongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testInjectLiteralAggregateParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("inject_literal_aggregate")
                .returnType(new TypeSignature("varchar", typeVariable("x")))
                .argumentType(new TypeSignature("varchar", typeVariable("x")))
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(InjectLiteralAggregateFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple aggregate with type literal");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();

        assertEquals(implementations.getGenericImplementations().size(), 1);
        ParametricAggregationImplementation implementation = implementations.getGenericImplementations().get(0);
        assertEquals(implementation.getDefinitionClass(), InjectLiteralAggregateFunction.class);
        assertDependencyCount(implementation, 1, 1, 1);

        assertTrue(implementation.getInputDependencies().get(0) instanceof LiteralImplementationDependency);
        assertTrue(implementation.getCombineDependencies().get(0) instanceof LiteralImplementationDependency);
        assertTrue(implementation.getOutputDependencies().get(0) instanceof LiteralImplementationDependency);

        assertFalse(implementation.hasSpecializedTypeParameters());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), createVarcharType(17), ImmutableList.of(createVarcharType(17)));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("parametric_aggregate_long_constraint")
    @Description("Parametric aggregate with parametric type returned")
    public static final class LongConstraintAggregateFunction
    {
        @InputFunction
        @LiteralParameters({"x", "y", "z"})
        @Constraint(variable = "z", expression = "x + y")
        public static void input(
                @AggregationState LongState state,
                @SqlType("varchar(x)") Slice slice1,
                @SqlType("varchar(y)") Slice slice2)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState LongState combine1,
                @AggregationState LongState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("varchar(z)")
        public static void output(
                @AggregationState LongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testLongConstraintAggregateFunctionParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("parametric_aggregate_long_constraint")
                .longVariable("z", "x + y")
                .returnType(new TypeSignature("varchar", typeVariable("z")))
                .argumentType(new TypeSignature("varchar", typeVariable("x")))
                .argumentType(new TypeSignature("varchar", typeVariable("y")))
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(LongConstraintAggregateFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Parametric aggregate with parametric type returned");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();

        assertEquals(implementations.getGenericImplementations().size(), 1);
        ParametricAggregationImplementation implementation = implementations.getGenericImplementations().get(0);
        assertEquals(implementation.getDefinitionClass(), LongConstraintAggregateFunction.class);
        assertDependencyCount(implementation, 0, 0, 0);

        assertFalse(implementation.hasSpecializedTypeParameters());
        assertEquals(implementation.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), createVarcharType(30), ImmutableList.of(createVarcharType(17), createVarcharType(13)));
        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());
        aggregation.specialize(boundSignature, NO_FUNCTION_DEPENDENCIES);
    }

    @AggregationFunction("fixed_type_parameter_injection")
    @Description("Simple aggregate with fixed parameter type injected")
    public static final class FixedTypeParameterInjectionAggregateFunction
    {
        @InputFunction
        public static void input(
                @TypeParameter("ROW(ARRAY(BIGINT),ROW(ROW(CHAR)),BIGINT,MAP(BIGINT,CHAR))") Type type,
                @AggregationState NullableDoubleState state,
                @SqlType("double") double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @TypeParameter("ROW(ARRAY(BIGINT),ROW(ROW(CHAR)),BIGINT,MAP(BIGINT,CHAR))") Type type,
                @AggregationState NullableDoubleState state,
                @AggregationState NullableDoubleState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("double")
        public static void output(
                @TypeParameter("ROW(ARRAY(BIGINT),ROW(ROW(CHAR)),BIGINT,MAP(BIGINT,CHAR))") Type type,
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @Test
    public void testFixedTypeParameterInjectionAggregateFunctionParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("fixed_type_parameter_injection")
                .returnType(DoubleType.DOUBLE.getTypeSignature())
                .argumentType(DoubleType.DOUBLE.getTypeSignature())
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(FixedTypeParameterInjectionAggregateFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple aggregate with fixed parameter type injected");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        assertEquals(aggregation.getStateDetails(), ImmutableList.of(toAccumulatorStateDetails(NullableDoubleState.class, ImmutableList.of())));
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();
        assertImplementationCount(implementations, 1, 0, 0);
        ParametricAggregationImplementation implementationDouble = implementations.getExactImplementations().get(expectedSignature);
        assertEquals(implementationDouble.getDefinitionClass(), FixedTypeParameterInjectionAggregateFunction.class);
        assertDependencyCount(implementationDouble, 1, 1, 1);
        assertFalse(implementationDouble.hasSpecializedTypeParameters());
        assertEquals(implementationDouble.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL));
    }

    @AggregationFunction("partially_fixed_type_parameter_injection")
    @Description("Simple aggregate with fixed parameter type injected")
    public static final class PartiallyFixedTypeParameterInjectionAggregateFunction
    {
        @InputFunction
        @TypeParameter("T1")
        @TypeParameter("T2")
        public static void input(
                @TypeParameter("ROW(ARRAY(T1),ROW(ROW(T2)),CHAR)") Type type,
                @AggregationState NullableDoubleState state,
                @SqlType("T1") double x, @SqlType("T2") double y)
        {
            // noop this is only for annotation testing purposes
        }

        @CombineFunction
        @TypeParameter("T1")
        @TypeParameter("T2")
        public static void combine(
                @TypeParameter("ROW(ARRAY(T1),ROW(ROW(T2)),CHAR)") Type type,
                @AggregationState NullableDoubleState state,
                @AggregationState NullableDoubleState otherState)
        {
            // noop this is only for annotation testing purposes
        }

        @OutputFunction("double")
        @TypeParameter("T1")
        @TypeParameter("T2")
        public static void output(
                @TypeParameter("ROW(ARRAY(T1),ROW(ROW(T2)),CHAR)") Type type,
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing purposes
        }
    }

    @Test
    public void testPartiallyFixedTypeParameterInjectionAggregateFunctionParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("partially_fixed_type_parameter_injection")
                .typeVariable("T1")
                .typeVariable("T2")
                .returnType(DoubleType.DOUBLE)
                .argumentType(new TypeSignature("T1"))
                .argumentType(new TypeSignature("T2"))
                .build();

        ParametricAggregation aggregation = getOnlyElement(parseFunctionDefinitions(PartiallyFixedTypeParameterInjectionAggregateFunction.class));
        assertEquals(aggregation.getFunctionMetadata().getDescription(), "Simple aggregate with fixed parameter type injected");
        assertTrue(aggregation.getFunctionMetadata().isDeterministic());
        assertEquals(aggregation.getFunctionMetadata().getSignature(), expectedSignature);
        assertEquals(aggregation.getStateDetails(), ImmutableList.of(toAccumulatorStateDetails(NullableDoubleState.class, ImmutableList.of())));
        ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = aggregation.getImplementations();
        assertImplementationCount(implementations, 0, 0, 1);
        ParametricAggregationImplementation implementationDouble = getOnlyElement(implementations.getGenericImplementations());
        assertEquals(implementationDouble.getDefinitionClass(), PartiallyFixedTypeParameterInjectionAggregateFunction.class);
        assertDependencyCount(implementationDouble, 1, 1, 1);
        assertFalse(implementationDouble.hasSpecializedTypeParameters());
        assertEquals(implementationDouble.getInputParameterKinds(), ImmutableList.of(STATE, INPUT_CHANNEL, INPUT_CHANNEL));

        BoundSignature boundSignature = new BoundSignature(aggregation.getFunctionMetadata().getSignature().getName(), DoubleType.DOUBLE, ImmutableList.of(DoubleType.DOUBLE, DoubleType.DOUBLE));
        specializeAggregationFunction(boundSignature, aggregation);
    }

    @AggregationFunction
    @Description("Aggregation output function with alias")
    public static final class AggregationOutputFunctionWithAlias
    {
        @InputFunction
        public static void input(@AggregationState VarianceState state, @SqlType(StandardTypes.DOUBLE) double value)
        {
            // noop this is only for annotation testing purposes
        }

        @CombineFunction
        public static void combine(@AggregationState VarianceState state, @AggregationState VarianceState otherState)
        {
            // noop this is only for annotation testing purposes
        }

        @AggregationFunction(value = "aggregation_output", alias = {"aggregation_output_alias_1", "aggregation_output_alias_2"})
        @OutputFunction(StandardTypes.DOUBLE)
        public static void output(@AggregationState VarianceState state, BlockBuilder out)
        {
            // noop this is only for annotation testing purposes
        }
    }

    @AggregationFunction(value = "aggregation", alias = {"aggregation_alias_1", "aggregation_alias_2"})
    @Description("Aggregation function with alias")
    public static final class AggregationFunctionWithAlias
    {
        @InputFunction
        public static void input(@AggregationState TriStateBooleanState state, @SqlType(StandardTypes.BOOLEAN) boolean value)
        {
            // noop this is only for annotation testing purposes
        }

        @CombineFunction
        public static void combine(@AggregationState TriStateBooleanState state, @AggregationState TriStateBooleanState otherState)
        {
            // noop this is only for annotation testing purposes
        }

        @OutputFunction(StandardTypes.BOOLEAN)
        public static void output(@AggregationState TriStateBooleanState state, BlockBuilder out)
        {
            // noop this is only for annotation testing purposes
        }
    }

    @Test
    public void testAggregateFunctionGetCanonicalName()
    {
        List<ParametricAggregation> aggregationOutputFunctions = parseFunctionDefinitions(AggregationOutputFunctionWithAlias.class);
        assertEquals(aggregationOutputFunctions.size(), 3);
        assertEquals(
                aggregationOutputFunctions.stream()
                        .map(aggregateFunction -> aggregateFunction.getFunctionMetadata().getSignature().getName())
                        .collect(toImmutableSet()),
                ImmutableSet.of("aggregation_output", "aggregation_output_alias_1", "aggregation_output_alias_2"));
        assertEquals(
                aggregationOutputFunctions.stream()
                        .map(aggregateFunction -> aggregateFunction.getFunctionMetadata().getCanonicalName())
                        .collect(toImmutableSet()),
                ImmutableSet.of("aggregation_output"));

        List<ParametricAggregation> aggregationFunctions = parseFunctionDefinitions(AggregationFunctionWithAlias.class);
        assertEquals(aggregationFunctions.size(), 3);
        assertEquals(
                aggregationFunctions.stream()
                        .map(aggregateFunction -> aggregateFunction.getFunctionMetadata().getSignature().getName())
                        .collect(toImmutableSet()),
                ImmutableSet.of("aggregation", "aggregation_alias_1", "aggregation_alias_2"));
        assertEquals(
                aggregationFunctions.stream()
                        .map(aggregateFunction -> aggregateFunction.getFunctionMetadata().getCanonicalName())
                        .collect(toImmutableSet()),
                ImmutableSet.of("aggregation"));
    }

    private static void specializeAggregationFunction(BoundSignature boundSignature, SqlAggregationFunction aggregation)
    {
        FunctionMetadata functionMetadata = aggregation.getFunctionMetadata();
        FunctionBinding functionBinding = MetadataManager.toFunctionBinding(functionMetadata.getFunctionId(), boundSignature, functionMetadata.getSignature());

        AggregationFunctionMetadata aggregationMetadata = aggregation.getAggregationMetadata();
        assertFalse(aggregationMetadata.isOrderSensitive());
        assertFalse(aggregationMetadata.getIntermediateTypes().isEmpty());

        ResolvedFunction resolvedFunction = METADATA.resolve(TEST_SESSION, GlobalSystemConnector.CATALOG_HANDLE, functionBinding, functionMetadata, aggregation.getFunctionDependencies(boundSignature));
        FunctionDependencies functionDependencies = new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, resolvedFunction.getTypeDependencies(), resolvedFunction.getFunctionDependencies());
        aggregation.specialize(boundSignature, functionDependencies);
    }
}
