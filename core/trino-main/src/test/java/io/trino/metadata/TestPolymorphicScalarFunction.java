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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TypeSignature;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.metadata.Signature.comparableWithVariadicBound;
import static io.trino.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_BIGINT_RETURN_VALUE;
import static io.trino.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_VARCHAR_RETURN_VALUE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPolymorphicScalarFunction
{
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Signature SIGNATURE = Signature.builder()
            .name("foo")
            .returnType(BIGINT.getTypeSignature())
            .argumentTypes(new TypeSignature("varchar", typeVariable("x")))
            .build();
    private static final int INPUT_VARCHAR_LENGTH = 10;
    private static final Slice INPUT_SLICE = Slices.allocate(INPUT_VARCHAR_LENGTH);
    private static final BoundSignature BOUND_SIGNATURE = new BoundSignature(SIGNATURE.getName(), BIGINT, ImmutableList.of(createVarcharType(INPUT_VARCHAR_LENGTH)));

    private static final TypeSignature DECIMAL_SIGNATURE = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));

    private static final DecimalType LONG_DECIMAL_BOUND_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1, 2);

    private static final DecimalType SHORT_DECIMAL_BOUND_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION, 2);

    @Test
    public void testSelectsMultipleChoiceWithBlockPosition()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .operatorType(IS_DISTINCT_FROM)
                .argumentTypes(DECIMAL_SIGNATURE, DECIMAL_SIGNATURE)
                .returnType(BOOLEAN.getTypeSignature())
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(signature)
                .argumentNullability(true, true)
                .deterministic(true)
                .choice(choice -> choice
                        .argumentProperties(NULL_FLAG, NULL_FLAG)
                        .implementation(methodsGroup -> methodsGroup
                                .methods("shortShort", "longLong")))
                .choice(choice -> choice
                        .argumentProperties(BLOCK_POSITION, BLOCK_POSITION)
                        .implementation(methodsGroup -> methodsGroup
                                .methodWithExplicitJavaTypes("blockPositionLongLong",
                                        asList(Optional.of(Int128.class), Optional.of(Int128.class)))
                                .methodWithExplicitJavaTypes("blockPositionShortShort",
                                        asList(Optional.of(long.class), Optional.of(long.class)))))
                .build();

        BoundSignature shortDecimalBoundSignature = new BoundSignature(signature.getName(), BOOLEAN, ImmutableList.of(SHORT_DECIMAL_BOUND_TYPE, SHORT_DECIMAL_BOUND_TYPE));
        ChoicesScalarFunctionImplementation functionImplementation = (ChoicesScalarFunctionImplementation) function.specialize(
                shortDecimalBoundSignature,
                new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of()));

        assertEquals(functionImplementation.getChoices().size(), 2);
        assertEquals(
                functionImplementation.getChoices().get(0).getInvocationConvention(),
                new InvocationConvention(ImmutableList.of(NULL_FLAG, NULL_FLAG), FAIL_ON_NULL, false, false));
        assertEquals(
                functionImplementation.getChoices().get(1).getInvocationConvention(),
                new InvocationConvention(ImmutableList.of(BLOCK_POSITION, BLOCK_POSITION), FAIL_ON_NULL, false, false));
        Block block1 = new LongArrayBlock(0, Optional.empty(), new long[0]);
        Block block2 = new LongArrayBlock(0, Optional.empty(), new long[0]);
        assertFalse((boolean) functionImplementation.getChoices().get(1).getMethodHandle().invoke(block1, 0, block2, 0));

        BoundSignature longDecimalBoundSignature = new BoundSignature(signature.getName(), BOOLEAN, ImmutableList.of(LONG_DECIMAL_BOUND_TYPE, LONG_DECIMAL_BOUND_TYPE));
        functionImplementation = (ChoicesScalarFunctionImplementation) function.specialize(
                longDecimalBoundSignature,
                new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of()));
        assertTrue((boolean) functionImplementation.getChoices().get(1).getMethodHandle().invoke(block1, 0, block2, 0));
    }

    @Test
    public void testSelectsMethodBasedOnArgumentTypes()
            throws Throwable
    {
        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("bigintToBigintReturnExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("varcharToBigintReturnExtraParameter")
                                .withExtraParameters(context -> ImmutableList.of(context.getLiteral("x")))))
                .build();

        ChoicesScalarFunctionImplementation functionImplementation = (ChoicesScalarFunctionImplementation) function.specialize(
                BOUND_SIGNATURE,
                new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of()));
        assertEquals(functionImplementation.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE), (long) INPUT_VARCHAR_LENGTH);
    }

    @Test
    public void testSelectsMethodBasedOnReturnType()
            throws Throwable
    {
        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarcharCreateSliceWithExtraParameterLength"))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("varcharToBigintReturnExtraParameter")
                                .withExtraParameters(context -> ImmutableList.of(42))))
                .build();

        ChoicesScalarFunctionImplementation functionImplementation = (ChoicesScalarFunctionImplementation) function.specialize(
                BOUND_SIGNATURE,
                new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of()));

        assertEquals(functionImplementation.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE), VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSameLiteralInArgumentsAndReturnValue()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .returnType(new TypeSignature("varchar", typeVariable("x")))
                .argumentTypes(new TypeSignature("varchar", typeVariable("x")))
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
                .build();

        BoundSignature boundSignature = new BoundSignature(signature.getName(), createVarcharType(INPUT_VARCHAR_LENGTH), ImmutableList.of(createVarcharType(INPUT_VARCHAR_LENGTH)));

        ChoicesScalarFunctionImplementation functionImplementation = (ChoicesScalarFunctionImplementation) function.specialize(
                boundSignature,
                new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of()));
        Slice slice = (Slice) functionImplementation.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice, VARCHAR_TO_VARCHAR_RETURN_VALUE);
    }

    @Test
    public void testTypeParameters()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .typeVariableConstraints(comparableWithVariadicBound("V", "ROW"))
                .returnType(new TypeSignature("V"))
                .argumentTypes(new TypeSignature("V"))
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
                .build();

        BoundSignature boundSignature = new BoundSignature(signature.getName(), VARCHAR, ImmutableList.of(VARCHAR));

        ChoicesScalarFunctionImplementation functionImplementation = (ChoicesScalarFunctionImplementation) function.specialize(
                boundSignature,
                new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of()));
        Slice slice = (Slice) functionImplementation.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice, VARCHAR_TO_VARCHAR_RETURN_VALUE);
    }

    @Test
    public void testSetsHiddenToTrueForOperators()
    {
        Signature signature = Signature.builder()
                .operatorType(ADD)
                .returnType(new TypeSignature("varchar", typeVariable("x")))
                .argumentTypes(new TypeSignature("varchar", typeVariable("x")))
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
                .build();

        BoundSignature boundSignature = new BoundSignature(signature.getName(), createVarcharType(INPUT_VARCHAR_LENGTH), ImmutableList.of(createVarcharType(INPUT_VARCHAR_LENGTH)));
        function.specialize(boundSignature, new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of()));
    }

    @Test
    public void testFailIfNotAllMethodsPresent()
    {
        assertThatThrownBy(() -> new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("bigintToBigintReturnExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup.methods("foo")))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("method foo was not found in class io.trino.metadata.TestPolymorphicScalarFunction\\$TestMethods");
    }

    @Test
    public void testFailNoMethodsAreSelectedWhenExtraParametersFunctionIsSet()
    {
        assertThatThrownBy(() -> new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .withExtraParameters(context -> ImmutableList.of(42))))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("methods must be selected first");
    }

    @Test
    public void testFailIfTwoMethodsWithSameArguments()
    {
        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToBigintReturnFirstExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToBigintReturnExtraParameter")))
                .build();

        assertThatThrownBy(() -> function.specialize(BOUND_SIGNATURE, new FunctionDependencies(METADATA, ImmutableMap.of(), ImmutableSet.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("two matching methods \\(varcharToBigintReturnFirstExtraParameter and varcharToBigintReturnExtraParameter\\) for parameter types \\[varchar\\(10\\)\\]");
    }

    public static final class TestMethods
    {
        static final Slice VARCHAR_TO_VARCHAR_RETURN_VALUE = Slices.utf8Slice("hello world");
        static final long VARCHAR_TO_BIGINT_RETURN_VALUE = 42L;

        public static Slice varcharToVarchar(Slice varchar)
        {
            return VARCHAR_TO_VARCHAR_RETURN_VALUE;
        }

        public static long varcharToBigint(Slice varchar)
        {
            return VARCHAR_TO_BIGINT_RETURN_VALUE;
        }

        public static long varcharToBigintReturnExtraParameter(Slice varchar, long extraParameter)
        {
            return extraParameter;
        }

        public static long bigintToBigintReturnExtraParameter(long bigint, int extraParameter)
        {
            return bigint;
        }

        public static long varcharToBigintReturnFirstExtraParameter(Slice varchar, long extraParameter1, int extraParameter2)
        {
            return extraParameter1;
        }

        public static Slice varcharToVarcharCreateSliceWithExtraParameterLength(Slice string, int extraParameter)
        {
            return Slices.allocate(extraParameter);
        }

        public static boolean blockPositionLongLong(Block left, int leftPosition, Block right, int rightPosition)
        {
            return true;
        }

        public static boolean blockPositionShortShort(Block left, int leftPosition, Block right, int rightPosition)
        {
            return false;
        }

        public static boolean shortShort(long left, boolean leftNull, long right, boolean rightNull)
        {
            return false;
        }

        public static boolean longLong(Int128 left, boolean leftNull, Int128 right, boolean rightNull)
        {
            return false;
        }
    }
}
