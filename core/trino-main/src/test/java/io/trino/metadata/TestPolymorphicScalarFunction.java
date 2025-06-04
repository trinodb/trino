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
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TypeSignature;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_BIGINT_RETURN_VALUE;
import static io.trino.metadata.TestPolymorphicScalarFunction.TestMethods.VARCHAR_TO_VARCHAR_RETURN_VALUE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPolymorphicScalarFunction
{
    private static final FunctionManager FUNCTION_MANAGER = createTestingFunctionManager();
    private static final String FUNCTION_NAME = "foo";
    private static final Signature SIGNATURE = Signature.builder()
            .returnType(BIGINT)
            .argumentType(new TypeSignature("varchar", typeVariable("x")))
            .build();
    private static final int INPUT_VARCHAR_LENGTH = 10;
    private static final Slice INPUT_SLICE = Slices.allocate(INPUT_VARCHAR_LENGTH);

    private static final BoundSignature BOUND_SIGNATURE = new BoundSignature(
            builtinFunctionName(FUNCTION_NAME),
            BIGINT,
            ImmutableList.of(createVarcharType(INPUT_VARCHAR_LENGTH)));

    private static final TypeSignature DECIMAL_SIGNATURE = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));

    private static final DecimalType LONG_DECIMAL_BOUND_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1, 2);

    private static final DecimalType SHORT_DECIMAL_BOUND_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION, 2);

    @Test
    public void testSelectsMultipleChoiceWithBlockPosition()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .argumentType(DECIMAL_SIGNATURE)
                .argumentType(DECIMAL_SIGNATURE)
                .returnType(BOOLEAN)
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(IDENTICAL, TestMethods.class)
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

        BoundSignature shortDecimalBoundSignature = new BoundSignature(
                builtinFunctionName(mangleOperatorName(IDENTICAL)),
                BOOLEAN,
                ImmutableList.of(SHORT_DECIMAL_BOUND_TYPE, SHORT_DECIMAL_BOUND_TYPE));
        ChoicesSpecializedSqlScalarFunction specializedFunction = (ChoicesSpecializedSqlScalarFunction) function.specialize(
                shortDecimalBoundSignature,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));

        assertThat(specializedFunction.getChoices()).hasSize(2);
        assertThat(specializedFunction.getChoices().get(0).getInvocationConvention()).isEqualTo(new InvocationConvention(ImmutableList.of(NULL_FLAG, NULL_FLAG), FAIL_ON_NULL, false, false));
        assertThat(specializedFunction.getChoices().get(1).getInvocationConvention()).isEqualTo(new InvocationConvention(ImmutableList.of(BLOCK_POSITION, BLOCK_POSITION), FAIL_ON_NULL, false, false));
        Block block1 = new LongArrayBlock(0, Optional.empty(), new long[0]);
        Block block2 = new LongArrayBlock(0, Optional.empty(), new long[0]);
        assertThat((boolean) specializedFunction.getChoices().get(1).getMethodHandle().invoke(block1, 0, block2, 0)).isFalse();

        BoundSignature longDecimalBoundSignature = new BoundSignature(
                builtinFunctionName(mangleOperatorName(IDENTICAL)),
                BOOLEAN,
                ImmutableList.of(LONG_DECIMAL_BOUND_TYPE, LONG_DECIMAL_BOUND_TYPE));
        specializedFunction = (ChoicesSpecializedSqlScalarFunction) function.specialize(
                longDecimalBoundSignature,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
        assertThat((boolean) specializedFunction.getChoices().get(1).getMethodHandle().invoke(block1, 0, block2, 0)).isTrue();
    }

    @Test
    public void testSelectsMethodBasedOnArgumentTypes()
            throws Throwable
    {
        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(FUNCTION_NAME, TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("bigintToBigintReturnExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("varcharToBigintReturnExtraParameter")
                                .withExtraParameters(context -> ImmutableList.of(context.getLiteral("x")))))
                .build();

        ChoicesSpecializedSqlScalarFunction specializedFunction = (ChoicesSpecializedSqlScalarFunction) function.specialize(
                BOUND_SIGNATURE,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
        assertThat(specializedFunction.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE)).isEqualTo((long) INPUT_VARCHAR_LENGTH);
    }

    @Test
    public void testSelectsMethodBasedOnReturnType()
            throws Throwable
    {
        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(FUNCTION_NAME, TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarcharCreateSliceWithExtraParameterLength"))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("varcharToBigintReturnExtraParameter")
                                .withExtraParameters(context -> ImmutableList.of(42))))
                .build();

        ChoicesSpecializedSqlScalarFunction specializedFunction = (ChoicesSpecializedSqlScalarFunction) function.specialize(
                BOUND_SIGNATURE,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));

        assertThat(specializedFunction.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE)).isEqualTo(VARCHAR_TO_BIGINT_RETURN_VALUE);
    }

    @Test
    public void testSameLiteralInArgumentsAndReturnValue()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .returnType(new TypeSignature("varchar", typeVariable("x")))
                .argumentType(new TypeSignature("varchar", typeVariable("x")))
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(FUNCTION_NAME, TestMethods.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
                .build();

        BoundSignature boundSignature = new BoundSignature(
                builtinFunctionName(FUNCTION_NAME),
                createVarcharType(INPUT_VARCHAR_LENGTH),
                ImmutableList.of(createVarcharType(INPUT_VARCHAR_LENGTH)));

        ChoicesSpecializedSqlScalarFunction specializedFunction = (ChoicesSpecializedSqlScalarFunction) function.specialize(
                boundSignature,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
        Slice slice = (Slice) specializedFunction.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE);
        assertThat(slice).isEqualTo(VARCHAR_TO_VARCHAR_RETURN_VALUE);
    }

    @Test
    public void testTypeParameters()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .typeVariableConstraint(TypeVariableConstraint.builder("V")
                        .comparableRequired()
                        .variadicBound("ROW")
                        .build())
                .returnType(new TypeSignature("V"))
                .argumentType(new TypeSignature("V"))
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(FUNCTION_NAME, TestMethods.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
                .build();

        BoundSignature boundSignature = new BoundSignature(
                builtinFunctionName(FUNCTION_NAME),
                VARCHAR,
                ImmutableList.of(VARCHAR));

        ChoicesSpecializedSqlScalarFunction specializedFunction = (ChoicesSpecializedSqlScalarFunction) function.specialize(
                boundSignature,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
        Slice slice = (Slice) specializedFunction.getChoices().get(0).getMethodHandle().invoke(INPUT_SLICE);
        assertThat(slice).isEqualTo(VARCHAR_TO_VARCHAR_RETURN_VALUE);
    }

    @Test
    public void testSetsHiddenToTrueForOperators()
    {
        Signature signature = Signature.builder()
                .returnType(new TypeSignature("varchar", typeVariable("x")))
                .argumentType(new TypeSignature("varchar", typeVariable("x")))
                .build();

        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(ADD, TestMethods.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToVarchar")))
                .build();

        BoundSignature boundSignature = new BoundSignature(
                builtinFunctionName(mangleOperatorName(ADD)),
                createVarcharType(INPUT_VARCHAR_LENGTH),
                ImmutableList.of(createVarcharType(INPUT_VARCHAR_LENGTH)));
        function.specialize(boundSignature, new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
    }

    @Test
    public void testFailIfNotAllMethodsPresent()
    {
        assertThatThrownBy(() -> new PolymorphicScalarFunctionBuilder(FUNCTION_NAME, TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("bigintToBigintReturnExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup.methods(FUNCTION_NAME)))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("method foo was not found in class io.trino.metadata.TestPolymorphicScalarFunction\\$TestMethods");
    }

    @Test
    public void testFailNoMethodsAreSelectedWhenExtraParametersFunctionIsSet()
    {
        assertThatThrownBy(() -> new PolymorphicScalarFunctionBuilder(FUNCTION_NAME, TestMethods.class)
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
        SqlScalarFunction function = new PolymorphicScalarFunctionBuilder(FUNCTION_NAME, TestMethods.class)
                .signature(SIGNATURE)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToBigintReturnFirstExtraParameter"))
                        .implementation(methodsGroup -> methodsGroup.methods("varcharToBigintReturnExtraParameter")))
                .build();

        assertThatThrownBy(() -> function.specialize(BOUND_SIGNATURE, new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of())))
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
