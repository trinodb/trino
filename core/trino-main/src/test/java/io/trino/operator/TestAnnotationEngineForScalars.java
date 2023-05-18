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
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InternalFunctionDependencies;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.operator.annotations.LiteralImplementationDependency;
import io.trino.operator.annotations.TypeImplementationDependency;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.ParametricScalar;
import io.trino.operator.scalar.annotations.ParametricScalarImplementation.ParametricScalarImplementationChoice;
import io.trino.operator.scalar.annotations.ScalarFromAnnotationsParser;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.Description;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAnnotationEngineForScalars
        extends TestAnnotationEngine
{
    private static final FunctionManager FUNCTION_MANAGER = createTestingFunctionManager();

    @ScalarFunction("single_implementation_parametric_scalar")
    @Description("Simple scalar with single implementation based on class")
    public static final class SingleImplementationScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testSingleImplementationScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("single_implementation_parametric_scalar")
                .returnType(DOUBLE)
                .argumentType(DOUBLE)
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(SingleImplementationScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Simple scalar with single implementation based on class");
        assertThat(functionMetadata.getFunctionNullability().isArgumentNullable(0)).isFalse();

        assertImplementationCount(scalar, 1, 0, 0);

        BoundSignature boundSignature = new BoundSignature(expectedSignature.getName(), DOUBLE, ImmutableList.of(DOUBLE));
        ChoicesSpecializedSqlScalarFunction specialized = (ChoicesSpecializedSqlScalarFunction) scalar.specialize(
                boundSignature,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
        assertThat(specialized.getChoices().get(0).getInstanceFactory()).isEmpty();
    }

    @ScalarFunction(value = "hidden_scalar_function", hidden = true)
    @Description("Simple scalar with hidden property set")
    public static final class HiddenScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testHiddenScalarParse()
    {
        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(HiddenScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isTrue();
    }

    @ScalarFunction(value = "non_deterministic_scalar_function", deterministic = false)
    @Description("Simple scalar with deterministic property reset")
    public static final class NonDeterministicScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testNonDeterministicScalarParse()
    {
        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(NonDeterministicScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.isDeterministic()).isFalse();
        assertThat(functionMetadata.isHidden()).isFalse();
    }

    @ScalarFunction("scalar_with_nullable")
    @Description("Simple scalar with nullable primitive")
    public static final class WithNullablePrimitiveArgScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(
                @SqlType(StandardTypes.DOUBLE) double v,
                @SqlType(StandardTypes.DOUBLE) double v2,
                @IsNull boolean v2isNull)
        {
            return v;
        }
    }

    @Test
    public void testWithNullablePrimitiveArgScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("scalar_with_nullable")
                .returnType(DOUBLE)
                .argumentType(DOUBLE)
                .argumentType(DOUBLE)
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(WithNullablePrimitiveArgScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Simple scalar with nullable primitive");
        assertThat(functionMetadata.getFunctionNullability().isArgumentNullable(0)).isFalse();
        assertThat(functionMetadata.getFunctionNullability().isArgumentNullable(1)).isTrue();

        BoundSignature boundSignature = new BoundSignature(expectedSignature.getName(), DOUBLE, ImmutableList.of(DOUBLE, DOUBLE));
        ChoicesSpecializedSqlScalarFunction specialized = (ChoicesSpecializedSqlScalarFunction) scalar.specialize(
                boundSignature,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
        assertThat(specialized.getChoices().get(0).getInstanceFactory()).isEmpty();
    }

    @ScalarFunction("scalar_with_nullable_complex")
    @Description("Simple scalar with nullable complex type")
    public static final class WithNullableComplexArgScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(
                @SqlType(StandardTypes.DOUBLE) double v,
                @SqlNullable @SqlType(StandardTypes.DOUBLE) Double v2)
        {
            return v;
        }
    }

    @Test
    public void testWithNullableComplexArgScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("scalar_with_nullable_complex")
                .returnType(DOUBLE)
                .argumentType(DOUBLE)
                .argumentType(DOUBLE)
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(WithNullableComplexArgScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Simple scalar with nullable complex type");
        assertThat(functionMetadata.getFunctionNullability().isArgumentNullable(0)).isFalse();
        assertThat(functionMetadata.getFunctionNullability().isArgumentNullable(1)).isTrue();

        BoundSignature boundSignature = new BoundSignature(expectedSignature.getName(), DOUBLE, ImmutableList.of(DOUBLE, DOUBLE));
        ChoicesSpecializedSqlScalarFunction specialized = (ChoicesSpecializedSqlScalarFunction) scalar.specialize(
                boundSignature,
                new InternalFunctionDependencies(FUNCTION_MANAGER::getScalarFunctionImplementation, ImmutableMap.of(), ImmutableSet.of()));
        assertThat(specialized.getChoices().get(0).getInstanceFactory()).isEmpty();
    }

    public static final class StaticMethodScalarFunction
    {
        @ScalarFunction("static_method_scalar")
        @Description("Simple scalar with single implementation based on method")
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testStaticMethodScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("static_method_scalar")
                .returnType(DOUBLE)
                .argumentType(DOUBLE)
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinitions(StaticMethodScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Simple scalar with single implementation based on method");
    }

    public static final class MultiScalarFunction
    {
        @ScalarFunction("static_method_scalar_1")
        @Description("Simple scalar with single implementation based on method 1")
        @SqlType(StandardTypes.DOUBLE)
        public static double fun1(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }

        @ScalarFunction(value = "static_method_scalar_2", hidden = true, deterministic = false)
        @Description("Simple scalar with single implementation based on method 2")
        @SqlType(StandardTypes.BIGINT)
        public static long fun2(@SqlType(StandardTypes.BIGINT) long v)
        {
            return v;
        }
    }

    @Test
    public void testMultiScalarParse()
    {
        Signature expectedSignature1 = Signature.builder()
                .name("static_method_scalar_1")
                .returnType(DOUBLE)
                .argumentType(DOUBLE)
                .build();

        Signature expectedSignature2 = Signature.builder()
                .name("static_method_scalar_2")
                .returnType(BIGINT)
                .argumentType(BIGINT)
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinitions(MultiScalarFunction.class);
        assertThat(functions).hasSize(2);
        ParametricScalar scalar1 = (ParametricScalar) functions.stream().filter(function -> function.getFunctionMetadata().getSignature().equals(expectedSignature1)).collect(toImmutableList()).get(0);
        ParametricScalar scalar2 = (ParametricScalar) functions.stream().filter(function -> function.getFunctionMetadata().getSignature().equals(expectedSignature2)).collect(toImmutableList()).get(0);

        assertImplementationCount(scalar1, 1, 0, 0);
        assertImplementationCount(scalar2, 1, 0, 0);

        FunctionMetadata functionMetadata1 = scalar1.getFunctionMetadata();
        assertThat(functionMetadata1.getSignature()).isEqualTo(expectedSignature1);
        assertThat(functionMetadata1.isDeterministic()).isTrue();
        assertThat(functionMetadata1.isHidden()).isFalse();
        assertThat(functionMetadata1.getDescription()).isEqualTo("Simple scalar with single implementation based on method 1");

        FunctionMetadata functionMetadata2 = scalar2.getFunctionMetadata();
        assertThat(functionMetadata2.getSignature()).isEqualTo(expectedSignature2);
        assertThat(functionMetadata2.isDeterministic()).isFalse();
        assertThat(functionMetadata2.isHidden()).isTrue();
        assertThat(functionMetadata2.getDescription()).isEqualTo("Simple scalar with single implementation based on method 2");
    }

    @ScalarFunction("parametric_scalar")
    @Description("Parametric scalar description")
    public static final class ParametricScalarFunction
    {
        @SqlType("T")
        @TypeParameter("T")
        public static double fun(@SqlType("T") double v)
        {
            return v;
        }

        @SqlType("T")
        @TypeParameter("T")
        public static long fun(@SqlType("T") long v)
        {
            return v;
        }
    }

    @Test
    public void testParametricScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("parametric_scalar")
                .typeVariable("T")
                .returnType(new TypeSignature("T"))
                .argumentType(new TypeSignature("T"))
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ParametricScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 0, 2, 0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Parametric scalar description");
    }

    @ScalarFunction("with_exact_scalar")
    @Description("Parametric scalar with exact and generic implementations")
    public static final class ComplexParametricScalarFunction
    {
        @SqlType(StandardTypes.BOOLEAN)
        @LiteralParameters("x")
        public static boolean fun1(@SqlType("array(varchar(x))") Block array)
        {
            return true;
        }

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean fun2(@SqlType("array(varchar(17))") Block array)
        {
            return true;
        }
    }

    @Test
    public void testComplexParametricScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("with_exact_scalar")
                .returnType(BOOLEAN)
                .argumentType(arrayType(new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x"))))
                .build();

        Signature exactSignature = Signature.builder()
                .name("with_exact_scalar")
                .returnType(BOOLEAN)
                .argumentType(arrayType(createVarcharType(17).getTypeSignature()))
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ComplexParametricScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar.getImplementations(), 1, 0, 1);
        assertThat(scalar.getImplementations().getExactImplementations().keySet()).containsExactly(exactSignature);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Parametric scalar with exact and generic implementations");
    }

    @ScalarFunction("parametric_scalar_inject")
    @Description("Parametric scalar with literal injected")
    public static final class SimpleInjectionScalarFunction
    {
        @SqlType(StandardTypes.BIGINT)
        @LiteralParameters("x")
        public static long fun(
                @LiteralParameter("x") Long literalParam,
                @SqlType("varchar(x)") Slice val)
        {
            return literalParam;
        }
    }

    @Test
    public void testSimpleInjectionScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("parametric_scalar_inject")
                .returnType(BIGINT)
                .argumentType(new TypeSignature("varchar", TypeSignatureParameter.typeVariable("x")))
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(SimpleInjectionScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 0, 0, 1);
        List<ParametricScalarImplementationChoice> parametricScalarImplementationChoices = scalar.getImplementations().getGenericImplementations().get(0).getChoices();
        assertThat(parametricScalarImplementationChoices).hasSize(1);
        List<ImplementationDependency> dependencies = parametricScalarImplementationChoices.get(0).getDependencies();
        assertThat(dependencies).hasSize(1);
        assertThat(dependencies.get(0)).isInstanceOf(LiteralImplementationDependency.class);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Parametric scalar with literal injected");
    }

    @ScalarFunction("parametric_scalar_inject_constructor")
    @Description("Parametric scalar with type injected though constructor")
    public static class ConstructorInjectionScalarFunction
    {
        @TypeParameter("T")
        public ConstructorInjectionScalarFunction(@TypeParameter("T") Type type) {}

        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("T")
        public long fun(@SqlType("array(T)") Block val)
        {
            return 17L;
        }

        @SqlType(StandardTypes.BIGINT)
        public long funBigint(@SqlType("array(bigint)") Block val)
        {
            return 17L;
        }

        @SqlType(StandardTypes.BIGINT)
        public long funDouble(@SqlType("array(double)") Block val)
        {
            return 17L;
        }
    }

    @Test
    public void testConstructorInjectionScalarParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("parametric_scalar_inject_constructor")
                .typeVariable("T")
                .returnType(BIGINT)
                .argumentType(arrayType(new TypeSignature("T")))
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ConstructorInjectionScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 2, 0, 1);
        List<ParametricScalarImplementationChoice> parametricScalarImplementationChoices = scalar.getImplementations().getGenericImplementations().get(0).getChoices();
        assertThat(parametricScalarImplementationChoices).hasSize(1);
        List<ImplementationDependency> dependencies = parametricScalarImplementationChoices.get(0).getDependencies();
        assertThat(dependencies).isEmpty();
        List<ImplementationDependency> constructorDependencies = parametricScalarImplementationChoices.get(0).getConstructorDependencies();
        assertThat(constructorDependencies).hasSize(1);
        assertThat(constructorDependencies.get(0)).isInstanceOf(TypeImplementationDependency.class);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Parametric scalar with type injected though constructor");
    }

    @ScalarFunction("fixed_type_parameter_scalar_function")
    @Description("Parametric scalar that uses TypeParameter with fixed type")
    public static final class FixedTypeParameterScalarFunction
    {
        @SqlType(StandardTypes.BIGINT)
        public static long fun(
                @TypeParameter("ROW(ARRAY(BIGINT),ROW(ROW(CHAR)),BIGINT,MAP(BIGINT,CHAR))") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testFixedTypeParameterParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("fixed_type_parameter_scalar_function")
                .returnType(BIGINT)
                .argumentType(BIGINT)
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(FixedTypeParameterScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 1, 0, 0);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Parametric scalar that uses TypeParameter with fixed type");
    }

    @ScalarFunction("partially_fixed_type_parameter_scalar_function")
    @Description("Parametric scalar that uses TypeParameter with partially fixed type")
    public static final class PartiallyFixedTypeParameterScalarFunction
    {
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("T1")
        @TypeParameter("T2")
        public static long fun(
                @TypeParameter("ROW(ARRAY(T1),ROW(ROW(T2)),CHAR)") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testPartiallyFixedTypeParameterParse()
    {
        Signature expectedSignature = Signature.builder()
                .name("partially_fixed_type_parameter_scalar_function")
                .typeVariable("T1")
                .typeVariable("T2")
                .returnType(BIGINT)
                .argumentType(BIGINT)
                .build();

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(PartiallyFixedTypeParameterScalarFunction.class);
        assertThat(functions).hasSize(1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 0, 0, 1);
        List<ParametricScalarImplementationChoice> parametricScalarImplementationChoices = scalar.getImplementations().getGenericImplementations().get(0).getChoices();
        assertThat(parametricScalarImplementationChoices).hasSize(1);
        List<ImplementationDependency> dependencies = parametricScalarImplementationChoices.get(0).getDependencies();
        assertThat(dependencies).hasSize(1);

        FunctionMetadata functionMetadata = scalar.getFunctionMetadata();
        assertThat(functionMetadata.getSignature()).isEqualTo(expectedSignature);
        assertThat(functionMetadata.isDeterministic()).isTrue();
        assertThat(functionMetadata.isHidden()).isFalse();
        assertThat(functionMetadata.getDescription()).isEqualTo("Parametric scalar that uses TypeParameter with partially fixed type");
    }
}
