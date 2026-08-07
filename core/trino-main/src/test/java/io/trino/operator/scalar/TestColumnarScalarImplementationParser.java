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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InternalFunctionDependencies;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.annotations.ScalarFromAnnotationsParser;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.ColumnarScalarFunctionImplementation;
import io.trino.spi.function.ColumnarScalarImplementation;
import io.trino.spi.function.ColumnarScalarSpecializer;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestColumnarScalarImplementationParser
{
    private static final FunctionManager FUNCTION_MANAGER = createTestingFunctionManager();

    @Test
    public void testDirectImplementation()
    {
        ParametricScalar function = parseFunctionDefinition(DirectImplementation.class);
        ArrayType arrayType = new ArrayType(BIGINT);
        BoundSignature signature = new BoundSignature(builtinFunctionName("direct_columnar"), arrayType, ImmutableList.of(arrayType));
        InternalFunctionDependencies dependencies = new InternalFunctionDependencies(
                FUNCTION_MANAGER::getScalarFunctionImplementation,
                ImmutableMap.of(BIGINT.getTypeDescriptor(), BIGINT),
                ImmutableSet.of());

        ColumnarScalarFunctionImplementation implementation = function.getColumnarScalarFunctionImplementation(signature, dependencies).orElseThrow();
        Block input = createLongsBlock(11L, 12L);
        assertThat(implementation.evaluate(null, SourcePage.create(input))).isSameAs(input);
        assertThat(function.getFunctionDependencies().getTypeDependencies()).contains(typeVariable("T"));
    }

    @Test
    public void testSpecializerImplementation()
    {
        ParametricScalar function = parseFunctionDefinitions(SpecializerImplementation.class);
        ArrayType arrayType = new ArrayType(BIGINT);
        BoundSignature signature = new BoundSignature(builtinFunctionName("specialized_columnar"), arrayType, ImmutableList.of(arrayType));
        FunctionDependencies dependencies = new InternalFunctionDependencies(
                FUNCTION_MANAGER::getScalarFunctionImplementation,
                ImmutableMap.of(),
                ImmutableSet.of());

        assertThat(function.getColumnarScalarFunctionImplementation(signature, dependencies)).isPresent();
    }

    @Test
    public void testDirectFunctionSetOverload()
    {
        ImmutableList<SqlScalarFunction> functions = ImmutableList.copyOf(ScalarFromAnnotationsParser.parseFunctionDefinitions(DirectFunctionSet.class));
        FunctionDependencies dependencies = new InternalFunctionDependencies(
                FUNCTION_MANAGER::getScalarFunctionImplementation,
                ImmutableMap.of(),
                ImmutableSet.of());

        ParametricScalar bigintFunction = (ParametricScalar) functions.stream()
                .filter(function -> function.getFunctionMetadata().getSignature().getReturnType().baseName().equals("bigint"))
                .collect(onlyElement());
        BoundSignature bigintSignature = new BoundSignature(builtinFunctionName("overloaded_columnar"), BIGINT, ImmutableList.of(BIGINT));
        assertThat(bigintFunction.getColumnarScalarFunctionImplementation(bigintSignature, dependencies)).isEmpty();

        ArrayType arrayType = new ArrayType(BIGINT);
        ParametricScalar arrayFunction = (ParametricScalar) functions.stream()
                .filter(function -> function.getFunctionMetadata().getSignature().getReturnType().baseName().equals("array"))
                .collect(onlyElement());
        BoundSignature arraySignature = new BoundSignature(builtinFunctionName("overloaded_columnar"), arrayType, ImmutableList.of(arrayType));
        assertThat(arrayFunction.getColumnarScalarFunctionImplementation(arraySignature, dependencies)).isPresent();
    }

    @Test
    public void testInvalidDirectImplementation()
    {
        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(InvalidReturnType.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("must return Block");
        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(InvalidArgumentType.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("must have type Block");
        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(InvalidNullable.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("cannot use @SqlNullable");
        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(InvalidZeroArguments.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("must have at least one SQL argument");
    }

    @Test
    public void testUnmatchedSpecializer()
    {
        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinitions(UnmatchedSpecializer.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not match a scalar overload");
    }

    private static ParametricScalar parseFunctionDefinition(Class<?> definition)
    {
        return (ParametricScalar) ScalarFromAnnotationsParser.parseFunctionDefinition(definition).stream().collect(onlyElement());
    }

    private static ParametricScalar parseFunctionDefinitions(Class<?> definition)
    {
        return (ParametricScalar) ScalarFromAnnotationsParser.parseFunctionDefinitions(definition).stream().collect(onlyElement());
    }

    @ScalarFunction("direct_columnar")
    public static final class DirectImplementation
    {
        @TypeParameter("T")
        @SqlType("array(T)")
        public static Block row(@SqlType("array(T)") Block value)
        {
            return value;
        }

        @ColumnarScalarImplementation
        @TypeParameter("T")
        @SqlType("array(T)")
        public static Block columnar(
                @TypeParameter("T") Type elementType,
                ConnectorSession session,
                @SqlType("array(T)") Block value)
        {
            assertThat(elementType).isEqualTo(BIGINT);
            assertThat(session).isNull();
            return value;
        }
    }

    public static final class SpecializerImplementation
    {
        @ScalarFunction("specialized_columnar")
        @TypeParameter("T")
        @SqlType("array(T)")
        public static Block row(@SqlType("array(T)") Block value)
        {
            return value;
        }

        @ColumnarScalarSpecializer
        @ScalarFunction("specialized_columnar")
        public static Optional<ColumnarScalarFunctionImplementation> columnar(BoundSignature signature, FunctionDependencies dependencies)
        {
            return Optional.of((_, page) -> page.getBlock(0));
        }
    }

    public static final class DirectFunctionSet
    {
        @ScalarFunction("overloaded_columnar")
        @SqlType("bigint")
        public static long bigint(@SqlType("bigint") long value)
        {
            return value;
        }

        @ScalarFunction("overloaded_columnar")
        @SqlType("array(bigint)")
        public static Block array(@SqlType("array(bigint)") Block value)
        {
            return value;
        }

        @ColumnarScalarImplementation
        @ScalarFunction("overloaded_columnar")
        @SqlType("array(bigint)")
        public static Block arrayColumnar(@SqlType("array(bigint)") Block value)
        {
            return value;
        }
    }

    public static final class UnmatchedSpecializer
    {
        @ScalarFunction("row_function")
        @SqlType("bigint")
        public static long row(@SqlType("bigint") long value)
        {
            return value;
        }

        @ColumnarScalarSpecializer
        @ScalarFunction("columnar_function")
        public static Optional<ColumnarScalarFunctionImplementation> columnar(BoundSignature signature, FunctionDependencies dependencies)
        {
            return Optional.empty();
        }
    }

    @ScalarFunction("invalid_return")
    public static final class InvalidReturnType
    {
        @SqlType("bigint")
        public static long row(@SqlType("bigint") long value)
        {
            return value;
        }

        @ColumnarScalarImplementation
        @SqlType("bigint")
        public static long columnar(@SqlType("bigint") Block value)
        {
            return 0;
        }
    }

    @ScalarFunction("invalid_argument")
    public static final class InvalidArgumentType
    {
        @SqlType("bigint")
        public static long row(@SqlType("bigint") long value)
        {
            return value;
        }

        @ColumnarScalarImplementation
        @SqlType("bigint")
        public static Block columnar(@SqlType("bigint") long value)
        {
            return createLongsBlock(value);
        }
    }

    @ScalarFunction("invalid_nullable")
    public static final class InvalidNullable
    {
        @SqlType("array(bigint)")
        public static Block row(@SqlType("array(bigint)") Block value)
        {
            return value;
        }

        @ColumnarScalarImplementation
        @SqlNullable
        @SqlType("array(bigint)")
        public static Block columnar(@SqlType("array(bigint)") Block value)
        {
            return value;
        }
    }

    @ScalarFunction("invalid_zero_arguments")
    public static final class InvalidZeroArguments
    {
        @SqlType("array(bigint)")
        public static Block row()
        {
            return createLongsBlock(new Long[0]);
        }

        @ColumnarScalarImplementation
        @SqlType("array(bigint)")
        public static Block columnar()
        {
            return createLongsBlock(new Long[0]);
        }
    }
}
