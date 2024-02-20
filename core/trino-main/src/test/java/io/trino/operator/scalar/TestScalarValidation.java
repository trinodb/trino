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

import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("UtilityClassWithoutPrivateConstructor")
public class TestScalarValidation
{
    @Test
    public void testBogusParametricMethodAnnotation()
    {
        assertThatThrownBy(() -> extractParametricScalar(BogusParametricMethodAnnotation.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Parametric class method .* is annotated with @ScalarFunction");
    }

    @ScalarFunction
    public static final class BogusParametricMethodAnnotation
    {
        @ScalarFunction
        public static void bad() {}
    }

    @Test
    public void testNoParametricMethods()
    {
        assertThatThrownBy(() -> extractParametricScalar(NoParametricMethods.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Parametric class .* does not have any annotated methods");
    }

    @SuppressWarnings("EmptyClass")
    @ScalarFunction
    public static final class NoParametricMethods {}

    @Test
    public void testMethodMissingReturnAnnotation()
    {
        assertThatThrownBy(() -> extractScalars(MethodMissingReturnAnnotation.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* is missing @SqlType annotation");
    }

    public static final class MethodMissingReturnAnnotation
    {
        @ScalarFunction
        public static void bad() {}
    }

    @Test
    public void testMethodMissingScalarAnnotation()
    {
        assertThatThrownBy(() -> extractScalars(MethodMissingScalarAnnotation.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Method .* annotated with @SqlType is missing @ScalarFunction or @ScalarOperator");
    }

    public static final class MethodMissingScalarAnnotation
    {
        @SuppressWarnings("unused")
        @SqlType
        public static void bad() {}
    }

    @Test
    public void testPrimitiveWrapperReturnWithoutNullable()
    {
        assertThatThrownBy(() -> extractScalars(PrimitiveWrapperReturnWithoutNullable.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* has wrapper return type Long but is missing @SqlNullable");
    }

    public static final class PrimitiveWrapperReturnWithoutNullable
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static Long bad()
        {
            return 0L;
        }
    }

    @Test
    public void testPrimitiveReturnWithNullable()
    {
        assertThatThrownBy(() -> extractScalars(PrimitiveReturnWithNullable.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* annotated with @SqlNullable has primitive return type long");
    }

    public static final class PrimitiveReturnWithNullable
    {
        @ScalarFunction
        @SqlNullable
        @SqlType(StandardTypes.BIGINT)
        public static long bad()
        {
            return 0L;
        }
    }

    @Test
    public void testPrimitiveWrapperParameterWithoutNullable()
    {
        assertThatThrownBy(() -> extractScalars(PrimitiveWrapperParameterWithoutNullable.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("A parameter with USE_NULL_FLAG or RETURN_NULL_ON_NULL convention must not use wrapper type. Found in method .*");
    }

    public static final class PrimitiveWrapperParameterWithoutNullable
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BOOLEAN) Boolean boxed)
        {
            return 0;
        }
    }

    @Test
    public void testPrimitiveParameterWithNullable()
    {
        assertThatThrownBy(() -> extractScalars(PrimitiveParameterWithNullable.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Method .* has parameter with primitive type double annotated with @SqlNullable");
    }

    public static final class PrimitiveParameterWithNullable
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlNullable @SqlType(StandardTypes.DOUBLE) double primitive)
        {
            return 0;
        }
    }

    @Test
    public void testParameterWithoutType()
    {
        assertThatThrownBy(() -> extractScalars(ParameterWithoutType.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* is missing @SqlType annotation for parameter");
    }

    public static final class ParameterWithoutType
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(long missing)
        {
            return 0;
        }
    }

    @Test
    public void testNonPublicAnnnotatedMethod()
    {
        assertThatThrownBy(() -> extractScalars(NonPublicAnnnotatedMethod.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* annotated with @ScalarFunction must be public");
    }

    public static final class NonPublicAnnnotatedMethod
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        private static long bad()
        {
            return 0;
        }
    }

    @Test
    public void testMethodWithLegacyNullable()
    {
        assertThatThrownBy(() -> extractScalars(MethodWithLegacyNullable.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* is annotated with @Nullable but not @SqlNullable");
    }

    public static final class MethodWithLegacyNullable
    {
        @ScalarFunction
        @Nullable
        @SqlType(StandardTypes.BIGINT)
        public static Long bad()
        {
            return 0L;
        }
    }

    @Test
    public void testParameterWithConnectorAndIsNull()
    {
        assertThatThrownBy(() -> extractScalars(ParameterWithConnectorAndIsNull.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* has @IsNull parameter that does not follow a @SqlType parameter");
    }

    public static final class ParameterWithConnectorAndIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(ConnectorSession session, @IsNull boolean isNull)
        {
            return 0;
        }
    }

    @Test
    public void testParameterWithOnlyIsNull()
    {
        assertThatThrownBy(() -> extractScalars(ParameterWithOnlyIsNull.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* has @IsNull parameter that does not follow a @SqlType parameter");
    }

    public static final class ParameterWithOnlyIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@IsNull boolean isNull)
        {
            return 0;
        }
    }

    @Test
    public void testParameterWithNonBooleanIsNull()
    {
        assertThatThrownBy(() -> extractScalars(ParameterWithNonBooleanIsNull.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* has non-boolean parameter with @IsNull");
    }

    public static final class ParameterWithNonBooleanIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BIGINT) long value, @IsNull int isNull)
        {
            return 0;
        }
    }

    @Test
    public void testParameterWithBoxedPrimitiveIsNull()
    {
        assertThatThrownBy(() -> extractScalars(ParameterWithBoxedPrimitiveIsNull.class))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("A parameter with USE_NULL_FLAG or RETURN_NULL_ON_NULL convention must not use wrapper type. Found in method .*");
    }

    public static final class ParameterWithBoxedPrimitiveIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BIGINT) Long value, @IsNull boolean isNull)
        {
            return 0;
        }
    }

    @Test
    public void testParameterWithOtherAnnotationsWithIsNull()
    {
        assertThatThrownBy(() -> extractScalars(ParameterWithOtherAnnotationsWithIsNull.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Method .* has @IsNull parameter that has other annotations");
    }

    public static final class ParameterWithOtherAnnotationsWithIsNull
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long bad(@SqlType(StandardTypes.BIGINT) long value, @IsNull @SqlNullable boolean isNull)
        {
            return 0;
        }
    }

    @Test
    public void testNonUpperCaseTypeParameters()
    {
        assertThatThrownBy(() -> extractScalars(TypeParameterWithNonUpperCaseAnnotation.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Expected type parameter to only contain A-Z and 0-9 \\(starting with A-Z\\), but got bad on method .*");
    }

    public static final class TypeParameterWithNonUpperCaseAnnotation
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("bad")
        public static long bad(@TypeParameter("array(bad)") Type type, @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testLeadingNumericTypeParameters()
    {
        assertThatThrownBy(() -> extractScalars(TypeParameterWithLeadingNumbers.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Expected type parameter to only contain A-Z and 0-9 \\(starting with A-Z\\), but got 1E on method .*");
    }

    public static final class TypeParameterWithLeadingNumbers
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("1E")
        public static long bad(@TypeParameter("array(1E)") Type type, @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testNonPrimitiveTypeParameters()
    {
        assertThatThrownBy(() -> extractScalars(TypeParameterWithNonPrimitiveAnnotation.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Expected type parameter not to take parameters, but got 'e' on method .*");
    }

    public static final class TypeParameterWithNonPrimitiveAnnotation
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("E")
        public static long bad(@TypeParameter("E(VARCHAR)") Type type, @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testValidTypeParameters()
    {
        extractScalars(ValidTypeParameter.class);
    }

    public static final class ValidTypeParameter
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long good1(
                @TypeParameter("ROW(ARRAY(BIGINT),MAP(INTEGER,DECIMAL),SMALLINT,CHAR,BOOLEAN,DATE,TIMESTAMP,VARCHAR)") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }

        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("E12")
        @TypeParameter("F34")
        public static long good2(
                @TypeParameter("ROW(ARRAY(E12),JSON,TIME,VARBINARY,ROW(ROW(F34)))") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testValidTypeParametersForConstructors()
    {
        extractParametricScalar(ConstructorWithValidTypeParameters.class);
    }

    @Test
    public void testInvalidTypeParametersForConstructors()
    {
        assertThatThrownBy(() -> extractParametricScalar(ConstructorWithInvalidTypeParameters.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Expected type parameter not to take parameters, but got 'k' on method .*");
    }

    private static void extractParametricScalar(Class<?> clazz)
    {
        InternalFunctionBundle.builder().scalar(clazz);
    }

    private static void extractScalars(Class<?> clazz)
    {
        InternalFunctionBundle.builder().scalars(clazz);
    }
}
