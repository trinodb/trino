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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.PolymorphicScalarFunctionBuilder;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.prestosql.metadata.PolymorphicScalarFunctionBuilder.constant;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TypeSignatureParameter.typeVariable;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.compare;
import static io.prestosql.util.Reflection.methodHandle;

public final class DecimalInequalityOperators
{
    private static final TypeSignature DECIMAL_SIGNATURE = new TypeSignature("decimal", typeVariable("a_precision"), typeVariable("a_scale"));

    private static final MethodHandle IS_RESULT_LESS_THAN = methodHandle(DecimalInequalityOperators.class, "isResultLessThan", int.class);
    private static final MethodHandle IS_RESULT_LESS_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultLessThanOrEqual", int.class);
    private static final MethodHandle IS_RESULT_GREATER_THAN = methodHandle(DecimalInequalityOperators.class, "isResultGreaterThan", int.class);
    private static final MethodHandle IS_RESULT_GREATER_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultGreaterThanOrEqual", int.class);

    public static final SqlScalarFunction DECIMAL_LESS_THAN_OPERATOR = comparisonOperator(LESS_THAN, IS_RESULT_LESS_THAN);
    public static final SqlScalarFunction DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR = comparisonOperator(LESS_THAN_OR_EQUAL, IS_RESULT_LESS_THAN_OR_EQUAL);
    public static final SqlScalarFunction DECIMAL_GREATER_THAN_OPERATOR = comparisonOperator(GREATER_THAN, IS_RESULT_GREATER_THAN);
    public static final SqlScalarFunction DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR = comparisonOperator(GREATER_THAN_OR_EQUAL, IS_RESULT_GREATER_THAN_OR_EQUAL);

    private DecimalInequalityOperators() {}

    @UsedByGeneratedCode
    public static boolean isResultLessThan(int comparisonResult)
    {
        return comparisonResult < 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultLessThanOrEqual(int comparisonResult)
    {
        return comparisonResult <= 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultGreaterThan(int comparisonResult)
    {
        return comparisonResult > 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultGreaterThanOrEqual(int comparisonResult)
    {
        return comparisonResult >= 0;
    }

    private static PolymorphicScalarFunctionBuilder makeBinaryOperatorFunctionBuilder(OperatorType operatorType)
    {
        Signature signature = Signature.builder()
                .operatorType(operatorType)
                .argumentTypes(DECIMAL_SIGNATURE, DECIMAL_SIGNATURE)
                .returnType(BOOLEAN.getTypeSignature())
                .build();
        return new PolymorphicScalarFunctionBuilder(DecimalInequalityOperators.class)
                .signature(signature)
                .deterministic(true);
    }

    private static SqlScalarFunction comparisonOperator(OperatorType operatorType, MethodHandle getResultMethodHandle)
    {
        return makeBinaryOperatorFunctionBuilder(operatorType)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("primitiveShortShort", "primitiveLongLong")
                                .withExtraParameters(constant(getResultMethodHandle))))
                .build();
    }

    @UsedByGeneratedCode
    public static Boolean boxedShortShort(long a, long b, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, Long.compare(a, b));
    }

    @UsedByGeneratedCode
    public static Boolean boxedLongLong(Slice left, Slice right, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, compare(left, right));
    }

    @UsedByGeneratedCode
    //TODO: remove when introducing nullable comparisons (<=, <, >, >=)
    public static boolean primitiveShortShort(long a, long b, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, Long.compare(a, b));
    }

    @UsedByGeneratedCode
    //TODO: remove when introducing nullable comparisons (<=, <, >, >=)
    public static boolean primitiveLongLong(Slice left, Slice right, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, compare(left, right));
    }

    private static boolean invokeGetResult(MethodHandle getResultMethodHandle, int comparisonResult)
    {
        try {
            return (boolean) getResultMethodHandle.invokeExact(comparisonResult);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @UsedByGeneratedCode
    public static boolean betweenShortShortShort(long value, long low, long high)
    {
        return low <= value && value <= high;
    }

    @UsedByGeneratedCode
    public static boolean betweenLongLongLong(Slice value, Slice low, Slice high)
    {
        return compare(low, value) <= 0 && compare(value, high) <= 0;
    }
}
