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

import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.TypeParameterSpecialization;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.util.function.Supplier;

import static io.trino.operator.scalar.TryFunction.NAME;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;

@Description("Internal try function for desugaring TRY")
@ScalarFunction(value = NAME, hidden = true, deterministic = false)
public final class TryFunction
{
    public static final String NAME = "$try";

    private TryFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlNullable
    @SqlType("T")
    public static Long tryLong(@SqlType("function(T)") TryLongLambda function)
    {
        try {
            return function.apply();
        }
        catch (TrinoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlNullable
    @SqlType("T")
    public static Double tryDouble(@SqlType("function(T)") TryDoubleLambda function)
    {
        try {
            return function.apply();
        }
        catch (TrinoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlNullable
    @SqlType("T")
    public static Boolean tryBoolean(@SqlType("function(T)") TryBooleanLambda function)
    {
        try {
            return function.apply();
        }
        catch (TrinoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Object.class)
    @SqlNullable
    @SqlType("T")
    public static Object tryObject(@SqlType("function(T)") TryObjectLambda function)
    {
        try {
            return function.apply();
        }
        catch (TrinoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @FunctionalInterface
    public interface TryLongLambda
            extends LambdaFunctionInterface
    {
        Long apply();
    }

    @FunctionalInterface
    public interface TryDoubleLambda
            extends LambdaFunctionInterface
    {
        Double apply();
    }

    @FunctionalInterface
    public interface TryBooleanLambda
            extends LambdaFunctionInterface
    {
        Boolean apply();
    }

    @FunctionalInterface
    public interface TryObjectLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }

    public static <T> T evaluate(Supplier<T> supplier, T defaultValue)
    {
        try {
            return supplier.get();
        }
        catch (TrinoException e) {
            propagateIfUnhandled(e);
            return defaultValue;
        }
    }

    private static void propagateIfUnhandled(TrinoException e)
            throws TrinoException
    {
        int errorCode = e.getErrorCode().getCode();
        if (errorCode == DIVISION_BY_ZERO.toErrorCode().getCode()
                || errorCode == INVALID_CAST_ARGUMENT.toErrorCode().getCode()
                || errorCode == INVALID_FUNCTION_ARGUMENT.toErrorCode().getCode()
                || errorCode == NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().getCode()) {
            return;
        }

        throw e;
    }
}
