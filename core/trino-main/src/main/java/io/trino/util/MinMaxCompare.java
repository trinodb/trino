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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.filterReturnValue;

public final class MinMaxCompare
{
    private static final MethodHandle MIN_FUNCTION = methodHandle(MinMaxCompare.class, "min", long.class);
    private static final MethodHandle MAX_FUNCTION = methodHandle(MinMaxCompare.class, "max", long.class);

    private MinMaxCompare() {}

    public static FunctionDependencyDeclaration getMinMaxCompareFunctionDependencies(TypeSignature typeSignature, boolean min)
    {
        OperatorType comparisonOperator = min ? COMPARISON_UNORDERED_LAST : COMPARISON_UNORDERED_FIRST;
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(comparisonOperator, ImmutableList.of(typeSignature, typeSignature))
                .build();
    }

    public static MethodHandle getMinMaxCompare(FunctionDependencies dependencies, Type type, InvocationConvention convention, boolean min)
    {
        OperatorType comparisonOperator = min ? COMPARISON_UNORDERED_LAST : COMPARISON_UNORDERED_FIRST;
        MethodHandle handle = dependencies.getOperatorInvoker(comparisonOperator, List.of(type, type), convention).getMethodHandle();
        return filterReturnValue(handle, min ? MIN_FUNCTION : MAX_FUNCTION);
    }

    public static MethodHandle getMinMaxCompare(TypeOperators typeOperators, Type type, InvocationConvention convention, boolean min)
    {
        MethodHandle handle;
        if (min) {
            handle = typeOperators.getComparisonUnorderedLastOperator(type, convention);
        }
        else {
            handle = typeOperators.getComparisonUnorderedFirstOperator(type, convention);
        }
        return filterReturnValue(handle, min ? MIN_FUNCTION : MAX_FUNCTION);
    }

    @UsedByGeneratedCode
    public static boolean min(long comparisonResult)
    {
        return comparisonResult < 0;
    }

    @UsedByGeneratedCode
    public static boolean max(long comparisonResult)
    {
        return comparisonResult > 0;
    }
}
