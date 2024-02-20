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

import com.google.common.primitives.Ints;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.util.Failures.checkCondition;

@ScalarFunction("array_sort")
@Description("Sorts the given array with a lambda comparator.")
public final class ArraySortComparatorFunction
{
    private final BufferedArrayValueBuilder arrayValueBuilder;
    private static final int INITIAL_LENGTH = 128;
    private List<Integer> positions = Ints.asList(new int[INITIAL_LENGTH]);

    @TypeParameter("T")
    public ArraySortComparatorFunction(@TypeParameter("T") Type elementType)
    {
        arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(elementType));
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public Block sort(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = READ_VALUE, argumentTypes = "T", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL)) MethodHandle readValue,
            @SqlType("array(T)") Block block,
            @SqlType("function(T, T, integer)") ComparatorObjectLambda function)
    {
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        Comparator<Integer> comparator = (x, y) -> {
            try {
                return comparatorResult(function.apply(
                        block.isNull(x) ? null : readValue.invoke(block, x),
                        block.isNull(y) ? null : readValue.invoke(block, y)));
            }
            catch (Throwable e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        };

        sortPositions(arrayLength, comparator);

        return arrayValueBuilder.build(arrayLength, elementBuilder -> {
            for (int i = 0; i < arrayLength; i++) {
                type.appendTo(block, positions.get(i), elementBuilder);
            }
        });
    }

    private void initPositionsList(int arrayLength)
    {
        if (positions.size() < arrayLength) {
            positions = Ints.asList(new int[arrayLength]);
        }
        for (int i = 0; i < arrayLength; i++) {
            positions.set(i, i);
        }
    }

    private void sortPositions(int arrayLength, Comparator<Integer> comparator)
    {
        List<Integer> list = positions.subList(0, arrayLength);

        try {
            list.sort(comparator);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Lambda comparator violates the comparator contract", e);
        }
    }

    private static int comparatorResult(Long result)
    {
        checkCondition(
                (result != null) && ((result == -1) || (result == 0) || (result == 1)),
                INVALID_FUNCTION_ARGUMENT,
                "Lambda comparator must return either -1, 0, or 1");
        return result.intValue();
    }

    @FunctionalInterface
    public interface ComparatorObjectLambda
            extends LambdaFunctionInterface
    {
        Long apply(Object x, Object y);
    }
}
