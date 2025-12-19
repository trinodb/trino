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

import io.trino.spi.block.Block;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static java.lang.Boolean.TRUE;

@Description("Returns the first element from the array that matches the given predicate")
@ScalarFunction("first_match")
public final class ArrayFirstMatchFunction
{
    private ArrayFirstMatchFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Object firstMatch(
            @OperatorDependency(operator = READ_VALUE, argumentTypes = "T", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL)) MethodHandle readValue,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") ObjectToBooleanFunction function)
            throws Throwable
    {
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (!arrayBlock.isNull(i)) {
                final Object element = readValue.invoke(arrayBlock, i);
                if (TRUE.equals(function.apply(element))) {
                    return element;
                }
            }
        }
        return null;
    }
}
