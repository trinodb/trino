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
import io.trino.spi.block.Block;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static java.lang.Math.toIntExact;

@ScalarFunction("element_at")
@Description("Get element of array at given index")
public final class ArrayElementAtFunction
{
    private ArrayElementAtFunction() {}

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Object elementAt(
            @TypeParameter("E") Type elementType,
            @OperatorDependency(operator = READ_VALUE, argumentTypes = "E", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL)) MethodHandle readValue,
            @SqlType("array(E)") Block array,
            @SqlType("bigint") long index)
            throws Throwable
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return readValue.invoke(array, position);
    }

    /**
     * @return TrinoException if the index is 0, -1 if the index is out of range (to tell the calling function to return null), and the element position otherwise.
     */
    private static int checkedIndexToBlockPosition(Block block, long index)
    {
        int arrayLength = block.getPositionCount();
        if (index == 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (Math.abs(index) > arrayLength) {
            return -1; // -1 indicates that the element is out of range and "ELEMENT_AT" should return null
        }
        if (index > 0) {
            return toIntExact(index - 1);
        }
        return toIntExact(arrayLength + index);
    }
}
