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
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.READ_VALUE;

@ScalarFunction("array_min")
@Description("Get minimum value of array")
public final class ArrayMinFunction
{
    private ArrayMinFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Object arrayMin(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL}, result = FAIL_ON_NULL)) MethodHandle compareMethodHandle,
            @OperatorDependency(operator = READ_VALUE, argumentTypes = "T", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL)) MethodHandle readValue,
            @SqlType("array(T)") Block block)
            throws Throwable
    {
        int selectedPosition = -1;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                return null;
            }
            if (selectedPosition < 0 || ((long) compareMethodHandle.invokeExact(block, position, block, selectedPosition)) < 0) {
                selectedPosition = position;
            }
        }
        if (selectedPosition < 0) {
            return null;
        }

        return readValue.invoke(block, selectedPosition);
    }
}
