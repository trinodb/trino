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
import io.trino.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;

@Description("Determines whether an array contains a sequence, with the values in the exact order")
@ScalarFunction("contains_sequence")
public final class ArrayContainsSequence
{
    private ArrayContainsSequence() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean containsSequence(
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
                    MethodHandle distinctFrom,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("array(T)") Block value)
            throws Throwable
    {
        int arrayLimit = arrayBlock.getPositionCount() - value.getPositionCount();
        for (int arrayPosition = 0; arrayPosition <= arrayLimit; arrayPosition++) {
            if (containsSequenceAt(distinctFrom, arrayBlock, arrayPosition, value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsSequenceAt(MethodHandle distinctFrom, Block arrayBlock, int arrayPosition, Block value)
            throws Throwable
    {
        for (int valuePosition = 0; valuePosition < value.getPositionCount(); valuePosition++) {
            if ((boolean) distinctFrom.invokeExact(arrayBlock, arrayPosition + valuePosition, value, valuePosition)) {
                return false;
            }
        }
        return true;
    }
}
