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
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.util.Failures.internalError;
import static java.lang.Math.min;

@ScalarFunction("array_remove")
@Description("Remove specified values from the given array")
public final class ArrayRemoveFunction
{
    private ArrayRemoveFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block remove(
            @OperatorDependency(
                    operator = EQUAL,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = NULLABLE_RETURN))
                    MethodHandle equalFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") Object value)
    {
        IntArrayList positions = new IntArrayList(min(64, array.getPositionCount()));

        for (int i = 0; i < array.getPositionCount(); i++) {
            Object element = readNativeValue(type, array, i);

            try {
                if (element == null) {
                    positions.add(i);
                    continue;
                }
                Boolean result = (Boolean) equalFunction.invoke(element, value);
                if (result == null) {
                    throw new TrinoException(NOT_SUPPORTED, "array_remove does not support arrays with elements that are null or contain null");
                }
                if (!result) {
                    positions.add(i);
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        if (array.getPositionCount() == positions.size()) {
            return array;
        }

        return array.copyPositions(positions.elements(), 0, positions.size());
    }
}
