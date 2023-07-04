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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.CastDependency;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;

public final class ArrayJoin
{
    private static final String NAME = "array_join";
    private static final String DESCRIPTION = "Concatenates the elements of the given array using a delimiter and an optional string to replace nulls";

    private ArrayJoin() {}

    @ScalarFunction(NAME)
    @Description(DESCRIPTION)
    @TypeParameter("E")
    @SqlNullable
    @SqlType("varchar")
    public static Slice arrayJoin(
            @CastDependency(fromType = "E", toType = "varchar", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = NULLABLE_RETURN, session = true)) MethodHandle castFunction,
            ConnectorSession session,
            @SqlType("array(E)") Block array,
            @SqlType("varchar") Slice delimiter)
    {
        return arrayJoin(castFunction, session, array, delimiter, null);
    }

    @ScalarFunction(NAME)
    @Description(DESCRIPTION)
    @TypeParameter("E")
    @SqlNullable
    @SqlType("varchar")
    public static Slice arrayJoin(
            @CastDependency(fromType = "E", toType = "varchar", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = NULLABLE_RETURN, session = true)) MethodHandle castFunction,
            ConnectorSession session,
            @SqlType("array(E)") Block array,
            @SqlType("varchar") Slice delimiter,
            @SqlType("varchar") Slice nullReplacement)
    {
        int numElements = array.getPositionCount();

        Slice[] slices = new Slice[numElements * 2];
        int sliceIndex = 0;
        for (int arrayIndex = 0; arrayIndex < numElements; arrayIndex++) {
            Slice value = null;
            if (!array.isNull(arrayIndex)) {
                try {
                    value = (Slice) castFunction.invokeExact(session, array, arrayIndex);
                }
                catch (Throwable throwable) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error casting array element to VARCHAR", throwable);
                }
            }

            if (value == null) {
                if (nullReplacement == null) {
                    continue;
                }
                value = nullReplacement;
            }

            if (sliceIndex > 0) {
                slices[sliceIndex++] = delimiter;
            }
            slices[sliceIndex++] = value;
        }

        int totalSize = 0;
        for (Slice slice : slices) {
            if (slice == null) {
                break;
            }
            totalSize += slice.length();
        }

        Slice result = Slices.allocate(totalSize);
        int offset = 0;
        for (Slice slice : slices) {
            if (slice == null) {
                break;
            }
            result.setBytes(offset, slice);
            offset += slice.length();
        }
        return result;
    }
}
