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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.CastDependency;
import io.trino.spi.function.Convention;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.OperatorType.CAST;

@ScalarOperator(CAST)
public final class ArrayToArrayCast
{
    private ArrayToArrayCast() {}

    @TypeParameter("F")
    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block filter(
            @TypeParameter("T") Type resultType,
            @CastDependency(fromType = "F", toType = "T", convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = BLOCK_BUILDER, session = true)) MethodHandle cast,
            ConnectorSession session,
            @SqlType("array(F)") Block array)
            throws Throwable
    {
        int positionCount = array.getPositionCount();
        BlockBuilder resultBuilder = resultType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (array.isNull(position)) {
                resultBuilder.appendNull();
            }
            else {
                cast.invokeExact(session, array, position, resultBuilder);
            }
        }
        return resultBuilder.build();
    }
}
