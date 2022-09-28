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
import io.trino.spi.function.TypeParameterSpecialization;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;

@ScalarOperator(CAST)
public final class ArrayToArrayCast
{
    private ArrayToArrayCast() {}

    @TypeParameter("F")
    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType("array(T)")
    public static Block filterLong(
            @TypeParameter("T") Type resultType,
            @CastDependency(fromType = "F", toType = "T", convention = @Convention(arguments = BLOCK_POSITION, result = NULLABLE_RETURN, session = true)) MethodHandle cast,
            ConnectorSession session,
            @SqlType("array(F)") Block array)
            throws Throwable
    {
        int positionCount = array.getPositionCount();
        BlockBuilder resultBuilder = resultType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (!array.isNull(position)) {
                Long value = (Long) cast.invokeExact(session, array, position);
                if (value != null) {
                    resultType.writeLong(resultBuilder, value);
                    continue;
                }
            }
            resultBuilder.appendNull();
        }
        return resultBuilder.build();
    }

    @TypeParameter("F")
    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType("array(T)")
    public static Block filterDouble(
            @TypeParameter("T") Type resultType,
            @CastDependency(fromType = "F", toType = "T", convention = @Convention(arguments = BLOCK_POSITION, result = NULLABLE_RETURN, session = true)) MethodHandle cast,
            ConnectorSession session,
            @SqlType("array(F)") Block array)
            throws Throwable
    {
        int positionCount = array.getPositionCount();
        BlockBuilder resultBuilder = resultType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (!array.isNull(position)) {
                Double value = (Double) cast.invokeExact(session, array, position);
                if (value != null) {
                    resultType.writeDouble(resultBuilder, value);
                    continue;
                }
            }
            resultBuilder.appendNull();
        }
        return resultBuilder.build();
    }

    @TypeParameter("F")
    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType("array(T)")
    public static Block filterBoolean(
            @TypeParameter("T") Type resultType,
            @CastDependency(fromType = "F", toType = "T", convention = @Convention(arguments = BLOCK_POSITION, result = NULLABLE_RETURN, session = true)) MethodHandle cast,
            ConnectorSession session,
            @SqlType("array(F)") Block array)
            throws Throwable
    {
        int positionCount = array.getPositionCount();
        BlockBuilder resultBuilder = resultType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (!array.isNull(position)) {
                Boolean value = (Boolean) cast.invokeExact(session, array, position);
                if (value != null) {
                    resultType.writeBoolean(resultBuilder, value);
                    continue;
                }
            }
            resultBuilder.appendNull();
        }
        return resultBuilder.build();
    }

    @TypeParameter("F")
    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Object.class)
    @SqlType("array(T)")
    public static Block filterObject(
            @TypeParameter("T") Type resultType,
            @CastDependency(fromType = "F", toType = "T", convention = @Convention(arguments = BLOCK_POSITION, result = NULLABLE_RETURN, session = true)) MethodHandle cast,
            ConnectorSession session,
            @SqlType("array(F)") Block array)
            throws Throwable
    {
        int positionCount = array.getPositionCount();
        BlockBuilder resultBuilder = resultType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (!array.isNull(position)) {
                Object value = (Object) cast.invoke(session, array, position);
                if (value != null) {
                    resultType.writeObject(resultBuilder, value);
                    continue;
                }
            }
            resultBuilder.appendNull();
        }
        return resultBuilder.build();
    }
}
