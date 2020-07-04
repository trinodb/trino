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
package io.prestosql.operator.scalar;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.operator.scalar.ArrayMinMaxUtils.booleanArrayMinMax;
import static io.prestosql.operator.scalar.ArrayMinMaxUtils.doubleArrayMinMax;
import static io.prestosql.operator.scalar.ArrayMinMaxUtils.longArrayMinMax;
import static io.prestosql.operator.scalar.ArrayMinMaxUtils.objectArrayMinMax;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;

@ScalarFunction("array_max")
@Description("Get maximum value of array")
public final class ArrayMaxFunction
{
    private ArrayMaxFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Long longArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return longArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Boolean booleanArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return booleanArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Double doubleArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return doubleArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Object sliceArrayMax(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return objectArrayMinMax(compareMethodHandle, elementType, block);
    }
}
