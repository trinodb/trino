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
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.TypeParameterSpecialization;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

@Description("Returns true if all elements of the array don't match the given predicate")
@ScalarFunction("none_match")
public final class ArrayNoneMatchFunction
{
    private ArrayNoneMatchFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Object.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchObject(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") ObjectToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchObject(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchLong(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchDouble(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean noneMatchBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        Boolean anyMatchResult = ArrayAnyMatchFunction.anyMatchBoolean(elementType, arrayBlock, function);
        if (anyMatchResult == null) {
            return null;
        }
        return !anyMatchResult;
    }
}
