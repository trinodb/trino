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
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.function.TypeParameterSpecialization;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import static java.lang.Boolean.TRUE;

@Description("Returns true if the array contains one or more elements that match the given predicate")
@ScalarFunction("any_match")
public final class ArrayAnyMatchFunction
{
    private ArrayAnyMatchFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Object.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean anyMatchObject(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") ObjectToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Object element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getObject(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return true;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean anyMatchLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Long element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getLong(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return true;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean anyMatchDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Double element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getDouble(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return true;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean anyMatchBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Boolean element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getBoolean(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return true;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return false;
    }
}
