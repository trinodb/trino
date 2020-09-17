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
package io.prestosql.operator.scalar.timestamptz;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampWithTimeZoneType;

import static io.prestosql.operator.scalar.timestamptz.TimestampWithTimeZoneOperators.NotEqual.notEqual;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.MAX_SHORT_PRECISION;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;

@ScalarOperator(IS_DISTINCT_FROM)
public final class TimestampWithTimeZoneDistinctFromOperator
{
    // We need these because it's currently not possible to inject the fully-bound type into the methods that require them below
    private static final TimestampWithTimeZoneType SHORT_TYPE = createTimestampWithTimeZoneType(0);
    private static final TimestampWithTimeZoneType LONG_TYPE = createTimestampWithTimeZoneType(MAX_SHORT_PRECISION + 1);

    private TimestampWithTimeZoneDistinctFromOperator() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @SqlType("timestamp(p) with time zone") long left,
            @IsNull boolean leftNull,
            @SqlType("timestamp(p) with time zone") long right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return notEqual(left, right);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @BlockPosition @SqlType(value = "timestamp(p) with time zone", nativeContainerType = long.class) Block left,
            @BlockIndex int leftPosition,
            @BlockPosition @SqlType(value = "timestamp(p) with time zone", nativeContainerType = long.class) Block right,
            @BlockIndex int rightPosition)
    {
        if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
            return true;
        }
        if (left.isNull(leftPosition)) {
            return false;
        }
        return notEqual(SHORT_TYPE.getLong(left, leftPosition), SHORT_TYPE.getLong(right, rightPosition));
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFromShort(
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone left,
            @IsNull boolean leftNull,
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return notEqual(left, right);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFromLong(
            @BlockPosition @SqlType(value = "timestamp(p) with time zone", nativeContainerType = LongTimestampWithTimeZone.class) Block left,
            @BlockIndex int leftPosition,
            @BlockPosition @SqlType(value = "timestamp(p) with time zone", nativeContainerType = LongTimestampWithTimeZone.class) Block right,
            @BlockIndex int rightPosition)
    {
        if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
            return true;
        }
        if (left.isNull(leftPosition)) {
            return false;
        }
        return !LONG_TYPE.equalTo(left, leftPosition, right, rightPosition);
    }
}
