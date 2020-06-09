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
package io.prestosql.plugin.mongodb;

import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.bson.types.ObjectId;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class ObjectIdFunctions
{
    private ObjectIdFunctions() {}

    @Description("Mongodb ObjectId")
    @ScalarFunction
    @SqlType("ObjectId")
    public static Slice objectid()
    {
        return Slices.wrappedBuffer(new ObjectId().toByteArray());
    }

    @Description("Mongodb ObjectId from the given string")
    @ScalarFunction
    @SqlType("ObjectId")
    public static Slice objectid(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return Slices.wrappedBuffer(new ObjectId(CharMatcher.is(' ').removeFrom(value.toStringUtf8())).toByteArray());
    }

    @ScalarFunction
    @SqlType("timestamp(3) with time zone") // ObjectId's timestamp is a point in time
    public static long objectidTimestamp(@SqlType("ObjectId") Slice value)
    {
        int epochSeconds = new ObjectId(value.getBytes()).getTimestamp();
        return packDateTimeWithZone(SECONDS.toMillis(epochSeconds), UTC_KEY);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType("ObjectId") Slice value)
    {
        String hexString = new ObjectId(value.getBytes()).toString();
        if (hexString.length() > x) {
            hexString = hexString.substring(0, toIntExact(x));
        }
        return utf8Slice(hexString);
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(@SqlType("ObjectId") Slice left, @IsNull boolean leftNull, @SqlType("ObjectId") Slice right, @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return notEqual(left, right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) >= 0;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType("ObjectId") Slice left, @SqlType("ObjectId") Slice right)
    {
        return compareTo(left, right) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("ObjectId") Slice value)
    {
        return new ObjectId(value.getBytes()).hashCode();
    }

    private static int compareTo(Slice left, Slice right)
    {
        return new ObjectId(left.getBytes()).compareTo(new ObjectId(right.getBytes()));
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType("ObjectId") Slice value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType("ObjectId") Slice value)
    {
        return XxHash64.hash(value);
    }
}
