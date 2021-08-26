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
package io.trino.plugin.mongodb;

import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.bson.types.ObjectId;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

    @Description("Timestamp from the given Mongodb ObjectId")
    @ScalarFunction
    @SqlType("timestamp(3) with time zone") // ObjectId's timestamp is a point in time
    public static long objectidTimestamp(@SqlType("ObjectId") Slice value)
    {
        int epochSeconds = new ObjectId(value.getBytes()).getTimestamp();
        return packDateTimeWithZone(SECONDS.toMillis(epochSeconds), UTC_KEY);
    }

    @Description("Mongodb ObjectId from the given timestamp")
    @ScalarFunction
    @SqlType("ObjectId")
    public static Slice timestampObjectid(@SqlType("timestamp(0) with time zone") long timestamp)
    {
        long epochSeconds = MILLISECONDS.toSeconds(unpackMillisUtc(timestamp));
        return Slices.wrappedBuffer(new ObjectId((int) epochSeconds, 0, (short) 0, 0).toByteArray());
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
}
