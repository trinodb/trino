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
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.bson.types.ObjectId;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.function.OperatorType.CAST;
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
}
