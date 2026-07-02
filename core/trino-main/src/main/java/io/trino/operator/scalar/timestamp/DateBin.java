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
package io.trino.operator.scalar.timestamp;

import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.floorDiv;

@Description("Bins the input timestamp by the given stride interval aligned to the given origin")
@ScalarFunction("date_bin")
public final class DateBin
{
    private DateBin() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long bin(
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long stride,
            @SqlType("timestamp(p)") long source,
            @SqlType("timestamp(p)") long origin)
    {
        long strideMicros = stride * 1_000L;
        if (strideMicros <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "stride must be positive");
        }
        return origin + floorDiv(source - origin, strideMicros) * strideMicros;
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static LongTimestamp bin(
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long stride,
            @SqlType("timestamp(p)") LongTimestamp source,
            @SqlType("timestamp(p)") LongTimestamp origin)
    {
        long strideMicros = stride * 1_000L;
        if (strideMicros <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "stride must be positive");
        }
        long epochMicros = origin.getEpochMicros() + floorDiv(source.getEpochMicros() - origin.getEpochMicros(), strideMicros) * strideMicros;
        return new LongTimestamp(epochMicros, 0);
    }
}
