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
package io.prestosql.type.coercions;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.String.format;

public class TimestampCoercers
{
    private TimestampCoercers() {}

    public static TypeCoercer<TimestampType, ?> createCoercer(TimestampType sourceType, Type targetType)
    {
        if (targetType instanceof TimestampWithTimeZoneType) {
            return TypeCoercer.create(sourceType, (TimestampWithTimeZoneType) targetType, TimestampCoercers::coerceTimestampToZonedTimestamp);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
    }

    private static void coerceTimestampToZonedTimestamp(TimestampType sourceType, TimestampWithTimeZoneType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeLong(blockBuilder, packDateTimeWithZone(sourceType.getLong(block, position), UTC_KEY));
    }
}
