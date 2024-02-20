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
package io.trino.plugin.raptor.legacy.storage.organization;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;

public final class TemporalFunction
{
    private TemporalFunction() {}

    public static int getDay(Type type, Block block, int position)
    {
        if (type.equals(DATE)) {
            return DATE.getInt(block, position);
        }

        if (type.equals(TIMESTAMP_MILLIS)) {
            long days = floorDiv(TIMESTAMP_MILLIS.getLong(block, position), MICROSECONDS_PER_DAY);
            return toIntExact(days);
        }

        throw new IllegalArgumentException("Wrong type for temporal column: " + type);
    }

    public static int getDayFromRange(ShardRange range)
    {
        Tuple min = range.getMinTuple();
        Tuple max = range.getMaxTuple();
        checkArgument(getOnlyElement(min.getTypes()).equals(getOnlyElement(max.getTypes())), "type of min and max is not same");

        Type type = getOnlyElement(min.getTypes());
        if (type.equals(DATE)) {
            return (int) getOnlyElement(min.getValues());
        }

        if (type.equals(TIMESTAMP_MILLIS)) {
            long minValue = (long) getOnlyElement(min.getValues());
            long maxValue = (long) getOnlyElement(max.getValues());
            return determineDay(minValue, maxValue);
        }

        throw new IllegalArgumentException("Wrong type for shard range: " + type);
    }

    private static int determineDay(long rangeStart, long rangeEnd)
    {
        int startDay = toIntExact(Duration.ofMillis(rangeStart).toDays());
        int endDay = toIntExact(Duration.ofMillis(rangeEnd).toDays());
        if (startDay == endDay) {
            return startDay;
        }

        if ((endDay - startDay) > 1) {
            // range spans multiple days, return the first full day
            return startDay + 1;
        }

        // range spans two days, return the day that has the larger time range
        long millisInStartDay = Duration.ofDays(endDay).toMillis() - rangeStart;
        long millisInEndDay = rangeEnd - Duration.ofDays(endDay).toMillis();
        return (millisInStartDay >= millisInEndDay) ? startDay : endDay;
    }
}
