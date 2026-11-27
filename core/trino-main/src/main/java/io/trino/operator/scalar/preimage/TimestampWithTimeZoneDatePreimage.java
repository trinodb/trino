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
package io.trino.operator.scalar.preimage;

import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;

/// Conservative preimage of `CAST(timestamp(p) with time zone AS date)`.
///
/// The cast returns the calendar date a value shows in the zone the value carries. A
/// `timestamp with time zone` domain orders values by instant alone, so it cannot select
/// values by zone, and the same instant casts to different dates in different zones. The
/// offset of a zone from UTC is always shorter than one day, so a value that casts to date
/// `d` lies after UTC midnight of `d - 1` and before UTC midnight of `d + 2`. The preimage of
/// a range of dates is that range of instants, widened by one day on each side. It is a
/// superset of the matching values, so the provider never answers a request for an exact result.
public final class TimestampWithTimeZoneDatePreimage
        implements DomainPreimage
{
    private static final long MILLISECONDS_PER_DAY = 86_400_000;

    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        if (!(context.signature().getArgumentTypes().getFirst() instanceof TimestampWithTimeZoneType source) || !resultDomain.getType().equals(DATE)) {
            return Optional.empty();
        }
        if (context.requiredExactness() == EXACT) {
            return Optional.empty();
        }
        List<Range> ranges = new ArrayList<>();
        for (Range range : resultDomain.getValues().getRanges().getOrderedRanges()) {
            Optional<Long> first = range.isLowUnbounded() ? Optional.empty() : Optional.of((long) range.getLowBoundedValue() + (range.isLowInclusive() ? 0 : 1));
            Optional<Long> last = range.isHighUnbounded() ? Optional.empty() : Optional.of((long) range.getHighBoundedValue() - (range.isHighInclusive() ? 0 : 1));
            if (first.isPresent() && last.isPresent() && first.get() > last.get()) {
                // an open range between two adjacent dates contains no date
                continue;
            }
            Optional<Object> low = first.flatMap(day -> utcMidnight(source, day - 1));
            Optional<Object> high = last.flatMap(day -> utcMidnight(source, day + 2));
            if (low.isPresent() && high.isPresent()) {
                ranges.add(Range.range(source, low.get(), true, high.get(), false));
            }
            else if (low.isPresent()) {
                ranges.add(Range.greaterThanOrEqual(source, low.get()));
            }
            else if (high.isPresent()) {
                ranges.add(Range.lessThan(source, high.get()));
            }
            else {
                ranges.add(Range.all(source));
            }
        }
        return Optional.of(new PreimageResult(Domain.create(ValueSet.copyOfRanges(source, ranges), resultDomain.isNullAllowed()), CONSERVATIVE));
    }

    /// UTC midnight of the given day, or empty when `timestamp with time zone` cannot represent
    /// it. No value lies beyond an instant the type cannot represent, so the caller leaves that
    /// side of the range unbounded.
    private static Optional<Object> utcMidnight(TimestampWithTimeZoneType type, long epochDay)
    {
        long epochMillis = epochDay * MILLISECONDS_PER_DAY;
        try {
            return Optional.of(type.isShort()
                    ? packDateTimeWithZone(epochMillis, UTC_KEY)
                    : LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, UTC_KEY));
        }
        catch (IllegalArgumentException e) {
            // The zone is packed into the low bits of the epoch millis, which shortens their range
            if (!e.getMessage().startsWith("Millis overflow:")) {
                throw e;
            }
            return Optional.empty();
        }
    }
}
