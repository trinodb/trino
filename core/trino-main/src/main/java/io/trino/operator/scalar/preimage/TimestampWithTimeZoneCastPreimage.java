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

import io.trino.spi.TrinoException;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;

public final class TimestampWithTimeZoneCastPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        if (!CastPreimages.eligible(context, source, target)) {
            return Optional.empty();
        }
        Comparator<Object> comparison = context.functions().resultComparator();
        try {
            return CastPreimages.compute(() -> CastPreimages.orderedRanges(
                    resultDomain,
                    source,
                    (value, lower, inclusive) -> bound(context, source, target, comparison, value, lower, inclusive)));
        }
        catch (IllegalArgumentException e) {
            // Packed timestamp-with-zone bounds have a smaller range than epoch millis.
            if (!e.getMessage().startsWith("Millis overflow:")) {
                throw e;
            }
            return Optional.empty();
        }
    }

    private static Optional<ValueSet> bound(Context context, Type source, Type target, Comparator<Object> comparison, Object value, boolean lower, boolean inclusive)
    {
        TimestampWithTimeZoneType timestamp = (TimestampWithTimeZoneType) target;
        if (!injectiveAt(context, timestamp, value)) {
            return Optional.empty();
        }
        value = timestamp.isShort()
                ? packDateTimeWithZone(unpackMillisUtc((long) value), context.session().getTimeZoneKey())
                : LongTimestampWithTimeZone.fromEpochMillisAndFraction(((LongTimestampWithTimeZone) value).getEpochMillis(), ((LongTimestampWithTimeZone) value).getPicosOfMilli(), context.session().getTimeZoneKey());
        Optional<Function<Object, Object>> forward = context.functions().coercion(source, target);
        Optional<Function<Object, Object>> reverse = context.functions().coercion(target, source);
        if (forward.isEmpty() || reverse.isEmpty()) {
            return Optional.empty();
        }
        Object input;
        try {
            input = reverse.get().apply(value);
        }
        catch (TrinoException e) {
            if (!CastPreimages.conversionFailure(e)) {
                throw e;
            }
            return Optional.empty();
        }
        if (source.equals(DATE)) {
            // The DATE cast samples the offset at UTC midnight. Its result can fall
            // on a different local date, so the reverse cast need not find the boundary.
            Optional<Object> previous = DATE.getPreviousValue(input);
            Optional<Object> next = DATE.getNextValue(input);
            if ((previous.isPresent() && comparison.compare(forward.get().apply(previous.get()), value) >= 0) ||
                    (next.isPresent() && comparison.compare(forward.get().apply(next.get()), value) <= 0)) {
                return Optional.empty();
            }
        }
        return Optional.of(CastPreimages.roundTripBound(source, input, comparison.compare(forward.get().apply(input), value), lower, inclusive));
    }

    static boolean injectiveAt(Context context, TimestampWithTimeZoneType timestamp, Object value)
    {
        Instant instant = timestamp.isShort()
                ? Instant.ofEpochMilli(unpackMillisUtc((long) value))
                : Instant.ofEpochMilli(((LongTimestampWithTimeZone) value).getEpochMillis()).plusNanos(((LongTimestampWithTimeZone) value).getPicosOfMilli() / 1000);
        return isTimestampToTimestampWithTimeZoneInjectiveAt(context.session().getTimeZoneKey().getZoneId(), instant);
    }

    public static boolean isTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId zone, Instant instant)
    {
        ZoneRules rules = zone.getRules();
        if (rules.isFixedOffset()) {
            return true;
        }
        // Regional rules before 1970 can differ from those used by the legacy casts.
        // Decline these bounds rather than infer an exact preimage from different rules.
        if (instant.isBefore(Instant.EPOCH)) {
            return false;
        }
        ZoneOffsetTransition previous = rules.previousTransition(instant.plusNanos(1));
        if (previous != null && instant.isBefore(previous.getInstant().plus(previous.getDuration().abs()))) {
            // After either a gap or an overlap, decline the interval covered by the offset change.
            return false;
        }
        ZoneOffsetTransition next = rules.nextTransition(instant);
        // Also decline the first occurrence of an overlap, before the transition.
        return next == null || !next.isOverlap() || instant.isBefore(next.getInstant().plus(next.getDuration()));
    }
}
