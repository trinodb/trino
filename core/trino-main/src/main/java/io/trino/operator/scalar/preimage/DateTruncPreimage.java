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

import io.airlift.slice.Slice;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.DateType.DATE;
import static java.lang.Math.floorDiv;
import static java.lang.Math.multiplyExact;

/// Ordered temporal buckets. Candidate and unit validation precede boundary evaluation.
public final class DateTruncPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        try {
            return project(context, resultDomain);
        }
        catch (ArithmeticException _) {
            // A planning constant can truncate or advance beyond the timestamp range.
            // Leave evaluation to the original expression instead of failing a valid query.
            return Optional.empty();
        }
    }

    private static Optional<PreimageResult> project(Context context, Domain resultDomain)
    {
        Type type = resultDomain.getType();
        if (context.signature().getArgumentTypes().size() != 2 || context.inputArgument() != 1 ||
                !(type.equals(DATE) || type instanceof TimestampType)) {
            return Optional.empty();
        }
        Slice unitValue = (Slice) context.arguments().getFirst().orElseThrow().getValue();
        String unit = unitValue.toStringUtf8().toLowerCase(Locale.ENGLISH);
        if (!Set.of("hour", "day", "week", "month", "quarter", "year").contains(unit) || (type.equals(DATE) && unit.equals("hour"))) {
            return Optional.empty();
        }
        List<Range> ranges = new ArrayList<>();
        for (Range range : resultDomain.getValues().getRanges().getOrderedRanges()) {
            ValueSet projected = ValueSet.all(type);
            if (!range.isLowUnbounded()) {
                Object value = range.getLowBoundedValue();
                Object floor = context.functions().invoke(List.of(unitValue, value));
                Object boundary = range.isLowInclusive() && floor.equals(value) ? floor : next(type, unit, floor);
                projected = projected.intersect(lowerBound(type, boundary));
            }
            if (!range.isHighUnbounded()) {
                Object value = range.getHighBoundedValue();
                Object floor = context.functions().invoke(List.of(unitValue, value));
                Object boundary = !range.isHighInclusive() && floor.equals(value) ? floor : next(type, unit, floor);
                projected = projected.intersect(upperBound(type, boundary));
            }
            ranges.addAll(projected.getRanges().getOrderedRanges());
        }
        return Optional.of(new PreimageResult(Domain.create(ValueSet.copyOfRanges(type, ranges), resultDomain.isNullAllowed()), EXACT));
    }

    private static ValueSet lowerBound(Type type, Object value)
    {
        if (type.equals(DATE)) {
            if ((long) value <= Integer.MIN_VALUE) {
                return ValueSet.all(type);
            }
            if ((long) value > Integer.MAX_VALUE) {
                return ValueSet.none(type);
            }
        }
        return ValueSet.ofRanges(Range.greaterThanOrEqual(type, value));
    }

    private static ValueSet upperBound(Type type, Object value)
    {
        if (type.equals(DATE)) {
            if ((long) value <= Integer.MIN_VALUE) {
                return ValueSet.none(type);
            }
            if ((long) value > Integer.MAX_VALUE) {
                return ValueSet.all(type);
            }
        }
        return ValueSet.ofRanges(Range.lessThan(type, value));
    }

    private static Object next(Type type, String unit, Object value)
    {
        LocalDateTime dateTime;
        if (type.equals(DATE)) {
            dateTime = LocalDate.ofEpochDay((long) value).atStartOfDay();
        }
        else {
            long micros = value instanceof LongTimestamp timestamp ? timestamp.getEpochMicros() : (long) value;
            dateTime = LocalDateTime.ofEpochSecond(floorDiv(micros, 1_000_000), 0, ZoneOffset.UTC);
        }
        LocalDateTime end = switch (unit) {
            case "hour" -> dateTime.plusHours(1);
            case "day" -> dateTime.plusDays(1);
            case "week" -> dateTime.plusWeeks(1);
            case "month" -> dateTime.plusMonths(1);
            case "quarter" -> dateTime.plusMonths(3);
            case "year" -> dateTime.plusYears(1);
            default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
        };
        if (type.equals(DATE)) {
            return end.toLocalDate().toEpochDay();
        }
        long micros = multiplyExact(end.toEpochSecond(ZoneOffset.UTC), 1_000_000);
        return ((TimestampType) type).isShort() ? micros : new LongTimestamp(micros, 0);
    }
}
