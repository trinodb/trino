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
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.DateType.DATE;
import static java.lang.Math.max;
import static java.lang.Math.min;

/// Bounds the strings that `CAST(varchar AS date)` maps into a set of dates.
///
/// The cast trims whitespace and accepts an optional sign, a year of any number of digits,
/// and a month and day of one or two digits each. A string whose first character is a digit
/// from `1` to `9` is therefore neither trimmed nor signed, and its year is exactly its
/// leading run of digits, which has no leading zero. Any other first character can start a
/// string that casts to any date. The preimage contains every string of the second kind, and
/// the strings of the first kind whose year, or for a single date whose year, month, and day
/// spelling, can produce a matching date. Because one date has several spellings, and the
/// preimage of a range includes whole years, the result is a superset.
///
/// String order does not follow year order across year lengths: `'305-01-01'` sorts between
/// `'2299'` and `'2301'`. A range unbounded on either side therefore includes years of every
/// length, which leaves nearly every string, and the provider declines it.
///
/// Update this provider whenever the cast's parsing rules change.
public final class VarcharToDatePreimage
        implements DomainPreimage
{
    private static final int MAX_YEAR = LocalDate.ofEpochDay(Integer.MAX_VALUE).getYear();

    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        if (!(context.signature().getArgumentTypes().getFirst() instanceof VarcharType source) || !resultDomain.getType().equals(DATE)) {
            return Optional.empty();
        }
        ValueSet dates = resultDomain.getValues();
        // Every string that casts successfully produces some date, and the empty string, the
        // only value of varchar(0), never casts successfully
        if (dates.isNone() || (!source.isUnbounded() && source.getBoundedLength() == 0)) {
            return Optional.of(new PreimageResult(Domain.create(ValueSet.none(source), resultDomain.isNullAllowed()), EXACT));
        }
        if (context.requiredExactness() == EXACT || dates.isAll()) {
            return Optional.empty();
        }
        ValueSet strings;
        ValueSet excluded = dates.complement();
        if (excluded.isDiscreteSet()) {
            strings = ValueSet.all(source).subtract(ValueSet.copyOfRanges(source, exactSpellings(source, excluded.getDiscreteSet())));
        }
        else {
            List<Range> ranges = new ArrayList<>(unsignedPrefixes(source));
            for (Range range : dates.getRanges().getOrderedRanges()) {
                if (range.isLowUnbounded() || range.isHighUnbounded()) {
                    // Years of every length below or above a bound span nearly every string
                    return Optional.empty();
                }
                ranges.addAll(range.isSingleValue()
                        ? spellings(source, LocalDate.ofEpochDay((long) range.getSingleValue()))
                        : years(source, range));
            }
            strings = ValueSet.copyOfRanges(source, ranges);
        }
        return Optional.of(new PreimageResult(Domain.create(strings, resultDomain.isNullAllowed()), CONSERVATIVE));
    }

    /// Strings whose first character is not a digit from `1` to `9`
    private static List<Range> unsignedPrefixes(VarcharType source)
    {
        return List.of(Range.lessThan(source, utf8Slice("1")), Range.greaterThanOrEqual(source, utf8Slice(":")));
    }

    /// Strings starting with a digit from `1` to `9` that may cast to a date in the bounded
    /// `range`: those with a year from the year of its lower bound through that of its upper bound.
    private static List<Range> years(VarcharType source, Range range)
    {
        long first = max(1, LocalDate.ofEpochDay((long) range.getLowBoundedValue()).getYear());
        long last = min(MAX_YEAR, LocalDate.ofEpochDay((long) range.getHighBoundedValue()).getYear());
        List<Range> ranges = new ArrayList<>();
        // Years of one length are ordered as strings. A string starting with year y and the
        // separator falls between the first and last years of its length, followed by "-".
        for (long low = first; low <= last; low = nextPowerOfTen(low)) {
            long high = min(last, nextPowerOfTen(low) - 1);
            ranges.add(range(source, low + "-", high + "."));
        }
        return ranges;
    }

    private static long nextPowerOfTen(long value)
    {
        long power = 10;
        while (power <= value) {
            power *= 10;
        }
        return power;
    }

    /// Strings starting with a digit from `1` to `9` that may cast to `date`: those that start
    /// with one of its spellings with an optional zero before a single-digit month or day.
    private static List<Range> spellings(VarcharType source, LocalDate date)
    {
        if (date.getYear() < 1) {
            return List.of();
        }
        return spellingPrefixes(date, true).stream()
                .map(prefix -> prefixRange(source, prefix))
                .toList();
    }

    /// Strings that cast to one of `dates` or fail. A spelling ending with a two-digit day
    /// qualifies: any further digit makes the day invalid. A one-digit day does not, because a
    /// further digit can form another valid day. A spelling longer than the source type is
    /// skipped: no value of the type starts with it, and shortening it to fit would remove
    /// strings that cast to other dates.
    private static List<Range> exactSpellings(VarcharType source, List<Object> dates)
    {
        List<Range> ranges = new ArrayList<>();
        for (Object value : dates) {
            LocalDate date = LocalDate.ofEpochDay((long) value);
            if (date.getYear() >= 1) {
                for (String prefix : spellingPrefixes(date, false)) {
                    if (source.isUnbounded() || prefix.length() <= source.getBoundedLength()) {
                        ranges.add(prefixRange(source, prefix));
                    }
                }
            }
        }
        return ranges;
    }

    private static List<String> spellingPrefixes(LocalDate date, boolean singleDigitDay)
    {
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();
        List<String> months = month < 10 ? List.of(Integer.toString(month), "0" + month) : List.of(Integer.toString(month));
        List<String> days = new ArrayList<>();
        if (day < 10) {
            if (singleDigitDay) {
                days.add(Integer.toString(day));
            }
            days.add("0" + day);
        }
        else {
            days.add(Integer.toString(day));
        }
        List<String> prefixes = new ArrayList<>();
        for (String monthText : months) {
            for (String dayText : days) {
                prefixes.add(date.getYear() + "-" + monthText + "-" + dayText);
            }
        }
        return prefixes;
    }

    /// Strings starting with `prefix`. The prefix ends with a digit, so incrementing it cannot overflow.
    private static Range prefixRange(VarcharType source, String prefix)
    {
        String next = prefix.substring(0, prefix.length() - 1) + (char) (prefix.charAt(prefix.length() - 1) + 1);
        return range(source, prefix, next);
    }

    /// Strings from `low` inclusive to `high` exclusive. A bound longer than the source type
    /// allows is replaced by a shorter one that keeps every value of the type in range: the
    /// lower bound by its truncation, which sorts before it, and the upper bound by its
    /// truncation with the last character incremented, which sorts after it. The result can
    /// only grow, so callers must use it for included strings, never for excluded ones.
    private static Range range(VarcharType source, String low, String high)
    {
        if (source.isUnbounded() || source.getBoundedLength() >= max(low.length(), high.length())) {
            return Range.range(source, utf8Slice(low), true, utf8Slice(high), false);
        }
        int length = source.getBoundedLength();
        String lowBound = low.substring(0, min(length, low.length()));
        if (high.length() <= length) {
            return Range.range(source, utf8Slice(lowBound), true, utf8Slice(high), false);
        }
        String truncated = high.substring(0, length);
        String highBound = truncated.substring(0, length - 1) + (char) (truncated.charAt(length - 1) + 1);
        return Range.range(source, utf8Slice(lowBound), true, utf8Slice(highBound), false);
    }
}
