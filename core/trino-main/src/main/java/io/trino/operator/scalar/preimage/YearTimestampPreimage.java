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
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.operator.scalar.timestamp.ExtractYear.extract;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;

public final class YearTimestampPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        if (context.signature().getArgumentTypes().size() != 1 ||
                !(context.signature().getArgumentTypes().getFirst() instanceof TimestampType type) || !resultDomain.getType().equals(BIGINT)) {
            return Optional.empty();
        }
        var bounds = type.getRange().orElseThrow();
        long minimum = type.isShort() ? extract((long) bounds.getMin()) : extract((LongTimestamp) bounds.getMin());
        long maximum = type.isShort() ? extract((long) bounds.getMax()) : extract((LongTimestamp) bounds.getMax());
        ValueSet years = resultDomain.getValues().intersect(ValueSet.ofRanges(Range.range(BIGINT, minimum, true, maximum, true)));
        List<Range> ranges = new ArrayList<>();
        for (Range range : years.getRanges().getOrderedRanges()) {
            long first = (long) range.getLowBoundedValue() + (range.isLowInclusive() ? 0 : 1);
            long last = (long) range.getHighBoundedValue() - (range.isHighInclusive() ? 0 : 1);
            if (first > last) {
                continue;
            }
            ValueSet values = ValueSet.all(type);
            if (first != minimum) {
                values = values.intersect(ValueSet.ofRanges(Range.greaterThanOrEqual(type, start(type, first))));
            }
            if (last != maximum) {
                values = values.intersect(ValueSet.ofRanges(Range.lessThan(type, start(type, last + 1))));
            }
            ranges.addAll(values.getRanges().getOrderedRanges());
        }
        return Optional.of(new PreimageResult(Domain.create(ValueSet.copyOfRanges(type, ranges), resultDomain.isNullAllowed()), EXACT));
    }

    private static Object start(TimestampType type, long year)
    {
        long micros = multiplyExact(LocalDate.of(toIntExact(year), 1, 1).toEpochDay(), 86_400_000_000L);
        return type.isShort() ? micros : new LongTimestamp(micros, 0);
    }
}
