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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.DateType.DATE;

public final class TimestampDatePreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        return CastPreimages.compute(() -> project(context, resultDomain));
    }

    private static Optional<PreimageResult> project(Context context, Domain resultDomain)
    {
        if (!(context.signature().getArgumentTypes().getFirst() instanceof TimestampType source) || !resultDomain.getType().equals(DATE)) {
            return Optional.empty();
        }
        var conversion = context.functions().coercion(DATE, source).orElseThrow();
        var bounds = source.getRange().orElseThrow();
        long minimum = Math.floorDiv(source.isShort() ? (long) bounds.getMin() : ((LongTimestamp) bounds.getMin()).getEpochMicros(), 86_400_000_000L);
        long maximum = Math.floorDiv(source.isShort() ? (long) bounds.getMax() : ((LongTimestamp) bounds.getMax()).getEpochMicros(), 86_400_000_000L);
        ValueSet dates = resultDomain.getValues().intersect(ValueSet.ofRanges(Range.range(DATE, minimum, true, maximum, true)));
        List<Range> ranges = new ArrayList<>();
        for (Range range : dates.getRanges().getOrderedRanges()) {
            long first = (long) range.getLowBoundedValue() + (range.isLowInclusive() ? 0 : 1);
            long last = (long) range.getHighBoundedValue() - (range.isHighInclusive() ? 0 : 1);
            if (first > last) {
                continue;
            }
            ValueSet projected = ValueSet.all(source);
            if (first != minimum) {
                long day = first;
                projected = projected.intersect(ValueSet.ofRanges(Range.greaterThanOrEqual(source, conversion.apply(day))));
            }
            if (last != maximum) {
                long day = last + 1;
                projected = projected.intersect(ValueSet.ofRanges(Range.lessThan(source, conversion.apply(day))));
            }
            ranges.addAll(projected.getRanges().getOrderedRanges());
        }
        return Optional.of(new PreimageResult(Domain.create(ValueSet.copyOfRanges(source, ranges), resultDomain.isNullAllowed()), EXACT));
    }
}
