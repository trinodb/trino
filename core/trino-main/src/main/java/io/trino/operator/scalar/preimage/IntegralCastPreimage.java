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
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

public final class IntegralCastPreimage
        implements DomainPreimage
{
    private static final List<Type> INTEGRAL_TYPES = List.of(TINYINT, SMALLINT, INTEGER, BIGINT);

    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        if (context.signature().getArgumentTypes().size() != 1) {
            return Optional.empty();
        }
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        int sourceRank = INTEGRAL_TYPES.indexOf(source);
        if (sourceRank < 0 || sourceRank >= INTEGRAL_TYPES.indexOf(target)) {
            return Optional.empty();
        }
        Type.Range bounds = source.getRange().orElseThrow();
        long minimum = (long) bounds.getMin();
        long maximum = (long) bounds.getMax();
        ValueSet values = resultDomain.getValues().intersect(ValueSet.ofRanges(Range.range(target, minimum, true, maximum, true)));
        List<Range> ranges = new ArrayList<>();
        for (Range range : values.getRanges().getOrderedRanges()) {
            long low = (long) range.getLowBoundedValue();
            long high = (long) range.getHighBoundedValue();
            if (low + (range.isLowInclusive() ? 0 : 1) > high - (range.isHighInclusive() ? 0 : 1)) {
                continue;
            }
            boolean lowUnbounded = low == minimum && range.isLowInclusive();
            boolean highUnbounded = high == maximum && range.isHighInclusive();
            if (lowUnbounded && highUnbounded) {
                ranges.add(Range.all(source));
            }
            else if (lowUnbounded) {
                ranges.add(range.isHighInclusive() ? Range.lessThanOrEqual(source, high) : Range.lessThan(source, high));
            }
            else if (highUnbounded) {
                ranges.add(range.isLowInclusive() ? Range.greaterThanOrEqual(source, low) : Range.greaterThan(source, low));
            }
            else {
                ranges.add(Range.range(source, low, range.isLowInclusive(), high, range.isHighInclusive()));
            }
        }
        return Optional.of(new PreimageResult(Domain.create(ValueSet.copyOfRanges(source, ranges), resultDomain.isNullAllowed()), EXACT));
    }
}
