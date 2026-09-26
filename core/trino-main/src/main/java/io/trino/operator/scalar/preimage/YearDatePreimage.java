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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.operator.scalar.DateTimeFunctions.yearFromDate;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static java.lang.Math.toIntExact;

public final class YearDatePreimage
        implements DomainPreimage
{
    private static final long MIN_YEAR = yearFromDate(Integer.MIN_VALUE);
    private static final long MAX_YEAR = yearFromDate(Integer.MAX_VALUE);

    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        if (!context.signature().getArgumentTypes().equals(List.of(DATE)) || !resultDomain.getType().equals(BIGINT)) {
            return Optional.empty();
        }
        ValueSet years = resultDomain.getValues().intersect(ValueSet.ofRanges(Range.range(BIGINT, MIN_YEAR, true, MAX_YEAR, true)));
        List<Range> ranges = new ArrayList<>();
        for (Range range : years.getRanges().getOrderedRanges()) {
            long firstYear = (long) range.getLowBoundedValue() + (range.isLowInclusive() ? 0 : 1);
            long lastYear = (long) range.getHighBoundedValue() - (range.isHighInclusive() ? 0 : 1);
            if (firstYear > lastYear) {
                continue;
            }
            if (firstYear == MIN_YEAR && lastYear == MAX_YEAR) {
                ranges.add(Range.all(DATE));
            }
            else if (firstYear == MIN_YEAR) {
                ranges.add(Range.lessThan(DATE, LocalDate.of(toIntExact(lastYear + 1), 1, 1).toEpochDay()));
            }
            else if (lastYear == MAX_YEAR) {
                ranges.add(Range.greaterThanOrEqual(DATE, LocalDate.of(toIntExact(firstYear), 1, 1).toEpochDay()));
            }
            else {
                ranges.add(Range.range(
                        DATE,
                        LocalDate.of(toIntExact(firstYear), 1, 1).toEpochDay(),
                        true,
                        LocalDate.of(toIntExact(lastYear + 1), 1, 1).toEpochDay(),
                        false));
            }
        }
        return Optional.of(new PreimageResult(Domain.create(ValueSet.copyOfRanges(DATE, ranges), resultDomain.isNullAllowed()), EXACT));
    }
}
