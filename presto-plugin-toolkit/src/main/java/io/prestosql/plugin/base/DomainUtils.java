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
package io.prestosql.plugin.base;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class DomainUtils
{
    private DomainUtils() {}

    public static <T> Optional<Set<String>> extractStringValues(TupleDomain<T> constraint, T column)
    {
        if (constraint.isNone()) {
            return Optional.of(ImmutableSet.of());
        }

        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return Optional.empty();
        }

        return extractStringValues(domain)
                .map(values -> values.stream()
                        .map(Slice::toStringUtf8)
                        .collect(toImmutableSet()));
    }

    public static Optional<Set<Slice>> extractStringValues(Domain domain)
    {
        requireNonNull(domain, "domain is null");
        if (domain.isAll()) {
            return Optional.empty();
        }
        if (domain.isSingleValue()) {
            return Optional.of(ImmutableSet.of(((Slice) domain.getSingleValue())));
        }
        if (domain.getValues() instanceof EquatableValueSet) {
            Collection<Object> values = ((EquatableValueSet) domain.getValues()).getValues();
            return Optional.of(values.stream()
                    .map(Slice.class::cast)
                    .collect(toImmutableSet()));
        }
        if (domain.getValues() instanceof SortedRangeSet) {
            ImmutableSet.Builder<Slice> result = ImmutableSet.builder();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                if (domain.isAll()) {
                    return Optional.empty();
                }
                if (!range.isSingleValue()) {
                    return Optional.empty();
                }

                result.add(((Slice) range.getSingleValue()));
            }

            return Optional.of(result.build());
        }
        return Optional.empty();
    }

    public static <T> Optional<String> tryGetSingleVarcharValue(TupleDomain<T> constraint, T index)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        Domain domain = constraint.getDomains().get().get(index);
        if ((domain == null) || !domain.isSingleValue()) {
            return Optional.empty();
        }

        Object value = domain.getSingleValue();
        return Optional.of(((Slice) value).toStringUtf8());
    }
}
