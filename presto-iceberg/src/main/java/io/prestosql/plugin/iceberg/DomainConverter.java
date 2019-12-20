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
package io.prestosql.plugin.iceberg;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.EquatableValueSet.ValueEntry;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.predicate.Utils.nativeValueToBlock;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class DomainConverter
{
    private DomainConverter() {}

    public static TupleDomain<IcebergColumnHandle> convertTupleDomainTypes(TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll() || tupleDomain.isNone()) {
            return tupleDomain;
        }
        if (!tupleDomain.getDomains().isPresent()) {
            return tupleDomain;
        }

        Map<IcebergColumnHandle, Domain> transformedMap = new HashMap<>();
        tupleDomain.getDomains().get().forEach((column, domain) -> {
            ValueSet valueSet = domain.getValues();
            ValueSet transformedValueSet = valueSet;
            Type type = domain.getType();
            if (type instanceof TimestampType || type instanceof TimestampWithTimeZoneType || type instanceof TimeType || type instanceof TimeWithTimeZoneType) {
                if (valueSet instanceof EquatableValueSet) {
                    EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
                    Set<ValueEntry> values = equatableValueSet.getEntries().stream()
                            .map(value -> ValueEntry.create(value.getType(), convertToMicros(type, (long) value.getValue())))
                            .collect(toImmutableSet());
                    transformedValueSet = new EquatableValueSet(equatableValueSet.getType(), equatableValueSet.isWhiteList(), values);
                }
                else if (valueSet instanceof SortedRangeSet) {
                    List<Range> ranges = new ArrayList<>();
                    for (Range range : valueSet.getRanges().getOrderedRanges()) {
                        Marker low = range.getLow();
                        if (low.getValueBlock().isPresent()) {
                            Block value = nativeValueToBlock(type, convertToMicros(type, (long) range.getLow().getValue()));
                            low = new Marker(range.getType(), Optional.of(value), range.getLow().getBound());
                        }

                        Marker high = range.getHigh();
                        if (high.getValueBlock().isPresent()) {
                            Block value = nativeValueToBlock(type, convertToMicros(type, (long) range.getHigh().getValue()));
                            high = new Marker(range.getType(), Optional.of(value), range.getHigh().getBound());
                        }

                        ranges.add(new Range(low, high));
                    }
                    transformedValueSet = SortedRangeSet.copyOf(valueSet.getType(), ranges);
                }
                transformedMap.put(column, Domain.create(transformedValueSet, domain.isNullAllowed()));
            }
        });
        return TupleDomain.withColumnDomains(transformedMap);
    }

    private static long convertToMicros(Type type, long value)
    {
        if (type instanceof TimestampWithTimeZoneType || type instanceof TimeWithTimeZoneType) {
            return MILLISECONDS.toMicros(unpackMillisUtc(value));
        }

        if (type instanceof TimestampType || type instanceof TimeType) {
            return MILLISECONDS.toMicros(value);
        }

        throw new IllegalArgumentException(type + " is unsupported");
    }
}
