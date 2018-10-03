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

import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.EquatableValueSet.ValueEntry;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.StandardTypes;
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
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class DomainConverter
{
    private DomainConverter() {}

    public static TupleDomain<HiveColumnHandle> convertTupleDomainTypes(TupleDomain<HiveColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll() || tupleDomain.isNone()) {
            return tupleDomain;
        }
        if (!tupleDomain.getDomains().isPresent()) {
            return tupleDomain;
        }

        Map<HiveColumnHandle, Domain> transformedMap = new HashMap<>();
        tupleDomain.getDomains().get().forEach((column, domain) -> {
            ValueSet valueSet = domain.getValues();
            ValueSet transformedValueSet = valueSet;
            Type type = domain.getType();
            String baseType = type.getTypeSignature().getBase();
            if (type.equals(TIMESTAMP) || type.equals(TIMESTAMP_WITH_TIME_ZONE) || type.equals(TIME) || type.equals(TIME_WITH_TIME_ZONE)) {
                if (valueSet instanceof EquatableValueSet) {
                    EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
                    Set<ValueEntry> values = equatableValueSet.getEntries().stream()
                            .map(value -> ValueEntry.create(value.getType(), convertToMicros(baseType, (long) value.getValue())))
                            .collect(toImmutableSet());
                    transformedValueSet = new EquatableValueSet(equatableValueSet.getType(), equatableValueSet.isWhiteList(), values);
                }
                else if (valueSet instanceof SortedRangeSet) {
                    List<Range> ranges = new ArrayList<>();
                    for (Range range : valueSet.getRanges().getOrderedRanges()) {
                        Marker low = range.getLow();
                        if (low.getValueBlock().isPresent()) {
                            Block value = nativeValueToBlock(type, convertToMicros(baseType, (long) range.getLow().getValue()));
                            low = new Marker(range.getType(), Optional.of(value), range.getLow().getBound());
                        }

                        Marker high = range.getHigh();
                        if (high.getValueBlock().isPresent()) {
                            Block value = nativeValueToBlock(type, convertToMicros(baseType, (long) range.getHigh().getValue()));
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

    private static long convertToMicros(String type, long value)
    {
        switch (type) {
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return MILLISECONDS.toMicros(unpackMillisUtc(value));
            case StandardTypes.TIME:
            case StandardTypes.TIMESTAMP:
                return MILLISECONDS.toMicros(value);
        }
        throw new IllegalArgumentException(type + " is unsupported");
    }
}
