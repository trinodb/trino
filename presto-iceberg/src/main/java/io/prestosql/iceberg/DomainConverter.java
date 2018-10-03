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
package io.prestosql.iceberg;

import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.EquatableValueSet.ValueEntry;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.Utils;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

public class DomainConverter
{
    private DomainConverter()
    {}

    public static TupleDomain<HiveColumnHandle> handleTypeDifference(TupleDomain<HiveColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll() || tupleDomain.isNone()) {
            return tupleDomain;
        }
        else {
            if (!tupleDomain.getDomains().isPresent()) {
                return tupleDomain;
            }

            Map<HiveColumnHandle, Domain> tDomainMap = tupleDomain.getDomains().get();
            Map<HiveColumnHandle, Domain> tDomainTransformedMap = new HashMap<>();
            for (Map.Entry<HiveColumnHandle, Domain> tDomainEntry : tDomainMap.entrySet()) {
                Domain domain = tDomainEntry.getValue();
                ValueSet valueSet = domain.getValues();
                ValueSet transformedValueSet = valueSet;
                Type type = domain.getType();
                String baseType = type.getTypeSignature().getBase();
                if (type.equals(TIMESTAMP) || type.equals(TIMESTAMP_WITH_TIME_ZONE) || type.equals(TIME)
                        || type.equals(TIME_WITH_TIME_ZONE)) {
                    if (valueSet instanceof EquatableValueSet) {
                        EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
                        Set<ValueEntry> values = equatableValueSet.getEntries().stream().map(
                                v -> ValueEntry.create(v.getType(), convertToMicros(baseType, (long) v.getValue()))).collect(Collectors.toSet());
                        transformedValueSet = new EquatableValueSet(equatableValueSet.getType(), equatableValueSet.isWhiteList(), values);
                    }
                    else if (valueSet instanceof SortedRangeSet) {
                        List<Range> ranges = new ArrayList<>();
                        for (Range range : valueSet.getRanges().getOrderedRanges()) {
                            Marker low = range.getLow();
                            if (low.getValueBlock().isPresent()) {
                                low = new Marker(range.getType(), Optional.of(Utils.nativeValueToBlock(type, convertToMicros(baseType, (Long) range.getLow().getValue()))), range.getLow().getBound());
                            }

                            Marker high = range.getHigh();
                            if (high.getValueBlock().isPresent()) {
                                high = new Marker(range.getType(), Optional.of(Utils.nativeValueToBlock(type, convertToMicros(baseType, (Long) range.getHigh().getValue()))), range.getHigh().getBound());
                            }

                            ranges.add(new Range(low, high));
                        }
                        transformedValueSet = SortedRangeSet.copyOf(valueSet.getType(), ranges);
                    }
                    tDomainTransformedMap.put(tDomainEntry.getKey(), Domain.create(transformedValueSet, domain.isNullAllowed()));
                }
            }
            return TupleDomain.withColumnDomains(tDomainTransformedMap);
        }
    }

    private static Long convertToMicros(String baseType, long value)
    {
        switch (baseType) {
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return TimeUnit.MILLISECONDS.toMicros(DateTimeEncoding.unpackMillisUtc(value));
            case StandardTypes.TIME:
            case StandardTypes.TIMESTAMP:
                return TimeUnit.MILLISECONDS.toMicros(value);
            default:
                throw new IllegalArgumentException(baseType + " is unsupported");
        }
    }
}
