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
package io.trino.plugin.paimon;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

final class PaimonRowRangeExtractor
{
    private PaimonRowRangeExtractor() {}

    static Optional<List<org.apache.paimon.utils.Range>> extractRowIdRanges(TupleDomain<PaimonColumnHandle> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        if (predicate.isAll()) {
            return Optional.empty();
        }
        if (predicate.isNone()) {
            return Optional.of(List.of());
        }

        Optional<Map<PaimonColumnHandle, Domain>> optionalDomains = predicate.getDomains();
        if (optionalDomains.isEmpty()) {
            return Optional.empty();
        }

        Domain rowIdDomain = null;
        for (Map.Entry<PaimonColumnHandle, Domain> entry : optionalDomains.get().entrySet()) {
            if (!isRowIdColumn(entry.getKey())) {
                continue;
            }
            rowIdDomain = entry.getValue();
            break;
        }
        if (rowIdDomain == null) {
            return Optional.empty();
        }

        if (rowIdDomain.isAll()) {
            return Optional.empty();
        }
        if (rowIdDomain.isNullAllowed()) {
            return Optional.empty();
        }
        if (rowIdDomain.getValues().isNone()) {
            return Optional.of(List.of());
        }

        return Optional.of(toPaimonRanges(rowIdDomain.getType(), rowIdDomain.getValues().getRanges()));
    }

    static TupleDomain<PaimonColumnHandle> removeRowIdPredicate(TupleDomain<PaimonColumnHandle> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        if (predicate.isAll() || predicate.isNone()) {
            return predicate;
        }
        Map<PaimonColumnHandle, Domain> domains = predicate.getDomains().orElseThrow();
        LinkedHashMap<PaimonColumnHandle, Domain> filtered = new LinkedHashMap<>();
        domains.forEach((column, domain) -> {
            if (!isRowIdColumn(column)) {
                filtered.put(column, domain);
            }
        });
        if (filtered.size() == domains.size()) {
            return predicate;
        }
        return TupleDomain.withColumnDomains(filtered);
    }

    private static boolean isRowIdColumn(PaimonColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        return PaimonColumnHandle.PAIMON_ROW_ID_NAME.equalsIgnoreCase(columnHandle.getColumnName());
    }

    private static List<org.apache.paimon.utils.Range> toPaimonRanges(Type type, Ranges ranges)
    {
        requireNonNull(type, "type is null");
        requireNonNull(ranges, "ranges is null");
        if (!BIGINT.equals(type)) {
            throw new IllegalArgumentException("Paimon row id domains must use BIGINT, got: " + type);
        }

        List<org.apache.paimon.utils.Range> result = new ArrayList<>();
        for (Range range : ranges.getOrderedRanges()) {
            long lower;
            if (range.isLowUnbounded()) {
                lower = Long.MIN_VALUE;
            }
            else {
                lower = (long) range.getLowBoundedValue();
                if (!range.isLowInclusive()) {
                    if (lower == Long.MAX_VALUE) {
                        continue;
                    }
                    lower++;
                }
            }

            long upper;
            if (range.isHighUnbounded()) {
                upper = Long.MAX_VALUE;
            }
            else {
                upper = (long) range.getHighBoundedValue();
                if (!range.isHighInclusive()) {
                    if (upper == Long.MIN_VALUE) {
                        continue;
                    }
                    upper--;
                }
            }

            if (lower > upper) {
                continue;
            }
            result.add(new org.apache.paimon.utils.Range(lower, upper));
        }
        return List.copyOf(result);
    }
}
