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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Collections.emptyMap;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

/**
 * Represents a TupleDomain containing dynamic filters.
 * This is similar to {@link TupleDomain} but with the ability to represent dynamic filters using Bloom filters {@link BloomFilterWithRange}.
 */
public final class DynamicFilterTupleDomain<T>
{
    private static final int INSTANCE_SIZE = instanceSize(DynamicFilterTupleDomain.class);
    private static final int MAX_BLOOM_FILTER_COUNT = 5;

    private static final DynamicFilterTupleDomain<?> NONE = new DynamicFilterTupleDomain<>(Optional.empty());
    private static final DynamicFilterTupleDomain<?> ALL = new DynamicFilterTupleDomain<>(Optional.of(emptyMap()));

    private final Optional<Map<T, DynamicFilterDomain>> domains;

    private DynamicFilterTupleDomain(Optional<Map<T, DynamicFilterDomain>> domains)
    {
        requireNonNull(domains, "domains is null");

        this.domains = domains.flatMap(map -> {
            if (containsNoneDomain(map)) {
                return Optional.empty();
            }
            return Optional.of(normalizeAndCopy(map));
        });
    }

    /**
     * Returns true if any tuples would satisfy this TupleDomain
     */
    public boolean isAll()
    {
        return domains.isPresent() && domains.get().isEmpty();
    }

    /**
     * Returns true if no tuple could ever satisfy this TupleDomain
     */
    public boolean isNone()
    {
        return domains.isEmpty();
    }

    public Optional<Map<T, DynamicFilterDomain>> getDomains()
    {
        return domains;
    }

    public DynamicFilterTupleDomain<T> simplifyDomains(int threshold)
    {
        if (isNone() || isAll()) {
            return this;
        }
        DynamicFilterTupleDomain<T> simplifiedTupleDomain = withColumnDomains(domains.get().entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().simplifyDomains(threshold))));
        long bloomFilterCount = simplifiedTupleDomain.getDomains().get().values().stream()
                .filter(domain -> domain.getBloomfilterWithRange().isPresent())
                .count();
        if (bloomFilterCount <= MAX_BLOOM_FILTER_COUNT) {
            return simplifiedTupleDomain;
        }
        // Drop the bloom filters with the highest distinct counts as they are likely to be the least selective
        // Having too many bloom filters can increase memory usage and exceed network request size limits
        List<Map.Entry<T, DynamicFilterDomain>> sortedByDistinctCount = simplifiedTupleDomain.getDomains().get().entrySet().stream()
                .sorted(comparing(entry -> entry.getValue().getBloomfilterWithRange()
                        .map(filter -> filter.bloomFilter().getMinDistinctHashes())
                        .orElse(Integer.MAX_VALUE)))
                .collect(toImmutableList());
        List<Map.Entry<T, DynamicFilterDomain>> bloomFiltersRetained = sortedByDistinctCount.subList(0, MAX_BLOOM_FILTER_COUNT);
        List<Map.Entry<T, DynamicFilterDomain>> bloomFiltersDropped = sortedByDistinctCount.subList(MAX_BLOOM_FILTER_COUNT, sortedByDistinctCount.size())
                .stream()
                .map(entry -> Map.entry(entry.getKey(), DynamicFilterDomain.fromDomain(entry.getValue().toDomain().simplify(threshold))))
                .collect(toImmutableList());
        return withColumnDomains(Stream.of(bloomFiltersRetained, bloomFiltersDropped)
                .flatMap(List::stream)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public TupleDomain<T> toTupleDomain()
    {
        if (isNone()) {
            return TupleDomain.none();
        }
        if (isAll()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(domains.get().entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().toDomain())));
    }

    public DynamicFilterTupleDomain<T> intersect(DynamicFilterTupleDomain<T> other)
    {
        requireNonNull(other, "other is null");

        if (this.isNone() || other.isNone()) {
            return none();
        }
        if (this == other) {
            return this;
        }
        if (this.isAll()) {
            return other;
        }
        if (other.isAll()) {
            return this;
        }

        Map<T, DynamicFilterDomain> intersected = new LinkedHashMap<>(this.getDomains().get());
        for (Map.Entry<T, DynamicFilterDomain> entry : other.getDomains().get().entrySet()) {
            DynamicFilterDomain intersectionDomain = intersected.get(entry.getKey());
            if (intersectionDomain == null) {
                intersected.put(entry.getKey(), entry.getValue());
            }
            else {
                DynamicFilterDomain intersect = intersectionDomain.intersect(entry.getValue());
                if (intersect.isNone()) {
                    return DynamicFilterTupleDomain.none();
                }
                intersected.put(entry.getKey(), intersect);
            }
        }
        return withColumnDomains(intersected);
    }

    public long getRetainedSizeInBytes(ToLongFunction<T> keySize)
    {
        return INSTANCE_SIZE
                + sizeOf(domains, value -> estimatedSizeOf(value, keySize, DynamicFilterDomain::getRetainedSizeInBytes));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicFilterTupleDomain<?> that = (DynamicFilterTupleDomain<?>) o;
        return Objects.equals(domains, that.domains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(domains);
    }

    @Override
    public String toString()
    {
        if (isAll()) {
            return "ALL";
        }
        if (isNone()) {
            return "NONE";
        }
        return domains.orElseThrow().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toString()))
                .toString();
    }

    public static <T> DynamicFilterTupleDomain<T> intersect(List<? extends DynamicFilterTupleDomain<? extends T>> domains)
    {
        if (domains.isEmpty()) {
            return all();
        }

        if (domains.size() == 1) {
            return upcast(domains.getFirst());
        }

        if (domains.stream().anyMatch(DynamicFilterTupleDomain::isNone)) {
            return none();
        }

        if (domains.stream().allMatch(domain -> domain.equals(domains.getFirst()))) {
            return upcast(domains.getFirst());
        }

        List<DynamicFilterTupleDomain<? extends T>> candidates = domains.stream()
                .filter(domain -> !domain.isAll())
                .collect(toImmutableList());

        if (candidates.isEmpty()) {
            return all();
        }

        if (candidates.size() == 1) {
            return upcast(candidates.getFirst());
        }

        Map<T, DynamicFilterDomain> intersected = new LinkedHashMap<>(candidates.get(0).getDomains().get());
        for (int i = 1; i < candidates.size(); i++) {
            for (Map.Entry<? extends T, DynamicFilterDomain> entry : candidates.get(i).getDomains().get().entrySet()) {
                DynamicFilterDomain intersectionDomain = intersected.get(entry.getKey());
                if (intersectionDomain == null) {
                    intersected.put(entry.getKey(), entry.getValue());
                }
                else {
                    DynamicFilterDomain intersect = intersectionDomain.intersect(entry.getValue());
                    if (intersect.isNone()) {
                        return DynamicFilterTupleDomain.none();
                    }
                    intersected.put(entry.getKey(), intersect);
                }
            }
        }

        return withColumnDomains(intersected);
    }

    public static <T> DynamicFilterTupleDomain<T> withColumnDomains(Map<T, DynamicFilterDomain> domains)
    {
        requireNonNull(domains, "domains is null");
        if (domains.isEmpty()) {
            return all();
        }
        return new DynamicFilterTupleDomain<>(Optional.of(domains));
    }

    @SuppressWarnings("unchecked")
    public static <T> DynamicFilterTupleDomain<T> none()
    {
        return (DynamicFilterTupleDomain<T>) NONE;
    }

    @SuppressWarnings("unchecked")
    public static <T> DynamicFilterTupleDomain<T> all()
    {
        return (DynamicFilterTupleDomain<T>) ALL;
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    @Deprecated // Discourage usages
    public static <T> DynamicFilterTupleDomain<T> fromColumnDomains(@JsonProperty("columnDomains") Optional<List<ColumnDomain<T>>> columnDomains)
    {
        if (columnDomains.isEmpty()) {
            return none();
        }
        return withColumnDomains(columnDomains.get().stream()
                .collect(toImmutableMap(ColumnDomain::getColumn, ColumnDomain::getDomain)));
    }

    @JsonProperty
    @Deprecated // For JSON serialization only, discourage usages
    public Optional<List<ColumnDomain<T>>> getColumnDomains()
    {
        return domains.map(map -> map.entrySet().stream()
                .map(entry -> new ColumnDomain<>(entry.getKey(), entry.getValue()))
                .collect(toImmutableList()));
    }

    // Available for Jackson serialization only!
    public static class ColumnDomain<C>
    {
        private final C column;
        private final DynamicFilterDomain domain;

        @JsonCreator
        public ColumnDomain(
                @JsonProperty("column") C column,
                @JsonProperty("domain") DynamicFilterDomain domain)
        {
            this.column = requireNonNull(column, "column is null");
            this.domain = requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public C getColumn()
        {
            return column;
        }

        @JsonProperty
        public DynamicFilterDomain getDomain()
        {
            return domain;
        }
    }

    public static <T> DynamicFilterTupleDomain<T> columnWiseUnion(List<DynamicFilterTupleDomain<T>> tupleDomains)
    {
        if (tupleDomains.isEmpty()) {
            throw new IllegalArgumentException("tupleDomains must have at least one element");
        }

        if (tupleDomains.size() == 1) {
            return tupleDomains.getFirst();
        }

        // gather all common columns
        Set<T> commonColumns = new HashSet<>();

        // first, find a non-none domain
        boolean found = false;
        Iterator<DynamicFilterTupleDomain<T>> domains = tupleDomains.iterator();
        while (domains.hasNext()) {
            DynamicFilterTupleDomain<T> domain = domains.next();
            if (domain.isAll()) {
                return DynamicFilterTupleDomain.all();
            }
            if (!domain.isNone()) {
                found = true;
                commonColumns.addAll(domain.getDomains().get().keySet());
                break;
            }
        }

        if (!found) {
            return DynamicFilterTupleDomain.none();
        }

        // then, get the common columns
        while (domains.hasNext()) {
            DynamicFilterTupleDomain<T> domain = domains.next();
            if (!domain.isNone()) {
                commonColumns.retainAll(domain.getDomains().get().keySet());
            }
        }

        // group domains by column (only for common columns)
        Map<T, List<DynamicFilterDomain>> domainsByColumn = new LinkedHashMap<>(tupleDomains.size());

        for (DynamicFilterTupleDomain<T> domain : tupleDomains) {
            if (!domain.isNone()) {
                for (Map.Entry<T, DynamicFilterDomain> entry : domain.getDomains().get().entrySet()) {
                    if (commonColumns.contains(entry.getKey())) {
                        List<DynamicFilterDomain> domainForColumn = domainsByColumn.computeIfAbsent(entry.getKey(), _ -> new ArrayList<>());
                        domainForColumn.add(entry.getValue());
                    }
                }
            }
        }

        // finally, do the column-wise union
        Map<T, DynamicFilterDomain> result = new LinkedHashMap<>(domainsByColumn.size());
        for (Map.Entry<T, List<DynamicFilterDomain>> entry : domainsByColumn.entrySet()) {
            result.put(entry.getKey(), DynamicFilterDomain.union(entry.getValue()));
        }
        return withColumnDomains(result);
    }

    @SuppressWarnings("unchecked")
    private static <T> DynamicFilterTupleDomain<T> upcast(DynamicFilterTupleDomain<? extends T> domain)
    {
        // DynamicFilterTupleDomain<T> is covariant with respect to T (because it's immutable), so it's a safe operation
        return (DynamicFilterTupleDomain<T>) domain;
    }

    private static <T> boolean containsNoneDomain(Map<T, DynamicFilterDomain> domains)
    {
        return domains.values().stream().anyMatch(DynamicFilterDomain::isNone);
    }

    private static <T> Map<T, DynamicFilterDomain> normalizeAndCopy(Map<T, DynamicFilterDomain> domains)
    {
        return domains.entrySet().stream()
                .filter(entry -> !entry.getValue().isAll())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
