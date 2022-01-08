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
package io.trino.spi.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Defines a set of valid tuples according to the constraints on each of its constituent columns
 */
public final class TupleDomain<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TupleDomain.class).instanceSize();

    private static final TupleDomain<?> NONE = new TupleDomain<>(Optional.empty());
    private static final TupleDomain<?> ALL = new TupleDomain<>(Optional.of(emptyMap()));

    /**
     * TupleDomain is internally represented as a normalized map of each column to its
     * respective allowable value Domain. Conceptually, these Domains can be thought of
     * as being AND'ed together to form the representative predicate.
     * <p>
     * This map is normalized in the following ways:
     * 1) The map will not contain Domain.none() as any of its values. If any of the Domain
     * values are Domain.none(), then the whole map will instead be null. This enforces the fact that
     * any single Domain.none() value effectively turns this TupleDomain into "none" as well.
     * 2) The map will not contain Domain.all() as any of its values. Our convention here is that
     * any unmentioned column is equivalent to having Domain.all(). To normalize this structure,
     * we remove any Domain.all() values from the map.
     */
    private final Optional<Map<T, Domain>> domains;

    private TupleDomain(Optional<Map<T, Domain>> domains)
    {
        requireNonNull(domains, "domains is null");

        this.domains = domains.flatMap(map -> {
            if (containsNoneDomain(map)) {
                return Optional.empty();
            }
            return Optional.of(Collections.unmodifiableMap(normalizeAndCopy(map)));
        });
    }

    public static <T> TupleDomain<T> withColumnDomains(Map<T, Domain> domains)
    {
        requireNonNull(domains, "domains is null");
        if (domains.isEmpty()) {
            return all();
        }
        return new TupleDomain<>(Optional.of(domains));
    }

    @SuppressWarnings("unchecked")
    public static <T> TupleDomain<T> none()
    {
        return (TupleDomain<T>) NONE;
    }

    @SuppressWarnings("unchecked")
    public static <T> TupleDomain<T> all()
    {
        return (TupleDomain<T>) ALL;
    }

    /**
     * Extract all column constraints that require exactly one value or only null in their respective Domains.
     * Returns an empty Optional if the Domain is none.
     */
    public static <T> Optional<Map<T, NullableValue>> extractFixedValues(TupleDomain<T> tupleDomain)
    {
        if (tupleDomain.getDomains().isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(tupleDomain.getDomains().get()
                .entrySet().stream()
                .filter(entry -> entry.getValue().isNullableSingleValue())
                .collect(toLinkedMap(Map.Entry::getKey, entry -> new NullableValue(entry.getValue().getType(), entry.getValue().getNullableSingleValue()))));
    }

    /**
     * Extract all column constraints that define a non-empty set of discrete values allowed for the columns in their respective Domains.
     * Returns an empty Optional if the Domain is none.
     */
    public static <T> Optional<Map<T, List<NullableValue>>> extractDiscreteValues(TupleDomain<T> tupleDomain)
    {
        if (tupleDomain.getDomains().isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(tupleDomain.getDomains().get()
                .entrySet().stream()
                .filter(entry -> entry.getValue().isNullableDiscreteSet())
                .collect(toLinkedMap(
                        Map.Entry::getKey,
                        entry -> {
                            Domain.DiscreteSet discreteValues = entry.getValue().getNullableDiscreteSet();
                            List<NullableValue> nullableValues = new ArrayList<>();
                            for (Object value : discreteValues.getNonNullValues()) {
                                nullableValues.add(new NullableValue(entry.getValue().getType(), value));
                            }
                            if (discreteValues.containsNull()) {
                                nullableValues.add(new NullableValue(entry.getValue().getType(), null));
                            }
                            return unmodifiableList(nullableValues);
                        })));
    }

    /**
     * Convert a map of columns to values into the TupleDomain which requires
     * those columns to be fixed to those values. Null is allowed as a fixed value.
     */
    public static <T> TupleDomain<T> fromFixedValues(Map<T, NullableValue> fixedValues)
    {
        return TupleDomain.withColumnDomains(fixedValues.entrySet().stream()
                .collect(toLinkedMap(
                        Map.Entry::getKey,
                        entry -> {
                            Type type = entry.getValue().getType();
                            Object value = entry.getValue().getValue();
                            return value == null ? Domain.onlyNull(type) : Domain.singleValue(type, value);
                        })));
    }

    /*
     * This method is for JSON serialization only. Do not use.
     * It's marked as @Deprecated to help avoid usage, and not because we plan to remove it.
     */
    @Deprecated
    @JsonCreator
    public static <T> TupleDomain<T> fromColumnDomains(@JsonProperty("columnDomains") Optional<List<ColumnDomain<T>>> columnDomains)
    {
        if (columnDomains.isEmpty()) {
            return none();
        }
        return withColumnDomains(columnDomains.get().stream()
                .collect(toLinkedMap(ColumnDomain::getColumn, ColumnDomain::getDomain)));
    }

    /*
     * This method is for JSON serialization only. Do not use.
     * It's marked as @Deprecated to help avoid usage, and not because we plan to remove it.
     */
    @Deprecated
    @JsonProperty
    public Optional<List<ColumnDomain<T>>> getColumnDomains()
    {
        return domains.map(map -> map.entrySet().stream()
                .map(entry -> new ColumnDomain<>(entry.getKey(), entry.getValue()))
                .collect(toUnmodifiableList()));
    }

    private static <T> boolean containsNoneDomain(Map<T, Domain> domains)
    {
        return domains.values().stream().anyMatch(Domain::isNone);
    }

    private static <T> Map<T, Domain> normalizeAndCopy(Map<T, Domain> domains)
    {
        return domains.entrySet().stream()
                .filter(entry -> !entry.getValue().isAll())
                .collect(toLinkedMap(Map.Entry::getKey, Map.Entry::getValue));
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

    /**
     * Gets the TupleDomain as a map of each column to its respective Domain.
     * - Will return an Optional.empty() if this is a 'none' TupleDomain.
     * - Unmentioned columns have an implicit value of Domain.all()
     * - The column Domains can be thought of as AND'ed to together to form the whole predicate
     */
    @JsonIgnore
    public Optional<Map<T, Domain>> getDomains()
    {
        return domains;
    }

    /**
     * Returns the strict intersection of the TupleDomains.
     * The resulting TupleDomain represents the set of tuples that would be valid
     * in both TupleDomains.
     */
    public TupleDomain<T> intersect(TupleDomain<T> other)
    {
        return intersect(List.of(this, other));
    }

    public static <T> TupleDomain<T> intersect(List<TupleDomain<T>> domains)
    {
        if (domains.size() < 2) {
            throw new IllegalArgumentException("Expected at least 2 elements");
        }

        if (domains.stream().anyMatch(TupleDomain::isNone)) {
            return none();
        }

        if (domains.stream().allMatch(domain -> domain.equals(domains.get(0)))) {
            return domains.get(0);
        }

        List<TupleDomain<T>> candidates = domains.stream()
                .filter(domain -> !domain.isAll())
                .collect(toList());

        if (candidates.isEmpty()) {
            return all();
        }

        if (candidates.size() == 1) {
            return candidates.get(0);
        }

        Map<T, Domain> intersected = new LinkedHashMap<>(candidates.get(0).getDomains().get());
        for (int i = 1; i < candidates.size(); i++) {
            for (Map.Entry<T, Domain> entry : candidates.get(i).getDomains().get().entrySet()) {
                Domain intersectionDomain = intersected.get(entry.getKey());
                if (intersectionDomain == null) {
                    intersected.put(entry.getKey(), entry.getValue());
                }
                else {
                    Domain intersect = intersectionDomain.intersect(entry.getValue());
                    if (intersect.isNone()) {
                        return TupleDomain.none();
                    }
                    intersected.put(entry.getKey(), intersect);
                }
            }
        }

        return withColumnDomains(intersected);
    }

    @SafeVarargs
    public static <T> TupleDomain<T> columnWiseUnion(TupleDomain<T> first, TupleDomain<T> second, TupleDomain<T>... rest)
    {
        List<TupleDomain<T>> domains = new ArrayList<>(rest.length + 2);
        domains.add(first);
        domains.add(second);
        domains.addAll(Arrays.asList(rest));

        return columnWiseUnion(domains);
    }

    /**
     * Returns the tuple domain that contains all other tuple domains, or {@code Optional.empty()} if they
     * are not supersets of each other.
     */
    public static <T> Optional<TupleDomain<T>> maximal(List<TupleDomain<T>> domains)
    {
        if (domains.isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<T> largest = domains.get(0);
        for (int i = 1; i < domains.size(); i++) {
            TupleDomain<T> current = domains.get(i);

            if (current.contains(largest)) {
                largest = current;
            }
            else if (!largest.contains(current)) {
                return Optional.empty();
            }
        }

        return Optional.of(largest);
    }

    /**
     * Returns a TupleDomain in which corresponding column Domains are unioned together.
     * <p>
     * Note that this is NOT equivalent to a strict union as the final result may allow tuples
     * that do not exist in either TupleDomain.
     * Example 1:
     * <p>
     * <ul>
     * <li>TupleDomain X: a => 1, b => 2
     * <li>TupleDomain Y: a => 2, b => 3
     * <li>Column-wise unioned TupleDomain: a => 1 OR 2, b => 2 OR 3
     * </ul>
     * <p>
     * In the above resulting TupleDomain, tuple (a => 1, b => 3) would be considered valid but would
     * not be valid for either TupleDomain X or TupleDomain Y.
     * Example 2:
     * <p>
     * Let a be of type DOUBLE
     * <ul>
     * <li>TupleDomain X: (a < 5)
     * <li>TupleDomain Y: (a > 0)
     * <li>Column-wise unioned TupleDomain: (a IS NOT NULL)
     * </ul>
     * </p>
     * In the above resulting TupleDomain, tuple (a => NaN) would be considered valid but would
     * not be valid for either TupleDomain X or TupleDomain Y.
     * However, this result is guaranteed to be a superset of the strict union.
     */
    public static <T> TupleDomain<T> columnWiseUnion(List<TupleDomain<T>> tupleDomains)
    {
        if (tupleDomains.isEmpty()) {
            throw new IllegalArgumentException("tupleDomains must have at least one element");
        }

        if (tupleDomains.size() == 1) {
            return tupleDomains.get(0);
        }

        // gather all common columns
        Set<T> commonColumns = new HashSet<>();

        // first, find a non-none domain
        boolean found = false;
        Iterator<TupleDomain<T>> domains = tupleDomains.iterator();
        while (domains.hasNext()) {
            TupleDomain<T> domain = domains.next();
            if (domain.isAll()) {
                return TupleDomain.all();
            }
            if (!domain.isNone()) {
                found = true;
                commonColumns.addAll(domain.getDomains().get().keySet());
                break;
            }
        }

        if (!found) {
            return TupleDomain.none();
        }

        // then, get the common columns
        while (domains.hasNext()) {
            TupleDomain<T> domain = domains.next();
            if (!domain.isNone()) {
                commonColumns.retainAll(domain.getDomains().get().keySet());
            }
        }

        // group domains by column (only for common columns)
        Map<T, List<Domain>> domainsByColumn = new LinkedHashMap<>(tupleDomains.size());

        for (TupleDomain<T> domain : tupleDomains) {
            if (!domain.isNone()) {
                for (Map.Entry<T, Domain> entry : domain.getDomains().get().entrySet()) {
                    if (commonColumns.contains(entry.getKey())) {
                        List<Domain> domainForColumn = domainsByColumn.get(entry.getKey());
                        if (domainForColumn == null) {
                            domainForColumn = new ArrayList<>();
                            domainsByColumn.put(entry.getKey(), domainForColumn);
                        }
                        domainForColumn.add(entry.getValue());
                    }
                }
            }
        }

        // finally, do the column-wise union
        Map<T, Domain> result = new LinkedHashMap<>(domainsByColumn.size());
        for (Map.Entry<T, List<Domain>> entry : domainsByColumn.entrySet()) {
            result.put(entry.getKey(), Domain.union(entry.getValue()));
        }
        return withColumnDomains(result);
    }

    /**
     * Returns true only if there exists a strict intersection between the TupleDomains.
     * i.e. there exists some potential tuple that would be allowable in both TupleDomains.
     */
    public boolean overlaps(TupleDomain<T> other)
    {
        requireNonNull(other, "other is null");

        if (this.isNone() || other.isNone()) {
            return false;
        }
        if (this == other || this.isAll() || other.isAll()) {
            return true;
        }

        Map<T, Domain> thisDomains = this.domains.orElseThrow();
        Map<T, Domain> otherDomains = other.getDomains().orElseThrow();

        for (Map.Entry<T, Domain> entry : otherDomains.entrySet()) {
            Domain commonColumnDomain = thisDomains.get(entry.getKey());
            if (commonColumnDomain != null) {
                if (!commonColumnDomain.overlaps(entry.getValue())) {
                    return false;
                }
            }
        }
        // All the common columns have overlapping domains
        return true;
    }

    /**
     * Returns true only if the this TupleDomain contains all possible tuples that would be allowable by
     * the other TupleDomain.
     */
    public boolean contains(TupleDomain<T> other)
    {
        if (other.isNone() || this == other) {
            return true;
        }
        if (isNone()) {
            return false;
        }
        Map<T, Domain> thisDomains = domains.orElseThrow();
        Map<T, Domain> otherDomains = other.getDomains().orElseThrow();
        for (Map.Entry<T, Domain> entry : thisDomains.entrySet()) {
            Domain otherDomain = otherDomains.get(entry.getKey());
            if (otherDomain == null || !entry.getValue().contains(otherDomain)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TupleDomain<?> other = (TupleDomain<?>) obj;
        return Objects.equals(this.domains, other.domains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(domains);
    }

    @Override
    public String toString()
    {
        return toString(ToStringSession.INSTANCE);
    }

    public String toString(ConnectorSession session)
    {
        if (isAll()) {
            return "ALL";
        }
        if (isNone()) {
            return "NONE";
        }
        return domains.orElseThrow().entrySet().stream()
                .collect(toLinkedMap(Map.Entry::getKey, entry -> entry.getValue().toString(session)))
                .toString();
    }

    public TupleDomain<T> filter(BiPredicate<T, Domain> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        return transformDomains((key, domain) -> {
            if (!predicate.test(key, domain)) {
                return Domain.all(domain.getType());
            }
            return domain;
        });
    }

    public <U> TupleDomain<U> transformKeys(Function<T, U> function)
    {
        if (isNone()) {
            return none();
        }
        if (isAll()) {
            return all();
        }

        Map<T, Domain> domains = this.domains.orElseThrow();
        HashMap<U, Domain> result = new LinkedHashMap<>(domains.size());
        for (Map.Entry<T, Domain> entry : domains.entrySet()) {
            U key = function.apply(entry.getKey());
            requireNonNull(key, () -> format("mapping function %s returned null for %s", function, entry.getKey()));

            Domain previous = result.put(key, entry.getValue());
            if (previous != null) {
                throw new IllegalArgumentException(format("Every argument must have a unique mapping. %s maps to %s and %s", entry.getKey(), entry.getValue(), previous));
            }
        }

        return TupleDomain.withColumnDomains(result);
    }

    public TupleDomain<T> simplify()
    {
        return transformDomains((key, domain) -> domain.simplify());
    }

    public TupleDomain<T> simplify(int threshold)
    {
        return transformDomains((key, domain) -> domain.simplify(threshold));
    }

    public TupleDomain<T> transformDomains(BiFunction<T, Domain, Domain> transformation)
    {
        requireNonNull(transformation, "transformation is null");
        if (isNone() || isAll()) {
            return this;
        }

        return withColumnDomains(domains.get().entrySet().stream()
                .collect(toLinkedMap(
                        Map.Entry::getKey,
                        entry -> {
                            Domain newDomain = transformation.apply(entry.getKey(), entry.getValue());
                            return requireNonNull(newDomain, "newDomain is null");
                        })));
    }

    public Predicate<Map<T, NullableValue>> asPredicate()
    {
        if (isNone()) {
            return bindings -> false;
        }
        Map<T, Domain> domains = this.domains.orElseThrow();
        return bindings -> {
            for (Map.Entry<T, NullableValue> entry : bindings.entrySet()) {
                Domain domain = domains.get(entry.getKey());
                if (domain != null && !domain.includesNullableValue(entry.getValue().getValue())) {
                    return false;
                }
            }
            return true;
        };
    }

    // Available for Jackson serialization only!
    public static class ColumnDomain<C>
    {
        private final C column;
        private final Domain domain;

        @JsonCreator
        public ColumnDomain(
                @JsonProperty("column") C column,
                @JsonProperty("domain") Domain domain)
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
        public Domain getDomain()
        {
            return domain;
        }
    }

    private static <T, K, U> Collector<T, ?, Map<K, U>> toLinkedMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper)
    {
        return toMap(
                keyMapper,
                valueMapper,
                (u, v) -> { throw new IllegalStateException(format("Duplicate values for a key: %s and %s", u, v)); },
                LinkedHashMap::new);
    }

    public long getRetainedSizeInBytes(ToLongFunction<T> keySize)
    {
        return INSTANCE_SIZE
                + sizeOf(domains, value -> estimatedSizeOf(value, keySize, Domain::getRetainedSizeInBytes));
    }
}
