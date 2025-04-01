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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Represents a Domain containing dynamic filters.
 * This is similar to {@link Domain} but with the ability to represent dynamic filters using Bloom filters {@link BloomFilterWithRange}.
 */
public final class DynamicFilterDomain
{
    private static final int INSTANCE_SIZE = instanceSize(DynamicFilterDomain.class);

    private final Optional<Domain> domain;
    private final Optional<BloomFilterWithRange> bloomfilterWithRange;

    private DynamicFilterDomain(Optional<Domain> domain, Optional<BloomFilterWithRange> bloomfilterWithRange)
    {
        this.domain = requireNonNull(domain, "domain is null");
        this.bloomfilterWithRange = requireNonNull(bloomfilterWithRange, "bloomfilterWithRange is null");
        checkArgument(domain.isPresent() != bloomfilterWithRange.isPresent(), "Exactly one of domain and bloomfilterWithRange must be present");
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static DynamicFilterDomain create(
            @JsonProperty("domain") Optional<Domain> domain,
            @JsonProperty("bloomfilterWithRange") Optional<BloomFilterWithRange> bloomfilterWithRange)
    {
        return new DynamicFilterDomain(domain, bloomfilterWithRange);
    }

    public static DynamicFilterDomain fromDomain(Domain domain)
    {
        return new DynamicFilterDomain(Optional.of(domain), Optional.empty());
    }

    public static DynamicFilterDomain fromBloomFilter(BloomFilterWithRange bloomfilterWithRange)
    {
        return new DynamicFilterDomain(Optional.empty(), Optional.of(bloomfilterWithRange));
    }

    public DynamicFilterDomain(LongBloomFilter bloomFilter, ValueSet ranges, Type type, boolean nullAllowed)
    {
        this(Optional.empty(), Optional.of(new BloomFilterWithRange(bloomFilter, ranges, type, nullAllowed)));
    }

    @JsonProperty
    public Optional<Domain> getDomain()
    {
        return domain;
    }

    @JsonProperty
    public Optional<BloomFilterWithRange> getBloomfilterWithRange()
    {
        return bloomfilterWithRange;
    }

    @JsonIgnore
    public Type getType()
    {
        return domain.map(Domain::getType).orElseGet(() -> bloomfilterWithRange.orElseThrow().type());
    }

    public Domain toDomain()
    {
        return domain.orElseGet(() -> toDomain(bloomfilterWithRange.orElseThrow()));
    }

    @JsonIgnore
    public Range getSpan()
    {
        return toDomain().getValues().getRanges().getSpan();
    }

    public boolean isAll()
    {
        return domain.map(Domain::isAll).orElse(false);
    }

    public boolean isNone()
    {
        return domain.map(Domain::isNone).orElseGet(() -> bloomfilterWithRange.orElseThrow().isNone());
    }

    public DynamicFilterDomain simplifyDomains(int threshold)
    {
        if (domain.isEmpty()) {
            BloomFilterWithRange filterWithRange = bloomfilterWithRange.orElseThrow();
            return fromBloomFilter(new BloomFilterWithRange(
                    filterWithRange.bloomFilter(),
                    ValueSet.ofRanges(filterWithRange.ranges().getRanges().getSpan()),
                    filterWithRange.type(),
                    filterWithRange.nullAllowed()));
        }
        Optional<BloomFilterWithRange> bloomFilterWithRange = toBloomFilter(domain.orElseThrow());
        return bloomFilterWithRange.map(DynamicFilterDomain::fromBloomFilter)
                .orElseGet(() -> fromDomain(domain.orElseThrow().simplify(threshold)));
    }

    public DynamicFilterDomain union(DynamicFilterDomain other)
    {
        if (domain.isPresent() && other.domain.isPresent()) {
            return fromDomain(domain.get().union(other.domain.get()));
        }
        if (bloomfilterWithRange.isPresent() && other.bloomfilterWithRange.isPresent()) {
            BloomFilterWithRange newBloomfilter = BloomFilterWithRange.union(ImmutableList.of(bloomfilterWithRange.orElseThrow(), other.bloomfilterWithRange.orElseThrow()));
            return DynamicFilterDomain.fromBloomFilter(newBloomfilter);
        }
        if (domain.isPresent()) {
            return union(other.bloomfilterWithRange.orElseThrow(), domain.get());
        }
        return union(bloomfilterWithRange.orElseThrow(), other.domain.orElseThrow());
    }

    public DynamicFilterDomain intersect(DynamicFilterDomain other)
    {
        if (domain.isPresent() && other.domain.isPresent()) {
            return fromDomain(domain.get().intersect(other.domain.get()));
        }
        if (bloomfilterWithRange.isPresent() && other.bloomfilterWithRange.isPresent()) {
            LongBloomFilter newBloomfilter = bloomfilterWithRange.get().bloomFilter();
            newBloomfilter.intersect(other.bloomfilterWithRange.get().bloomFilter());
            return new DynamicFilterDomain(
                    newBloomfilter,
                    bloomfilterWithRange.get().ranges().intersect(other.bloomfilterWithRange.get().ranges()),
                    bloomfilterWithRange.get().type(),
                    bloomfilterWithRange.get().nullAllowed() && other.bloomfilterWithRange.get().nullAllowed());
        }
        if (domain.isPresent()) {
            return intersect(other.bloomfilterWithRange.orElseThrow(), domain.get());
        }
        return intersect(bloomfilterWithRange.orElseThrow(), other.domain.orElseThrow());
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                domain.map(Domain::getRetainedSizeInBytes).orElse(0L) +
                bloomfilterWithRange.map(BloomFilterWithRange::getRetainedSizeInBytes).orElse(0L);
    }

    public DynamicFilterDomain withNullsAllowed()
    {
        if (domain.isPresent()) {
            return fromDomain(Domain.create(domain.get().getValues(), true));
        }
        BloomFilterWithRange filterWithRange = bloomfilterWithRange.orElseThrow();
        return fromBloomFilter(new BloomFilterWithRange(filterWithRange.bloomFilter(), filterWithRange.ranges(), filterWithRange.type(), true));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicFilterDomain that = (DynamicFilterDomain) o;
        return Objects.equals(domain, that.domain)
                && Objects.equals(bloomfilterWithRange, that.bloomfilterWithRange);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(domain, bloomfilterWithRange);
    }

    @Override
    public String toString()
    {
        if (domain.isPresent()) {
            return domain.get().toString();
        }
        return bloomfilterWithRange.get().toString();
    }

    public String toString(ConnectorSession connectorSession, int limit)
    {
        if (domain.isPresent()) {
            return domain.get().toString(connectorSession, limit);
        }
        return bloomfilterWithRange.get().toString();
    }

    public static DynamicFilterDomain all(Type type)
    {
        return fromDomain(Domain.all(type));
    }

    public static DynamicFilterDomain none(Type type)
    {
        return fromDomain(Domain.none(type));
    }

    public static DynamicFilterDomain onlyNull(Type type)
    {
        return fromDomain(Domain.onlyNull(type));
    }

    public static DynamicFilterDomain singleValue(Type type, Object value)
    {
        return fromDomain(Domain.singleValue(type, value, false));
    }

    public static DynamicFilterDomain multipleValues(Type type, List<?> values)
    {
        return fromDomain(Domain.multipleValues(type, values, false));
    }

    public static DynamicFilterDomain union(List<DynamicFilterDomain> domains)
    {
        if (domains.isEmpty()) {
            throw new IllegalArgumentException("domains cannot be empty for union");
        }
        if (domains.size() == 1) {
            return domains.get(0);
        }
        List<Domain> domainsToMerge = domains.stream()
                .map(DynamicFilterDomain::getDomain)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        Domain mergedDomain = domainsToMerge.isEmpty() ? Domain.none(domains.get(0).getType()) : Domain.union(domainsToMerge);

        List<BloomFilterWithRange> bloomFiltersToMerge = domains.stream()
                .map(DynamicFilterDomain::getBloomfilterWithRange)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        if (bloomFiltersToMerge.isEmpty()) {
            return fromDomain(mergedDomain);
        }
        BloomFilterWithRange mergedBloomFilter = BloomFilterWithRange.union(bloomFiltersToMerge);
        return union(mergedBloomFilter, mergedDomain);
    }

    private static DynamicFilterDomain union(BloomFilterWithRange filterWithRange, Domain domain)
    {
        if (domain.isNone()) {
            return fromBloomFilter(filterWithRange);
        }
        if (filterWithRange.isNone() || domain.isAll()) {
            return fromDomain(domain);
        }
        if (domain.isNullableDiscreteSet()) {
            for (Object value : domain.getNullableDiscreteSet().getNonNullValues()) {
                filterWithRange.bloomFilter().insert((long) value);
            }
            return DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                    filterWithRange.bloomFilter(),
                    filterWithRange.ranges().union(ValueSet.ofRanges(domain.getValues().getRanges().getSpan())),
                    filterWithRange.type(),
                    domain.isNullAllowed() || filterWithRange.nullAllowed()));
        }

        Domain bloomFilterSpan = toDomain(filterWithRange);
        return fromDomain(bloomFilterSpan.union(domain));
    }

    private static DynamicFilterDomain intersect(BloomFilterWithRange filterWithRange, Domain domain)
    {
        if (domain.isNone() || filterWithRange.isNone()) {
            return fromDomain(Domain.none(domain.getType()));
        }
        if (domain.isAll()) {
            return fromBloomFilter(filterWithRange);
        }
        if (domain.isNullableDiscreteSet()) {
            LongBloomFilter bloomFilter = new LongBloomFilter();
            for (Object value : domain.getNullableDiscreteSet().getNonNullValues()) {
                if (filterWithRange.bloomFilter().contains((long) value)) {
                    bloomFilter.insert((long) value);
                }
            }
            if (bloomFilter.isEmpty()) {
                return DynamicFilterDomain.none(domain.getType());
            }
            return new DynamicFilterDomain(
                    bloomFilter,
                    filterWithRange.ranges().intersect(ValueSet.ofRanges(domain.getValues().getRanges().getSpan())),
                    filterWithRange.type(),
                    domain.isNullAllowed() && filterWithRange.nullAllowed());
        }
        Domain bloomFilterRange = toDomain(filterWithRange);
        return fromDomain(bloomFilterRange.intersect(domain));
    }

    private static Domain toDomain(BloomFilterWithRange filterWithRange)
    {
        return Domain.create(filterWithRange.ranges(), filterWithRange.nullAllowed());
    }

    private static Optional<BloomFilterWithRange> toBloomFilter(Domain domain)
    {
        if (domain.getType().getJavaType() != long.class || !domain.isNullableDiscreteSet()) {
            return Optional.empty();
        }
        List<Object> values = domain.getNullableDiscreteSet().getNonNullValues();
        LongBloomFilter bloomFilter = new LongBloomFilter();
        for (Object value : values) {
            bloomFilter.insert((long) value);
        }
        return Optional.of(new BloomFilterWithRange(
                bloomFilter,
                ValueSet.ofRanges(domain.getValues().getRanges().getSpan()),
                domain.getType(),
                domain.isNullAllowed()));
    }
}
