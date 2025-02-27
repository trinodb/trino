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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.sql.planner.BloomFilterWithRange.fromDomain;
import static java.util.Objects.requireNonNull;

public class DynamicFilterDomain
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
    public static DynamicFilterDomain create(
            @JsonProperty("domain") Optional<Domain> domain,
            @JsonProperty("bloomfilterWithRange") Optional<BloomFilterWithRange> bloomfilterWithRange)
    {
        return new DynamicFilterDomain(domain, bloomfilterWithRange);
    }

    public DynamicFilterDomain(Domain domain)
    {
        this(Optional.of(domain), Optional.empty());
    }

    public DynamicFilterDomain(BloomFilterWithRange.LongBloomFilter bloomFilter, Range range, Type type, boolean nullAllowed)
    {
        this(Optional.empty(), Optional.of(new BloomFilterWithRange(bloomFilter, range, type, nullAllowed)));
    }

    public DynamicFilterDomain(BloomFilterWithRange bloomfilterWithRange)
    {
        this(Optional.empty(), Optional.of(bloomfilterWithRange));
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
        return domain.orElseGet(() -> fromBloomfilter(bloomfilterWithRange.orElseThrow()));
    }

    @JsonIgnore
    public Range getSpan()
    {
        return domain.map(domainValue -> domainValue.getValues().getRanges().getSpan())
                .orElseGet(() -> bloomfilterWithRange.orElseThrow().getSpan());
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
            return this;
        }
        Optional<BloomFilterWithRange> bloomFilterWithRange = fromDomain(domain.orElseThrow());
        return bloomFilterWithRange.map(DynamicFilterDomain::new)
                .orElseGet(() -> new DynamicFilterDomain(domain.orElseThrow().simplify(threshold)));
    }

    public DynamicFilterDomain union(DynamicFilterDomain other)
    {
        if (domain.isPresent() && other.domain.isPresent()) {
            return new DynamicFilterDomain(domain.get().union(other.domain.get()));
        }
        if (bloomfilterWithRange.isPresent() && other.bloomfilterWithRange.isPresent()) {
            BloomFilterWithRange.LongBloomFilter newBloomfilter = bloomfilterWithRange.get().bloomFilter();
            newBloomfilter.merge(other.bloomfilterWithRange.get().bloomFilter());
            return new DynamicFilterDomain(
                    newBloomfilter,
                    bloomfilterWithRange.get().range().union(other.bloomfilterWithRange.get().range()).getRanges().getSpan(),
                    bloomfilterWithRange.get().type(),
                    bloomfilterWithRange.get().nullAllowed() || other.bloomfilterWithRange.get().nullAllowed());
        }
        if (domain.isPresent()) {
            return union(other.bloomfilterWithRange.orElseThrow(), domain.get());
        }
        return union(bloomfilterWithRange.orElseThrow(), other.domain.orElseThrow());
    }

    public DynamicFilterDomain intersect(DynamicFilterDomain other)
    {
        if (domain.isPresent() && other.domain.isPresent()) {
            return new DynamicFilterDomain(domain.get().intersect(other.domain.get()));
        }
        if (bloomfilterWithRange.isPresent() && other.bloomfilterWithRange.isPresent()) {
            BloomFilterWithRange.LongBloomFilter newBloomfilter = bloomfilterWithRange.get().bloomFilter();
            newBloomfilter.intersect(other.bloomfilterWithRange.get().bloomFilter());
            return new DynamicFilterDomain(
                    newBloomfilter,
                    bloomfilterWithRange.get().range().intersect(other.bloomfilterWithRange.get().range()).getRanges().getSpan(),
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
            return new DynamicFilterDomain(Domain.create(domain.get().getValues(), true));
        }
        return new DynamicFilterDomain(new BloomFilterWithRange(
                bloomfilterWithRange.orElseThrow().bloomFilter(),
                bloomfilterWithRange.orElseThrow().getSpan(),
                bloomfilterWithRange.orElseThrow().type(),
                true));
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

    public static DynamicFilterDomain onlyNull(Type type)
    {
        return new DynamicFilterDomain(Domain.onlyNull(type));
    }

    public static DynamicFilterDomain singleValue(Type type, Object value)
    {
        return new DynamicFilterDomain(Domain.singleValue(type, value, false));
    }

    public static DynamicFilterDomain multipleValues(Type type, List<?> values)
    {
        return new DynamicFilterDomain(Domain.multipleValues(type, values, false));
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
            return new DynamicFilterDomain(mergedDomain);
        }
        BloomFilterWithRange mergedBloomFilter = BloomFilterWithRange.union(bloomFiltersToMerge);
        return union(mergedBloomFilter, mergedDomain);
    }

    public static DynamicFilterDomain all(Type type)
    {
        return new DynamicFilterDomain(Domain.all(type));
    }

    public static DynamicFilterDomain none(Type type)
    {
        return new DynamicFilterDomain(Domain.none(type));
    }

    private static DynamicFilterDomain union(BloomFilterWithRange filterWithRange, Domain domain)
    {
        if (domain.isNone()) {
            return new DynamicFilterDomain(filterWithRange);
        }
        if (filterWithRange.isNone() || domain.isAll()) {
            return new DynamicFilterDomain(domain);
        }
        if (domain.isNullableDiscreteSet()) {
            for (Object value : domain.getNullableDiscreteSet().getNonNullValues()) {
                filterWithRange.bloomFilter().insert((long) value);
            }
            return new DynamicFilterDomain(
                    filterWithRange.bloomFilter(),
                    domain.getValues().getRanges().getSpan(),
                    filterWithRange.type(),
                    domain.isNullAllowed() || filterWithRange.nullAllowed());
        }

        Domain bloomFilterSpan = fromBloomfilter(filterWithRange);
        return new DynamicFilterDomain(bloomFilterSpan.union(domain));
    }

    private static DynamicFilterDomain intersect(BloomFilterWithRange filterWithRange, Domain domain)
    {
        if (domain.isNone() || filterWithRange.isNone()) {
            return new DynamicFilterDomain(Domain.none(domain.getType()));
        }
        if (domain.isAll()) {
            return new DynamicFilterDomain(filterWithRange);
        }
        if (domain.isNullableDiscreteSet()) {
            BloomFilterWithRange.LongBloomFilter bloomFilter = new BloomFilterWithRange.LongBloomFilter();
            for (Object value : domain.getNullableDiscreteSet().getNonNullValues()) {
                if (filterWithRange.bloomFilter().contains((long) value)) {
                    bloomFilter.insert((long) value);
                }
            }
            return new DynamicFilterDomain(
                    bloomFilter,
                    domain.intersect(Domain.create(filterWithRange.range(), filterWithRange.nullAllowed())).getValues().getRanges().getSpan(),
                    filterWithRange.type(),
                    domain.isNullAllowed() && filterWithRange.nullAllowed());
        }
        Domain bitmapSpan = fromBloomfilter(filterWithRange);
        return new DynamicFilterDomain(bitmapSpan.intersect(domain));
    }

    private static Domain fromBloomfilter(BloomFilterWithRange filterWithRange)
    {
        return Domain.create(filterWithRange.range(), filterWithRange.nullAllowed());
    }
}
