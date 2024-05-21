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
package io.trino.plugin.base.cache;

import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toMap;

public final class CacheUtils
{
    private CacheUtils() {}

    /**
     * Normalizes {@link TupleDomain} so that equal tuple domains are serialized in same way.
     * Without normalization 1) domain map entry order might differ 2) {@link SortedRangeSet}
     * {@code sortedRanges} block might differ.
     */
    public static TupleDomain<CacheColumnId> normalizeTupleDomain(TupleDomain<CacheColumnId> tupleDomain)
    {
        if (tupleDomain.getDomains().isEmpty()) {
            return tupleDomain;
        }

        Map<CacheColumnId, Domain> domains = tupleDomain.getDomains().get();
        return TupleDomain.withColumnDomains(domains.entrySet().stream()
                // sort domains by string representation of column
                .sorted(comparing(domainEntry -> domainEntry.getKey().toString()))
                .collect(toLinkedMap(
                        Map.Entry::getKey,
                        entry -> {
                            Domain domain = entry.getValue();
                            if (domain.getValues() instanceof SortedRangeSet values) {
                                // normalize sorted range set
                                domain = Domain.create(values.normalize(), domain.isNullAllowed());
                            }
                            return domain;
                        })));
    }

    private static <T, K, U> Collector<T, ?, Map<K, U>> toLinkedMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper)
    {
        return toMap(
                keyMapper,
                valueMapper,
                (u, v) -> {
                    throw new IllegalStateException(format("Duplicate values for a key: %s and %s", u, v));
                },
                LinkedHashMap::new);
    }
}
