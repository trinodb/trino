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
package io.prestosql.sql.planner;

import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

@ThreadSafe
class LocalDynamicFiltersCollector
{
    /**
     * May contains domains for dynamic filters for different table scans
     * (e.g. in case of co-located joins).
     */
    @GuardedBy("this")
    private final Map<Symbol, Domain> dynamicFilterDomainsResult = new HashMap<>();

    public synchronized TupleDomain<Symbol> getDynamicFilter(Set<Symbol> probeSymbols)
    {
        Map<Symbol, Domain> probeSymbolDomains = dynamicFilterDomainsResult.entrySet().stream()
                .filter(entry -> probeSymbols.contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        return TupleDomain.withColumnDomains(probeSymbolDomains);
    }

    public synchronized void addDynamicFilter(Map<Symbol, Domain> dynamicFilterDomains)
    {
        for (Map.Entry<Symbol, Domain> entry : dynamicFilterDomains.entrySet()) {
            dynamicFilterDomainsResult.merge(entry.getKey(), entry.getValue(), Domain::intersect);
        }
    }
}
