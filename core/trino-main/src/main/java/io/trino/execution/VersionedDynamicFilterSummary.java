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
package io.trino.execution;

import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.DynamicFilterId;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public final class VersionedDynamicFilterSummary
        implements VersionedSummary
{
    @GuardedBy("this")
    private final Map<DynamicFilterId, VersionedDomain> dynamicFilterDomains = new HashMap<>();

    @Override
    public void merge(long newVersion, SummaryInfo newStats)
    {
        DynamicFilterSummary newSummaryStats = (DynamicFilterSummary) newStats;
        synchronized (this) {
            for (Map.Entry<DynamicFilterId, Domain> entry : newSummaryStats.getDynamicFilterDomains().entrySet()) {
                dynamicFilterDomains.merge(
                        entry.getKey(),
                        new VersionedDomain(newVersion, entry.getValue()),
                        (oldDomain, newDomain) -> new VersionedDomain(
                                max(oldDomain.getVersion(), newDomain.getVersion()),
                                oldDomain.getDomain().intersect(newDomain.getDomain())));
            }
        }
    }

    @Override
    public synchronized void acknowledge(long callersCurrentVersion)
    {
        // Remove dynamic filter domains that are already received by caller.
        // This assumes there is only one dynamic filters consumer.
        dynamicFilterDomains.values().removeIf(domain -> domain.getVersion() <= callersCurrentVersion);
    }

    @Override
    public synchronized SummaryInfo getCurrentInfo()
    {
        return new DynamicFilterSummary(
                dynamicFilterDomains.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getDomain())));
    }

    private static class VersionedDomain
    {
        private final long version;
        private final Domain domain;

        private VersionedDomain(long version, Domain domain)
        {
            this.version = version;
            this.domain = requireNonNull(domain, "domain is null");
        }

        private long getVersion()
        {
            return version;
        }

        private Domain getDomain()
        {
            return domain;
        }
    }
}
