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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.DynamicFilterId;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DynamicFiltersCollector
{
    public static final long INITIAL_DYNAMIC_FILTERS_VERSION = 0L;
    public static final VersionedDynamicFilterDomains INITIAL_DYNAMIC_FILTER_DOMAINS =
            new VersionedDynamicFilterDomains(INITIAL_DYNAMIC_FILTERS_VERSION, ImmutableMap.of());

    private final Runnable notifyTaskStatusChanged;
    @GuardedBy("this")
    private final Map<DynamicFilterId, VersionedDomain> dynamicFilterDomains = new HashMap<>();
    @GuardedBy("this")
    private long currentVersion;

    public DynamicFiltersCollector(Runnable notifyTaskStatusChanged)
    {
        this.notifyTaskStatusChanged = requireNonNull(notifyTaskStatusChanged, "notifyTaskStatusChanged is null");
    }

    public void updateDomains(Map<DynamicFilterId, Domain> newDynamicFilterDomains)
    {
        if (newDynamicFilterDomains.isEmpty()) {
            return;
        }

        synchronized (this) {
            long currentVersion = ++this.currentVersion;
            for (Map.Entry<DynamicFilterId, Domain> entry : newDynamicFilterDomains.entrySet()) {
                dynamicFilterDomains.merge(
                        entry.getKey(),
                        new VersionedDomain(currentVersion, entry.getValue()),
                        (oldDomain, newDomain) -> new VersionedDomain(
                                max(oldDomain.getVersion(), newDomain.getVersion()),
                                oldDomain.getDomain().intersect(newDomain.getDomain())));
            }
        }

        notifyTaskStatusChanged.run();
    }

    public synchronized long getDynamicFiltersVersion()
    {
        return currentVersion;
    }

    public synchronized VersionedDynamicFilterDomains acknowledgeAndGetNewDomains(long callersCurrentVersion)
    {
        acknowledge(callersCurrentVersion);

        return getCurrentDynamicFilterDomains();
    }

    public synchronized void acknowledge(long callersCurrentVersion)
    {
        // Remove dynamic filter domains that are already received by caller.
        // This assumes there is only one dynamic filters consumer.
        dynamicFilterDomains.values().removeIf(domain -> domain.getVersion() <= callersCurrentVersion);
    }

    public synchronized VersionedDynamicFilterDomains getCurrentDynamicFilterDomains()
    {
        return new VersionedDynamicFilterDomains(
                currentVersion,
                dynamicFilterDomains.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getDomain())));
    }

    public static class VersionedDynamicFilterDomains
    {
        private final long version;
        private final Map<DynamicFilterId, Domain> dynamicFilterDomains;

        @JsonCreator
        public VersionedDynamicFilterDomains(long version, Map<DynamicFilterId, Domain> dynamicFilterDomains)
        {
            this.version = version;
            this.dynamicFilterDomains = ImmutableMap.copyOf(requireNonNull(dynamicFilterDomains, "dynamicFilterDomains is null"));
        }

        @JsonProperty
        public long getVersion()
        {
            return version;
        }

        @JsonProperty
        public Map<DynamicFilterId, Domain> getDynamicFilterDomains()
        {
            return dynamicFilterDomains;
        }
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

        public long getVersion()
        {
            return version;
        }

        public Domain getDomain()
        {
            return domain;
        }
    }
}
