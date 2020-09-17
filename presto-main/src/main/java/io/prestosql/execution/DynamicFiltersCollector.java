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
package io.prestosql.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.sql.planner.plan.DynamicFilterId;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DynamicFiltersCollector
{
    public static final long INITIAL_DYNAMIC_FILTER_VERSION = 0L;
    /**
     * Dynamic filters having this version are no longer collected.
     */
    public static final long IGNORED_DYNAMIC_FILTER_VERSION = Long.MAX_VALUE;
    /**
     * Dummy domain used for communicating that dynamic filter is being ignored.
     */
    @VisibleForTesting
    static final VersionedDomain IGNORED_DOMAIN = new VersionedDomain(IGNORED_DYNAMIC_FILTER_VERSION, Optional.empty());

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
            newDynamicFilterDomains.entrySet().stream()
                    // skip ignored dynamic filters
                    .filter(entry -> !dynamicFilterDomains.containsKey(entry.getKey()) || dynamicFilterDomains.get(entry.getKey()).getVersion() != IGNORED_DYNAMIC_FILTER_VERSION)
                    .forEach(entry -> dynamicFilterDomains.merge(
                            entry.getKey(),
                            new VersionedDomain(currentVersion, Optional.of(entry.getValue().simplify())),
                            (oldDomain, newDomain) -> new VersionedDomain(
                                    max(oldDomain.getVersion(), newDomain.getVersion()),
                                    Optional.of(oldDomain.getDomain().get().intersect(newDomain.getDomain().get())))));
        }

        notifyTaskStatusChanged.run();
    }

    public synchronized Map<DynamicFilterId, Long> getDynamicFilterVersions()
    {
        return dynamicFilterDomains.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getVersion()));
    }

    public synchronized Map<DynamicFilterId, VersionedDomain> acknowledgeAndGetNewDomains(Map<DynamicFilterId, Long> callersDynamicFilterVersions)
    {
        // Remove dynamic filter domains that are already received by caller.
        // This assumes there is only one dynamic filters consumer.
        dynamicFilterDomains.entrySet().removeIf(entry ->
                entry.getValue().getVersion() <= callersDynamicFilterVersions.getOrDefault(entry.getKey(), INITIAL_DYNAMIC_FILTER_VERSION));

        // mark ignored dynamic filters so they are no longer collected
        callersDynamicFilterVersions.entrySet().stream()
                .filter(entry -> entry.getValue() == IGNORED_DYNAMIC_FILTER_VERSION)
                .forEach(entry -> dynamicFilterDomains.put(entry.getKey(), IGNORED_DOMAIN));

        // return only domains that were requested by caller (includes dummy "ignored" domains so that caller knows that dynamic filters were dropped)
        return dynamicFilterDomains.entrySet().stream()
                .filter(entry -> callersDynamicFilterVersions.containsKey(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<DynamicFilterId, VersionedDomain> acknowledgeIgnoredDomains(Map<DynamicFilterId, Long> callersDynamicFilterVersions)
    {
        return callersDynamicFilterVersions.entrySet().stream()
                .filter(entry -> entry.getValue() == IGNORED_DYNAMIC_FILTER_VERSION)
                .collect(toImmutableMap(Map.Entry::getKey, entry -> IGNORED_DOMAIN));
    }

    public static class VersionedDomain
    {
        private final long version;
        private final Optional<Domain> domain;

        @JsonCreator
        public VersionedDomain(long version, Optional<Domain> domain)
        {
            checkArgument(version == IGNORED_DYNAMIC_FILTER_VERSION ^ domain.isPresent(),
                    "Empty domain is only allowed when ignored dynamic filter version is used");
            this.version = version;
            this.domain = requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public long getVersion()
        {
            return version;
        }

        @JsonProperty
        public Optional<Domain> getDomain()
        {
            return domain;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            VersionedDomain that = (VersionedDomain) o;
            return version == that.version &&
                    domain.equals(that.domain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(version, domain);
        }
    }
}
