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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEnumMap;
import static io.trino.execution.SummaryInfo.Type.DYNAMIC_FILTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VersionedSummaryInfoCollector
{
    public static final long INITIAL_SUMMARY_VERSION = 0L;
    public static final List<SummaryInfo> INITIAL_VERSIONED_SUMMARY = ImmutableList.of();

    private final Runnable notifyTaskStatusChanged;
    private final Map<SummaryInfo.Type, VersionedSummary> versionedSummaries;
    @GuardedBy("this")
    private long currentVersion;

    public VersionedSummaryInfoCollector(Runnable notifyTaskStatusChanged)
    {
        this.notifyTaskStatusChanged = requireNonNull(notifyTaskStatusChanged, "notifyTaskStatusChanged is null");
        this.versionedSummaries = immutableEnumMap(ImmutableMap.<SummaryInfo.Type, VersionedSummary>builder()
                .put(DYNAMIC_FILTER, new VersionedDynamicFilterSummary())
                .build());
    }

    public void updateSummary(SummaryInfo newSummaryInfo)
    {
        if (newSummaryInfo.isEmpty()) {
            return;
        }

        VersionedSummary versionedSummary = versionedSummaries.get(newSummaryInfo.getType());
        requireNonNull(versionedSummary, format("versionedSummary not found for type %s", newSummaryInfo.getType()));
        synchronized (this) {
            long currentVersion = ++this.currentVersion;
            versionedSummary.merge(currentVersion, newSummaryInfo);
        }

        notifyTaskStatusChanged.run();
    }

    public synchronized long getCurrentSummaryVersion()
    {
        return currentVersion;
    }

    public synchronized VersionedSummaryInfo acknowledgeAndGetNewSummaryInfo(long callersCurrentVersion)
    {
        return new VersionedSummaryInfo(
                currentVersion,
                versionedSummaries.values().stream()
                        .map(versionedSummary -> versionedSummary.acknowledgeAndGetNewStats(callersCurrentVersion))
                        .collect(toImmutableList()));
    }

    public void acknowledge(long callersCurrentVersion)
    {
        versionedSummaries.values().forEach(versionedSummary -> versionedSummary.acknowledge(callersCurrentVersion));
    }

    public synchronized VersionedSummaryInfo getCurrentSummaryInfo()
    {
        return new VersionedSummaryInfo(
                currentVersion,
                versionedSummaries.values().stream()
                        .map(VersionedSummary::getCurrentInfo)
                        .collect(toImmutableList()));
    }

    public static class VersionedSummaryInfo
    {
        private final long version;
        private final List<SummaryInfo> summaryInfo;

        @JsonCreator
        public VersionedSummaryInfo(long version, List<SummaryInfo> summaryInfo)
        {
            this.version = version;
            this.summaryInfo = ImmutableList.copyOf(requireNonNull(summaryInfo, "summaryStats is null"));
        }

        @JsonProperty
        public long getVersion()
        {
            return version;
        }

        @JsonProperty
        public List<SummaryInfo> getSummaryInfo()
        {
            return summaryInfo;
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
            VersionedSummaryInfo that = (VersionedSummaryInfo) o;
            return version == that.version && Objects.equals(summaryInfo, that.summaryInfo);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(version, summaryInfo);
        }
    }
}
