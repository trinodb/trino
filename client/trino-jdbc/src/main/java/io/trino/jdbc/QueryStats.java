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
package io.trino.jdbc;

import io.trino.client.StatementStats;

import java.util.Optional;
import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;

public final class QueryStats
{
    private final String queryId;
    private final String state;
    private final boolean queued;
    private final boolean scheduled;
    private final OptionalDouble progressPercentage;
    private final int nodes;
    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;
    private final long planningTimeMillis;
    private final long analysisTimeMillis;
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long queuedTimeMillis;
    private final long elapsedTimeMillis;
    private final long finishingTimeMillis;
    private final long physicalInputTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final long peakMemoryBytes;
    private final long physicalInputBytes;
    private final long physicalWrittenBytes;
    private final long internalNetworkInputBytes;
    private final Optional<StageStats> rootStage;

    public QueryStats(
            String queryId,
            String state,
            boolean queued,
            boolean scheduled,
            OptionalDouble progressPercentage,
            int nodes,
            int totalSplits,
            int queuedSplits,
            int runningSplits,
            int completedSplits,
            long planningTimeMillis,
            long analysisTimeMillis,
            long cpuTimeMillis,
            long wallTimeMillis,
            long queuedTimeMillis,
            long elapsedTimeMillis,
            long finishingTimeMillis,
            long physicalInputTimeMillis,
            long processedRows,
            long processedBytes,
            long peakMemoryBytes,
            long physicalInputBytes,
            long physicalWrittenBytes,
            long internalNetworkInputBytes,
            Optional<StageStats> rootStage)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.state = requireNonNull(state, "state is null");
        this.queued = queued;
        this.scheduled = scheduled;
        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
        this.nodes = nodes;
        this.totalSplits = totalSplits;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.completedSplits = completedSplits;
        this.planningTimeMillis = planningTimeMillis;
        this.analysisTimeMillis = analysisTimeMillis;
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.elapsedTimeMillis = elapsedTimeMillis;
        this.finishingTimeMillis = finishingTimeMillis;
        this.physicalInputTimeMillis = physicalInputTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.peakMemoryBytes = peakMemoryBytes;
        this.physicalInputBytes = physicalInputBytes;
        this.physicalWrittenBytes = physicalWrittenBytes;
        this.internalNetworkInputBytes = internalNetworkInputBytes;
        this.rootStage = requireNonNull(rootStage, "rootStage is null");
    }

    static QueryStats create(String queryId, StatementStats stats)
    {
        return new QueryStats(
                queryId,
                stats.getState(),
                stats.isQueued(),
                stats.isScheduled(),
                stats.getProgressPercentage(),
                stats.getNodes(),
                stats.getTotalSplits(),
                stats.getQueuedSplits(),
                stats.getRunningSplits(),
                stats.getCompletedSplits(),
                stats.getPlanningTimeMillis(),
                stats.getAnalysisTimeMillis(),
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis(),
                stats.getQueuedTimeMillis(),
                stats.getElapsedTimeMillis(),
                stats.getFinishingTimeMillis(),
                stats.getPhysicalInputTimeMillis(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                stats.getPeakMemoryBytes(),
                stats.getPhysicalInputBytes(),
                stats.getPhysicalWrittenBytes(),
                stats.getInternalNetworkInputBytes(),
                Optional.ofNullable(stats.getRootStage()).map(StageStats::create));
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getState()
    {
        return state;
    }

    public boolean isQueued()
    {
        return queued;
    }

    public boolean isScheduled()
    {
        return scheduled;
    }

    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }

    public int getNodes()
    {
        return nodes;
    }

    public int getTotalSplits()
    {
        return totalSplits;
    }

    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    public int getRunningSplits()
    {
        return runningSplits;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public long getPlanningTimeMillis()
    {
        return planningTimeMillis;
    }

    public long getAnalysisTimeMillis()
    {
        return analysisTimeMillis;
    }

    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    public long getFinishingTimeMillis()
    {
        return finishingTimeMillis;
    }

    public long getPhysicalInputTimeMillis()
    {
        return physicalInputTimeMillis;
    }

    public long getProcessedRows()
    {
        return processedRows;
    }

    public long getProcessedBytes()
    {
        return processedBytes;
    }

    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    public long getPhysicalInputBytes()
    {
        return physicalInputBytes;
    }

    public long getPhysicalWrittenBytes()
    {
        return physicalWrittenBytes;
    }

    public long getInternalNetworkInputBytes()
    {
        return internalNetworkInputBytes;
    }

    public Optional<StageStats> getRootStage()
    {
        return rootStage;
    }
}
