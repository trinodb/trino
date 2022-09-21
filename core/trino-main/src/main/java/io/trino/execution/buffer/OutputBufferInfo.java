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
package io.trino.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.metrics.TDigestHistogram;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class OutputBufferInfo
{
    private final String type;
    private final BufferState state;
    private final boolean canAddBuffers;
    private final boolean canAddPages;
    private final long totalBufferedBytes;
    private final long totalBufferedPages;
    private final long totalRowsSent;
    private final long totalPagesSent;
    private final Optional<List<PipelinedBufferInfo>> pipelinedBufferStates;
    private final Optional<TDigestHistogram> utilization;
    private final Optional<SpoolingOutputStats.Snapshot> spoolingOutputStats;

    @JsonCreator
    public OutputBufferInfo(
            @JsonProperty("type") String type,
            @JsonProperty("state") BufferState state,
            @JsonProperty("canAddBuffers") boolean canAddBuffers,
            @JsonProperty("canAddPages") boolean canAddPages,
            @JsonProperty("totalBufferedBytes") long totalBufferedBytes,
            @JsonProperty("totalBufferedPages") long totalBufferedPages,
            @JsonProperty("totalRowsSent") long totalRowsSent,
            @JsonProperty("totalPagesSent") long totalPagesSent,
            @JsonProperty("pipelinedBufferStates") Optional<List<PipelinedBufferInfo>> pipelinedBufferStates,
            @JsonProperty("utilization") Optional<TDigestHistogram> utilization,
            @JsonProperty("spoolingOutputStats") Optional<SpoolingOutputStats.Snapshot> spoolingOutputStats)
    {
        this.type = type;
        this.state = state;
        this.canAddBuffers = canAddBuffers;
        this.canAddPages = canAddPages;
        this.totalBufferedBytes = totalBufferedBytes;
        this.totalBufferedPages = totalBufferedPages;
        this.totalRowsSent = totalRowsSent;
        this.totalPagesSent = totalPagesSent;
        this.pipelinedBufferStates = requireNonNull(pipelinedBufferStates, "pipelinedBufferStates is null").map(ImmutableList::copyOf);
        this.utilization = utilization;
        this.spoolingOutputStats = requireNonNull(spoolingOutputStats, "spoolingOutputStats is null");
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public BufferState getState()
    {
        return state;
    }

    @JsonProperty
    public Optional<List<PipelinedBufferInfo>> getPipelinedBufferStates()
    {
        return pipelinedBufferStates;
    }

    @JsonProperty
    public boolean isCanAddBuffers()
    {
        return canAddBuffers;
    }

    @JsonProperty
    public boolean isCanAddPages()
    {
        return canAddPages;
    }

    @JsonProperty
    public long getTotalBufferedBytes()
    {
        return totalBufferedBytes;
    }

    @JsonProperty
    public long getTotalBufferedPages()
    {
        return totalBufferedPages;
    }

    @JsonProperty
    public long getTotalRowsSent()
    {
        return totalRowsSent;
    }

    @JsonProperty
    public long getTotalPagesSent()
    {
        return totalPagesSent;
    }

    @JsonProperty
    public Optional<TDigestHistogram> getUtilization()
    {
        return utilization;
    }

    @JsonProperty
    public Optional<SpoolingOutputStats.Snapshot> getSpoolingOutputStats()
    {
        return spoolingOutputStats;
    }

    public OutputBufferInfo summarize()
    {
        return new OutputBufferInfo(
                type,
                state,
                canAddBuffers,
                canAddPages,
                totalBufferedBytes,
                totalBufferedPages,
                totalRowsSent,
                totalPagesSent,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public OutputBufferInfo summarizeFinal()
    {
        return new OutputBufferInfo(
                type,
                state,
                canAddBuffers,
                canAddPages,
                totalBufferedBytes,
                totalBufferedPages,
                totalRowsSent,
                totalPagesSent,
                Optional.empty(),
                utilization,
                spoolingOutputStats);
    }

    public OutputBufferInfo pruneSpoolingOutputStats()
    {
        return new OutputBufferInfo(
                type,
                state,
                canAddBuffers,
                canAddPages,
                totalBufferedBytes,
                totalBufferedPages,
                totalRowsSent,
                totalPagesSent,
                pipelinedBufferStates,
                utilization,
                Optional.empty());
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
        OutputBufferInfo that = (OutputBufferInfo) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(canAddBuffers, that.canAddBuffers) &&
                Objects.equals(canAddPages, that.canAddPages) &&
                Objects.equals(totalBufferedBytes, that.totalBufferedBytes) &&
                Objects.equals(totalBufferedPages, that.totalBufferedPages) &&
                Objects.equals(totalRowsSent, that.totalRowsSent) &&
                Objects.equals(totalPagesSent, that.totalPagesSent) &&
                state == that.state &&
                Objects.equals(pipelinedBufferStates, that.pipelinedBufferStates) &&
                Objects.equals(utilization, that.utilization);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, canAddBuffers, canAddPages, totalBufferedBytes, totalBufferedPages, totalRowsSent, totalPagesSent, pipelinedBufferStates, utilization);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("state", state)
                .add("canAddBuffers", canAddBuffers)
                .add("canAddPages", canAddPages)
                .add("totalBufferedBytes", totalBufferedBytes)
                .add("totalBufferedPages", totalBufferedPages)
                .add("totalRowsSent", totalRowsSent)
                .add("totalPagesSent", totalPagesSent)
                .add("pipelinedBufferStates", pipelinedBufferStates)
                .add("bufferUtilization", utilization)
                .toString();
    }
}
