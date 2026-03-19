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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.metrics.Metrics;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record OutputBufferInfo(
        String type,
        BufferState state,
        boolean canAddBuffers,
        boolean canAddPages,
        long totalBufferedBytes,
        long totalBufferedPages,
        long totalRowsSent,
        long totalPagesSent,
        Optional<List<PipelinedBufferInfo>> pipelinedBufferStates,
        Optional<TDigestHistogram> utilization,
        Optional<SpoolingOutputStats.Snapshot> spoolingOutputStats,
        Optional<Metrics> metrics)
{
    public OutputBufferInfo
    {
        pipelinedBufferStates = requireNonNull(pipelinedBufferStates, "pipelinedBufferStates is null").map(ImmutableList::copyOf);
        requireNonNull(spoolingOutputStats, "spoolingOutputStats is null");
        requireNonNull(metrics, "metrics is null");
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
                spoolingOutputStats,
                metrics);
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
                Optional.empty(),
                metrics);
    }
}
