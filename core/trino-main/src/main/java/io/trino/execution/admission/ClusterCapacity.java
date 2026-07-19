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
package io.trino.execution.admission;

import io.trino.memory.MemoryInfo;

import java.util.Collection;
import java.util.Optional;

import static java.lang.Math.max;
import static java.lang.Math.round;

/**
 * A point-in-time snapshot of observed cluster capacity, derived from the per-worker
 * {@link MemoryInfo} reported to {@code ClusterMemoryManager}.
 *
 * @param freeMemoryBytes summed free bytes of the general memory pool across observed workers
 * @param availableVcpu approximate free vCPU across observed workers, derived from
 *         {@code availableProcessors * (1 - systemCpuLoad)}
 */
public record ClusterCapacity(long freeMemoryBytes, int availableVcpu)
{
    public ClusterCapacity
    {
        if (freeMemoryBytes < 0) {
            throw new IllegalArgumentException("freeMemoryBytes must not be negative");
        }
        if (availableVcpu < 0) {
            throw new IllegalArgumentException("availableVcpu must not be negative");
        }
    }

    /**
     * Folds a collection of per-worker memory info (as returned by
     * {@code ClusterMemoryManager.getWorkersMemoryInfo()}) into a single capacity snapshot.
     * Workers that have not yet reported memory info are skipped.
     */
    public static ClusterCapacity from(Collection<Optional<MemoryInfo>> workerMemoryInfo)
    {
        long freeMemoryBytes = 0;
        long availableVcpu = 0;
        for (Optional<MemoryInfo> maybeInfo : workerMemoryInfo) {
            if (maybeInfo.isEmpty()) {
                continue;
            }
            MemoryInfo info = maybeInfo.get();
            freeMemoryBytes += info.getPool().getFreeBytes();
            double idleFraction = max(0.0, 1.0 - info.getSystemCpuLoad());
            availableVcpu += round(info.getAvailableProcessors() * idleFraction);
        }
        return new ClusterCapacity(max(0, freeMemoryBytes), (int) max(0, availableVcpu));
    }
}
