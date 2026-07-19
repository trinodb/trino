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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * A thread-safe, fixed-window rolling average of recently completed queries' peak memory.
 * <p>
 * The {@link ResourceAwareAdmissionController} uses this average as the per-query "required free memory"
 * estimate for the next admission decision. Samples are the peak <em>user</em> memory
 * reservation of completed queries, matching the general-pool free bytes the gate observes
 * through {@link ClusterCapacity}.
 * <p>
 * Backed by a count-based circular buffer of the last {@code windowSize} samples with an
 * O(1) running sum, so {@link #averageOrDefault} is cheap to call on every poll cycle. Until
 * at least one sample has been recorded, {@link #averageOrDefault} returns the supplied
 * fallback (the configured {@code required-free-memory} default), giving sane cold-start
 * behavior without special-casing the gate.
 */
final class QueryMemoryHistory
{
    private final long[] samplesBytes;

    private int nextIndex;      // ring write cursor
    private int count;          // number of valid samples (<= windowSize)
    private long sumBytes;      // running sum of valid samples, for O(1) average

    QueryMemoryHistory(int windowSize)
    {
        checkArgument(windowSize > 0, "windowSize must be positive");
        this.samplesBytes = new long[windowSize];
    }

    /**
     * Record one completed query's peak memory. Negative values are clamped to zero so a
     * misbehaving metric can never corrupt the running sum.
     */
    synchronized void record(long peakMemoryBytes)
    {
        long sample = max(0, peakMemoryBytes);
        if (count == samplesBytes.length) {
            // window full: evict the oldest sample (the one at the cursor) before overwriting it
            sumBytes -= samplesBytes[nextIndex];
        }
        else {
            count++;
        }
        samplesBytes[nextIndex] = sample;
        sumBytes += sample;
        nextIndex = (nextIndex + 1) % samplesBytes.length;
    }

    /**
     * Average peak memory over the current window, or {@code fallback} when no samples have
     * been recorded yet (cold start).
     */
    synchronized DataSize averageOrDefault(DataSize fallback)
    {
        requireNonNull(fallback, "fallback is null");
        if (count == 0) {
            return fallback;
        }
        return DataSize.ofBytes(sumBytes / count);
    }

    @VisibleForTesting
    synchronized int sampleCount()
    {
        return count;
    }
}
