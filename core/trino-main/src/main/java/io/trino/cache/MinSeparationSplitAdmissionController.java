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
package io.trino.cache;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.cache.CacheSplitId;
import it.unimi.dsi.fastutil.ints.Int2LongArrayMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * A simple split admission controller that ensures that a worker processes a minimum number of splits
 * with distinct CacheSplitID before scheduling the next batch of splits which can contain splits with the
 * same CacheSplitID.
 */
public class MinSeparationSplitAdmissionController
        implements SplitAdmissionController
{
    private static final int MAX_CACHE_SPLITS = 1_000_000;
    private static final long PENDING_SPLIT_MARKER = Long.MAX_VALUE;
    private static final long NOT_SCHEDULED_SPLIT_MARKER = -1;

    private final int minSplitSeparation;

    @GuardedBy("this")
    private final Map<HostAddress, WorkerInfo> workerInfos = new HashMap<>();

    public MinSeparationSplitAdmissionController(int minSplitSeparation)
    {
        verify(minSplitSeparation > 0, "minSplitSeparation must be greater than 0");
        this.minSplitSeparation = minSplitSeparation;
    }

    @Override
    public synchronized boolean canScheduleSplit(CacheSplitId splitId, HostAddress address)
    {
        WorkerInfo workerInfo = workerInfos.computeIfAbsent(address, _ -> new WorkerInfo());
        int cacheSplitKey = getCacheSplitKey(splitId);
        long splitSequenceId = workerInfo.scheduledSplits.get(cacheSplitKey);
        if (splitSequenceId == NOT_SCHEDULED_SPLIT_MARKER) {
            // We use PENDING_SPLIT_MARKER to indicate that a split with a given CacheSplitId has been added
            // to the queue for scheduling. However, it has not been scheduled yet. This marker prevents returning
            // splits with the same CacheSplitId from concurrently executing CacheSplitSources.
            workerInfo.scheduledSplits.put(cacheSplitKey, PENDING_SPLIT_MARKER);
            return true;
        }

        // Count-based heuristic
        return workerInfo.processedSplitCount - splitSequenceId >= minSplitSeparation;
    }

    @Override
    public synchronized void splitsScheduled(List<Split> splits)
    {
        for (Split split : splits) {
            Optional<CacheSplitId> cacheSplitId = split.getCacheSplitId();
            List<HostAddress> addresses = split.getAddresses();
            // We only care about splits that are cacheable and have preferred addresses (worker)
            if (cacheSplitId.isPresent()) {
                HostAddress address = getOnlyElement(addresses);
                WorkerInfo workerInfo = workerInfos.computeIfAbsent(address, _ -> new WorkerInfo());
                int cacheSplitKey = getCacheSplitKey(cacheSplitId.get());
                // Do not update split sequence id if the split was already executed. This way subsequent splits with same
                // split id won't have to hold execution of the split
                long splitSequenceId = workerInfo.scheduledSplits.get(cacheSplitKey);
                checkState(
                        splitSequenceId != NOT_SCHEDULED_SPLIT_MARKER,
                        "Expected sequence ID for a split: %s",
                        cacheSplitId.get());
                if (splitSequenceId == PENDING_SPLIT_MARKER) {
                    workerInfo.scheduledSplits.put(cacheSplitKey, workerInfo.processedSplitCount++);
                }
            }
        }
    }

    private static int getCacheSplitKey(CacheSplitId splitId)
    {
        // Use the hash code of the split id as the key to cap memory usage
        return splitId.hashCode() % MAX_CACHE_SPLITS;
    }

    private static final class WorkerInfo
    {
        private final Int2LongMap scheduledSplits;
        private long processedSplitCount;

        public WorkerInfo()
        {
            scheduledSplits = new Int2LongArrayMap();
            // Set the default return value to NOT_SCHEDULED_SPLIT_MARKER to indicate that the split
            // has not been scheduled yet.
            scheduledSplits.defaultReturnValue(NOT_SCHEDULED_SPLIT_MARKER);
            processedSplitCount = 0;
        }
    }
}
