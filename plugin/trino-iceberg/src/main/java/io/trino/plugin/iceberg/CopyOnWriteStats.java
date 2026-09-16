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
package io.trino.plugin.iceberg;

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

/**
 * Coordinator-side counters for copy-on-write merge operations on Iceberg tables.
 *
 * <p>Exposed over JMX so operators can observe both the steady-state cost (rewritten files and
 * bytes) and the failure modes that this connector handles specially: the
 * commit-state-unknown rollback path (which intentionally leaves orphan files for
 * {@code remove_orphan_files} to clean up), the dangling delete-file pruning that V2/V3 row
 * deletes require, and the unique-rewrite-task invariant whose violation indicates a planner
 * regression that would otherwise duplicate rows.
 *
 * <p>All counters are coordinator-scoped; worker-side rewrite metrics live on
 * {@link CopyOnWriteFileRewriter.RewriteMetrics} and surface through Trino's
 * {@code OperatorStats} via {@code IcebergMergeSink}.
 *
 * <p>Each metric is an Airlift {@link CounterStat}, exported as a nested JMX bean so consumers
 * see the standard total/{@code 1m}/{@code 5m}/{@code 15m} rate windows used everywhere else
 * in Trino, with no per-connector rate computation required.
 */
@ThreadSafe
public class CopyOnWriteStats
{
    private final CounterStat commits = new CounterStat();
    private final CounterStat rewrittenFiles = new CounterStat();
    private final CounterStat rewrittenOldBytes = new CounterStat();
    private final CounterStat rewrittenNewBytes = new CounterStat();
    private final CounterStat danglingDeletesRemoved = new CounterStat();
    private final CounterStat commitStateUnknown = new CounterStat();
    private final CounterStat orphanFilesCleanedUp = new CounterStat();
    private final CounterStat uniqueRewriteTaskInvariantViolations = new CounterStat();

    /**
     * Records a successful copy-on-write commit. Insert-only MERGEs invoke this with all-zero
     * deltas so {@link #getCommits()} still observes the commit; negative or zero byte/file
     * deltas are ignored to keep the totals monotonic in the face of a buggy caller.
     */
    public void recordCommit(long rewrittenFileCount, long oldBytes, long newBytes)
    {
        commits.update(1);
        if (rewrittenFileCount > 0) {
            rewrittenFiles.update(rewrittenFileCount);
        }
        if (oldBytes > 0) {
            rewrittenOldBytes.update(oldBytes);
        }
        if (newBytes > 0) {
            rewrittenNewBytes.update(newBytes);
        }
    }

    public void recordDanglingDeletesRemoved(long count)
    {
        if (count > 0) {
            danglingDeletesRemoved.update(count);
        }
    }

    public void recordCommitStateUnknown()
    {
        commitStateUnknown.update(1);
    }

    public void recordOrphanFilesCleanedUp(long count)
    {
        if (count > 0) {
            orphanFilesCleanedUp.update(count);
        }
    }

    public void recordUniqueRewriteTaskInvariantViolation()
    {
        uniqueRewriteTaskInvariantViolations.update(1);
    }

    @Managed
    @Nested
    public CounterStat getCommits()
    {
        return commits;
    }

    @Managed
    @Nested
    public CounterStat getRewrittenFiles()
    {
        return rewrittenFiles;
    }

    @Managed
    @Nested
    public CounterStat getRewrittenOldBytes()
    {
        return rewrittenOldBytes;
    }

    @Managed
    @Nested
    public CounterStat getRewrittenNewBytes()
    {
        return rewrittenNewBytes;
    }

    @Managed
    @Nested
    public CounterStat getDanglingDeletesRemoved()
    {
        return danglingDeletesRemoved;
    }

    /**
     * Number of CoW commits whose final state was reported as unknown (catalog HTTP 5xx, IO
     * timeout, etc). The connector intentionally skips orphan-file cleanup in this case so a
     * silently-accepted snapshot is not corrupted; expect non-zero counts to correlate with
     * orphan-file build-up that {@code remove_orphan_files} will reclaim later.
     */
    @Managed
    @Nested
    public CounterStat getCommitStateUnknown()
    {
        return commitStateUnknown;
    }

    @Managed
    @Nested
    public CounterStat getOrphanFilesCleanedUp()
    {
        return orphanFilesCleanedUp;
    }

    /**
     * Counts violations of the "one rewrite task per old data file" invariant detected at commit
     * time. A non-zero value indicates a real bug in the merge layout / bucketing path: every
     * such violation, if not caught, would duplicate the surviving rows of the rewritten file.
     * Treat any non-zero rate as a P0 alert.
     */
    @Managed
    @Nested
    public CounterStat getUniqueRewriteTaskInvariantViolations()
    {
        return uniqueRewriteTaskInvariantViolations;
    }
}
