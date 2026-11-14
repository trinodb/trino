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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.spi.TrinoException;
import jakarta.annotation.Nullable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;

import java.util.concurrent.locks.ReentrantLock;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Serializes commits to the same table, identified by its UUID, from this coordinator in arrival order. A commit runs
 * without the lock after waiting longer than the timeout, so a stuck commit cannot hold up the table, or when the table
 * has no UUID.
 */
public class TableCommitLocks
{
    private static final Logger log = Logger.get(TableCommitLocks.class);

    private final long waitTimeoutMillis;
    // A lock is only collected once no commit holds or waits for it
    private final NonEvictableLoadingCache<String, ReentrantLock> locks = buildNonEvictableCache(
            CacheBuilder.newBuilder().weakValues(),
            CacheLoader.from(() -> new ReentrantLock(true)));

    @Inject
    public TableCommitLocks(IcebergConfig config)
    {
        this.waitTimeoutMillis = config.getCommitLockWaitTimeout().toMillis();
    }

    public void runWithLock(Table table, Runnable commit)
    {
        // Read the UUID from the metadata, as Table.uuid() fails for v1 tables written without one
        runWithLock(((HasTableOperations) table).operations().current().uuid(), commit);
    }

    @VisibleForTesting
    void runWithLock(@Nullable String tableUuid, Runnable commit)
    {
        if (tableUuid == null) {
            commit.run();
            return;
        }

        ReentrantLock lock = locks.getUnchecked(tableUuid);
        try {
            boolean locked = lock.tryLock(waitTimeoutMillis, MILLISECONDS);
            if (!locked) {
                log.warn("Commit to table %s waited %s for the commit lock, committing without it", tableUuid, new Duration(waitTimeoutMillis, MILLISECONDS));
            }
            try {
                commit.run();
            }
            finally {
                if (locked) {
                    lock.unlock();
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Interrupted while waiting to commit to table " + tableUuid, e);
        }
    }
}
