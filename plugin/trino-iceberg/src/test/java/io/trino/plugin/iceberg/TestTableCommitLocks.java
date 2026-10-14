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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types.LongType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(10)
final class TestTableCommitLocks
{
    private static final String TABLE_UUID = "9a1f7c2e-0000-0000-0000-000000000001";
    private static final String OTHER_TABLE_UUID = "9a1f7c2e-0000-0000-0000-000000000002";
    private static final String V1_METADATA_WITHOUT_UUID =
            """
            {
              "format-version": 1,
              "location": "local:///legacy_table",
              "last-updated-ms": 0,
              "last-column-id": 1,
              "schema": {"type": "struct", "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
              "partition-spec": []
            }
            """;

    private final List<String> commits = new CopyOnWriteArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<Thread> threads = new CopyOnWriteArrayList<>();
    private final List<Throwable> threadFailures = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown()
    {
        releaseAndJoin();
        assertThat(threadFailures).isEmpty();
    }

    @Test
    void testCommitsToTheSameTableRunInArrivalOrder()
    {
        TableCommitLocks locks = newCommitLocks(new Duration(1, MINUTES));
        holdLock(locks, TABLE_UUID);
        queueCommit(locks, TABLE_UUID, "first");
        queueCommit(locks, TABLE_UUID, "second");

        locks.runWithLock(OTHER_TABLE_UUID, record("other table"));
        releaseAndJoin();

        assertThat(commits).containsExactly("other table", "holder", "first", "second");
    }

    @Test
    void testCommitWithoutLockAfterWaitTimeout()
    {
        TableCommitLocks locks = newCommitLocks(new Duration(10, MILLISECONDS));
        holdLock(locks, TABLE_UUID);

        locks.runWithLock(TABLE_UUID, record("timed out"));
        releaseAndJoin();

        assertThat(commits).containsExactly("timed out", "holder");
    }

    @Test
    void testInterruptedWhileWaiting()
    {
        TableCommitLocks locks = newCommitLocks(new Duration(1, MINUTES));
        holdLock(locks, TABLE_UUID);
        Thread waiter = startThread(() -> {
            assertThatThrownBy(() -> locks.runWithLock(TABLE_UUID, record("waiter")))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_COMMIT_ERROR.toErrorCode()));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        });
        awaitParked(waiter);

        waiter.interrupt();
        releaseAndJoin();

        assertThat(commits).containsExactly("holder");
    }

    @Test
    void testLocksOnUuidFromTableMetadata()
    {
        TableCommitLocks locks = newCommitLocks(new Duration(1, MINUTES));
        TableMetadata metadata = newTableMetadata(new Schema(required(1, "x", LongType.get())), PartitionSpec.unpartitioned(), "local:///table", ImmutableMap.of());
        holdLock(locks, metadata.uuid());
        awaitParked(startThread(() -> locks.runWithLock(table(metadata), record("same uuid"))));

        releaseAndJoin();

        assertThat(commits).containsExactly("holder", "same uuid");
    }

    @Test
    void testTableWithoutUuidCommitsWithoutLock()
    {
        TableCommitLocks locks = newCommitLocks(new Duration(1, MINUTES));

        locks.runWithLock(table(TableMetadataParser.fromJson(V1_METADATA_WITHOUT_UUID)), record("no uuid"));

        assertThat(commits).containsExactly("no uuid");
    }

    private static TableCommitLocks newCommitLocks(Duration waitTimeout)
    {
        return new TableCommitLocks(new IcebergConfig().setCommitLockWaitTimeout(waitTimeout));
    }

    private static Table table(TableMetadata metadata)
    {
        return new BaseTable(new StaticTableOperations(metadata, null), "table");
    }

    private Runnable record(String commit)
    {
        return () -> commits.add(commit);
    }

    // Starts a commit that holds the table's lock until releaseAndJoin, recording "holder"
    private void holdLock(TableCommitLocks locks, String tableUuid)
    {
        awaitParked(startThread(() -> locks.runWithLock(tableUuid, () -> {
            await(release);
            commits.add("holder");
        })));
    }

    // Starts a commit and returns once it waits for the table's lock
    private void queueCommit(TableCommitLocks locks, String tableUuid, String commit)
    {
        awaitParked(startThread(() -> locks.runWithLock(tableUuid, record(commit))));
    }

    private Thread startThread(Runnable runnable)
    {
        Thread thread = new Thread(runnable);
        // A failed assertion must not leave the thread running
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((_, failure) -> threadFailures.add(failure));
        threads.add(thread);
        thread.start();
        return thread;
    }

    private void releaseAndJoin()
    {
        release.countDown();
        for (Thread thread : threads) {
            try {
                thread.join();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // The holder parks on its latch and waiters park in a timed tryLock
    private static void awaitParked(Thread thread)
    {
        while (thread.getState() != WAITING && thread.getState() != TIMED_WAITING) {
            assertThat(thread.getState()).as("thread finished instead of waiting").isNotEqualTo(TERMINATED);
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
