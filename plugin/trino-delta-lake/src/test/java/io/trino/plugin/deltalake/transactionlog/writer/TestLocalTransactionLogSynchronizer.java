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
package io.trino.plugin.deltalake.transactionlog.writer;

import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.NoOpTableCredentialsProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestLocalTransactionLogSynchronizer
{
    private static LocalTransactionLogSynchronizer createSynchronizer(Path rootPath)
    {
        return new LocalTransactionLogSynchronizer(new DefaultDeltaLakeFileSystemFactory(new LocalFileSystemFactory(rootPath), new NoOpTableCredentialsProvider()));
    }

    @Test
    void testIsSafe()
    {
        assertThat(createSynchronizer(Path.of(System.getProperty("java.io.tmpdir"))).isUnsafe()).isFalse();
    }

    @Test
    void testConflictingWriteFails(@TempDir Path rootPath)
    {
        LocalTransactionLogSynchronizer synchronizer = createSynchronizer(rootPath);
        Location entry = Location.of("local:///_delta_log/00000000000000000001.json");

        synchronizer.write(SESSION, Optional.empty(), "cluster", entry, "first".getBytes(UTF_8));

        assertThatThrownBy(() -> synchronizer.write(SESSION, Optional.empty(), "cluster", entry, "second".getBytes(UTF_8)))
                .isInstanceOf(TransactionConflictException.class);
    }

    @Test
    void testConcurrentWritesOnlyOneWins(@TempDir Path rootPath)
            throws Exception
    {
        LocalTransactionLogSynchronizer synchronizer = createSynchronizer(rootPath);
        Location entry = Location.of("local:///_delta_log/00000000000000000001.json");

        int writers = 8;
        CyclicBarrier barrier = new CyclicBarrier(writers);
        ExecutorService executor = newFixedThreadPool(writers, daemonThreadsNamed("testConcurrentWritesOnlyOneWins-%s"));
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < writers; i++) {
                int writer = i;
                futures.add(executor.submit(() -> {
                    barrier.await(20, SECONDS);
                    try {
                        synchronizer.write(SESSION, Optional.empty(), "cluster", entry, ("writer-" + writer).getBytes(UTF_8));
                        return true;
                    }
                    catch (TransactionConflictException e) {
                        return false;
                    }
                }));
            }
            long successes = 0;
            for (Future<Boolean> future : futures) {
                if (future.get(20, SECONDS)) {
                    successes++;
                }
            }
            assertThat(successes).isEqualTo(1);
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(20, SECONDS)).isTrue();
        }
    }
}
