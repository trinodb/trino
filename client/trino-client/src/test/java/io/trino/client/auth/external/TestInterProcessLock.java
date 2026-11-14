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
package io.trino.client.auth.external;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.trino.client.auth.external.TestingLockHolder.acquiredLock;
import static io.trino.client.auth.external.TestingLockHolder.holdInAnotherProcess;
import static io.trino.client.auth.external.TestingLockHolder.holdSharedInAnotherProcess;
import static io.trino.client.auth.external.TestingLockHolder.startLockHolder;
import static io.trino.client.auth.external.TestingLockHolder.tryLockInAnotherProcess;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
final class TestInterProcessLock
{
    @Test
    void testTryLockIsExclusive(@TempDir Path tempDir)
            throws Exception
    {
        Path lockFile = tempDir.resolve("lock");
        InterProcessLock lock = new InterProcessLock(lockFile);
        assertThat(lock.isLocked()).isFalse();

        assertThat(lock.tryLock()).isTrue();
        assertThat(lock.isLocked()).isTrue();
        assertThat(lock.tryLock()).isFalse();
        assertThat(tryLockInAnotherProcess(lockFile)).isFalse();

        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
        assertThat(tryLockInAnotherProcess(lockFile)).isTrue();
    }

    @Test
    void testIsLockedDoesNotCreateLockFile(@TempDir Path tempDir)
    {
        Path lockFile = tempDir.resolve("lock");
        assertThat(new InterProcessLock(lockFile).isLocked()).isFalse();
        assertThat(Files.exists(lockFile)).isFalse();
    }

    @Test
    void testTryLockFromManyThreads(@TempDir Path tempDir)
            throws Exception
    {
        InterProcessLock lock = new InterProcessLock(tempDir.resolve("lock"));
        int threads = 8;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(executor.submit(() -> {
                    barrier.await();
                    return lock.tryLock();
                }));
            }
            int acquired = 0;
            for (Future<Boolean> result : results) {
                if (result.get()) {
                    acquired++;
                }
            }
            assertThat(acquired).isEqualTo(1);
        }
        finally {
            executor.shutdownNow();
        }
        lock.unlock();
    }

    @Test
    void testTryLockFromManyProcesses(@TempDir Path tempDir)
            throws Exception
    {
        Path lockFile = tempDir.resolve("lock");
        List<Process> processes = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                processes.add(startLockHolder(lockFile));
            }
            int acquired = 0;
            for (Process process : processes) {
                if (acquiredLock(process)) {
                    acquired++;
                }
            }
            assertThat(acquired).isEqualTo(1);
            assertThat(new InterProcessLock(lockFile).isLocked()).isTrue();
        }
        finally {
            for (Process process : processes) {
                process.destroyForcibly();
            }
        }
    }

    @Test
    void testTryLockWithTimeoutWaitsForRelease(@TempDir Path tempDir)
            throws Exception
    {
        InterProcessLock lock = new InterProcessLock(tempDir.resolve("lock"));
        assertThat(lock.tryLock()).isTrue();

        ExecutorService executor = newFixedThreadPool(1);
        try {
            Future<Boolean> waiter = executor.submit(() -> lock.tryLock(Duration.ofSeconds(10)));
            Thread.sleep(200);
            assertThat(waiter.isDone()).isFalse();

            lock.unlock();
            assertThat(waiter.get()).isTrue();
        }
        finally {
            executor.shutdownNow();
        }
        lock.unlock();
    }

    @Test
    void testTryLockWithTimeoutFailsOnIoError(@TempDir Path tempDir)
    {
        // A directory cannot be opened for writing
        InterProcessLock lock = new InterProcessLock(tempDir);
        long start = System.nanoTime();

        assertThatThrownBy(() -> lock.tryLock(Duration.ofSeconds(10)))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to open lock file");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void testLockIsKeptWhenAnotherInstanceChecksIt(@TempDir Path tempDir)
            throws Exception
    {
        Path lockFile = tempDir.resolve("lock");
        InterProcessLock lock = new InterProcessLock(lockFile);
        assertThat(lock.tryLock()).isTrue();

        // On Unix, closing any descriptor of the file would release the lock of the whole process
        InterProcessLock otherInstance = new InterProcessLock(lockFile);
        assertThat(otherInstance.isLocked()).isTrue();
        assertThat(otherInstance.tryLock()).isFalse();
        assertThat(tryLockInAnotherProcess(lockFile)).isFalse();
        // Keeps the other instance reachable, as the JDK closes its channel when it is garbage collected
        assertThat(otherInstance.isLocked()).isTrue();

        lock.unlock();
    }

    @Test
    void testChecksDoNotConflict(@TempDir Path tempDir)
            throws Exception
    {
        Path lockFile = tempDir.resolve("lock");
        // Another process is in the middle of a check
        Process checker = holdSharedInAnotherProcess(lockFile);
        try {
            InterProcessLock lock = new InterProcessLock(lockFile);
            assertThat(lock.isLocked()).isFalse();
            assertThat(lock.tryLock()).isFalse();
        }
        finally {
            checker.destroyForcibly().waitFor();
        }
    }

    @Test
    void testLockFileIsDeleted(@TempDir Path tempDir)
            throws Exception
    {
        Path lockFile = tempDir.resolve("lock");
        InterProcessLock lock = new InterProcessLock(lockFile);
        assertThat(lock.tryLock()).isTrue();
        lock.unlock();

        // The file is deleted, for example when a user cleans up ~/.trino, and another process creates a new one
        Files.delete(lockFile);
        Process holder = holdInAnotherProcess(lockFile);
        try {
            assertThat(lock.isLocked()).isTrue();
            assertThat(lock.tryLock()).isFalse();
        }
        finally {
            holder.destroyForcibly().waitFor();
        }

        assertThat(lock.tryLock()).isTrue();
        assertThat(tryLockInAnotherProcess(lockFile)).isFalse();
        lock.unlock();
    }

    @Test
    void testLockIsReleasedWhenHoldingProcessExits(@TempDir Path tempDir)
            throws Exception
    {
        Path lockFile = tempDir.resolve("lock");
        Process holder = holdInAnotherProcess(lockFile);
        try {
            InterProcessLock lock = new InterProcessLock(lockFile);
            assertThat(lock.isLocked()).isTrue();
            assertThat(lock.tryLock()).isFalse();

            // The process is killed without releasing the lock, as when an IDE is killed during a login
            holder.destroyForcibly().waitFor();
            assertThat(lock.isLocked()).isFalse();
            assertThat(lock.tryLock()).isTrue();
            lock.unlock();
        }
        finally {
            holder.destroyForcibly();
        }
    }
}
