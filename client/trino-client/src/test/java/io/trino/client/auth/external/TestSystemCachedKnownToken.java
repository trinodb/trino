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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.client.auth.external.TestingLockHolder.holdInAnotherProcess;
import static io.trino.client.auth.external.TestingLockHolder.holdSharedInAnotherProcess;
import static io.trino.client.auth.external.TestingLockHolder.tryLockInAnotherProcess;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
final class TestSystemCachedKnownToken
{
    @Test
    void testGetTokenReturnsEmptyWhenNoTokenFile(@TempDir Path tempDir)
    {
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        assertThat(knownToken.getToken()).isEmpty();
    }

    @Test
    void testGetTokenReturnsTokenFromFile(@TempDir Path tempDir)
            throws IOException
    {
        Files.writeString(tempDir.resolve(".token"), "test-token-value");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("test-token-value"));
    }

    @Test
    void testGetTokenReturnsEmptyForEmptyFile(@TempDir Path tempDir)
            throws IOException
    {
        Files.writeString(tempDir.resolve(".token"), "  \n  ");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        assertThat(knownToken.getToken()).isEmpty();
    }

    @Test
    void testSetupTokenWritesTokenToFile(@TempDir Path tempDir)
    {
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        knownToken.setupToken(() -> Optional.of(new Token("new-token")));
        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("new-token"));
    }

    @Test
    void testSetupTokenCreatesDirectory(@TempDir Path tempDir)
    {
        Path nestedDir = tempDir.resolve("nested").resolve("dir");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(nestedDir);
        knownToken.setupToken(() -> Optional.of(new Token("token")));
        assertThat(Files.isDirectory(nestedDir)).isTrue();
        assertThat(knownToken.getToken()).isPresent();
    }

    @Test
    void testSetupTokenClearsOldTokenBeforeObtainingNew(@TempDir Path tempDir)
            throws IOException
    {
        Files.writeString(tempDir.resolve(".token"), "old-token");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        knownToken.setupToken(() -> Optional.of(new Token("new-token")));
        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("new-token"));
    }

    @Test
    void testSetupTokenClearsTokenWhenSourceReturnsEmpty(@TempDir Path tempDir)
            throws IOException
    {
        Files.writeString(tempDir.resolve(".token"), "old-token");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        knownToken.setupToken(Optional::empty);
        assertThat(knownToken.getToken()).isEmpty();
    }

    @Test
    void testSetupTokenClearsTokenWhenSourceFails(@TempDir Path tempDir)
            throws Exception
    {
        Files.writeString(tempDir.resolve(".token"), "old-token");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);

        assertThatThrownBy(() -> knownToken.setupToken(() -> {
            throw new RuntimeException("Auth is expected to fail");
        })).hasMessage("Auth is expected to fail");

        // Old token should be cleared, lock should be released
        assertThat(knownToken.getToken()).isEmpty();
        assertThat(tryLockInAnotherProcess(tempDir.resolve(".token.lock"))).isTrue();
    }

    @Test
    void testSetupTokenReleasesLockAfterSuccess(@TempDir Path tempDir)
            throws Exception
    {
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        knownToken.setupToken(() -> Optional.of(new Token("token")));
        assertThat(tryLockInAnotherProcess(tempDir.resolve(".token.lock"))).isTrue();
    }

    @Test
    void testSetupTokenHoldsLockDuringAuthentication(@TempDir Path tempDir)
    {
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        knownToken.setupToken(() -> {
            try {
                assertThat(tryLockInAnotherProcess(tempDir.resolve(".token.lock"))).isFalse();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Optional.of(new Token("token"));
        });
        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("token"));
    }

    @Test
    void testSetupTokenSkipsWhileAnotherProcessHoldsLock(@TempDir Path tempDir)
            throws Exception
    {
        Files.writeString(tempDir.resolve(".token"), "existing-token");
        Process holder = holdInAnotherProcess(tempDir.resolve(".token.lock"));
        try {
            AtomicBoolean tokenSourceCalled = new AtomicBoolean(false);
            SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);

            knownToken.setupToken(() -> {
                tokenSourceCalled.set(true);
                return Optional.of(new Token("should-not-be-written"));
            });

            // Token source should not have been invoked
            assertThat(tokenSourceCalled.get()).isFalse();
            // Original token file should be unchanged
            assertThat(Files.readString(tempDir.resolve(".token"))).isEqualTo("existing-token");
        }
        finally {
            holder.destroyForcibly();
        }
    }

    @Test
    void testSetupTokenWaitsBrieflyForLock(@TempDir Path tempDir)
            throws Exception
    {
        // Another process takes the lock for a moment, as getToken does to check it
        Process holder = holdSharedInAnotherProcess(tempDir.resolve(".token.lock"));
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir, Duration.ofMinutes(1), Duration.ofSeconds(10));
        FutureTask<Void> setup = new FutureTask<>(() -> knownToken.setupToken(() -> Optional.of(new Token("new-token"))), null);

        Thread setupThread = new Thread(setup);
        setupThread.start();
        try {
            // Sleeping between attempts to take the lock
            while (setupThread.getState() != TIMED_WAITING) {
                assertThat(setupThread.isAlive()).isTrue();
                Thread.sleep(1);
            }
        }
        finally {
            holder.destroyForcibly().waitFor();
        }
        setup.get();

        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("new-token"));
    }

    @Test
    void testSetupTokenSkipsWhenTokenAppearedWhileWaitingForLock(@TempDir Path tempDir)
            throws Exception
    {
        // Long enough for the second authentication to take the lock after the first one
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir, Duration.ofMinutes(1), Duration.ofSeconds(10));
        CountDownLatch firstAuthenticationStarted = new CountDownLatch(1);
        CountDownLatch finishFirstAuthentication = new CountDownLatch(1);
        AtomicBoolean secondTokenSourceCalled = new AtomicBoolean(false);

        FutureTask<Void> first = new FutureTask<>(() -> knownToken.setupToken(() -> {
            firstAuthenticationStarted.countDown();
            try {
                finishFirstAuthentication.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return Optional.of(new Token("first-token"));
        }), null);
        FutureTask<Void> second = new FutureTask<>(() -> knownToken.setupToken(() -> {
            secondTokenSourceCalled.set(true);
            return Optional.of(new Token("second-token"));
        }), null);

        new Thread(first).start();
        try {
            firstAuthenticationStarted.await();
            Thread secondThread = new Thread(second);
            secondThread.start();
            // The second authentication reads the token file before it starts sleeping between attempts to take the lock
            while (secondThread.getState() != TIMED_WAITING) {
                assertThat(secondThread.isAlive()).isTrue();
                Thread.sleep(1);
            }
        }
        finally {
            finishFirstAuthentication.countDown();
        }
        first.get();
        second.get();

        assertThat(secondTokenSourceCalled.get()).isFalse();
        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("first-token"));
    }

    @Test
    void testLockFileOfOlderClientIsIgnored(@TempDir Path tempDir)
            throws IOException
    {
        // Older clients locked by creating .token.lck and left it behind when they exited during a login
        Files.createFile(tempDir.resolve(".token.lck"));
        Files.writeString(tempDir.resolve(".token"), "existing-token");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir, Duration.ofMillis(1001));

        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("existing-token"));

        knownToken.setupToken(() -> Optional.of(new Token("new-token")));
        assertThat(knownToken.getToken())
                .isPresent()
                .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("new-token"));
    }

    @Test
    void testSetupTokenSetsOwnerOnlyPermissions(@TempDir Path tempDir)
            throws IOException
    {
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);

        knownToken.setupToken(() -> Optional.of(new Token("secret-token")));

        assertThat(Files.getPosixFilePermissions(tempDir.resolve(".token")))
                .containsExactlyInAnyOrder(OWNER_READ, OWNER_WRITE);
        assertThat(Files.getPosixFilePermissions(tempDir.resolve(".token.lock")))
                .containsExactlyInAnyOrder(OWNER_READ, OWNER_WRITE);
    }

    @Test
    void testGetTokenThrowsWhenLockIsHeldAfterMaxWait(@TempDir Path tempDir)
            throws Exception
    {
        Process holder = holdInAnotherProcess(tempDir.resolve(".token.lock"));
        try {
            SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir, Duration.ofMillis(1001));

            assertThatThrownBy(knownToken::getToken)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Lock file")
                    .hasMessageContaining("is still locked after waiting");
        }
        finally {
            holder.destroyForcibly();
        }
    }

    @Test
    void testGetTokenFailsOnIoError(@TempDir Path tempDir)
            throws IOException
    {
        // A directory cannot be opened for writing
        Files.createDirectory(tempDir.resolve(".token.lock"));
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);

        assertThatThrownBy(knownToken::getToken)
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to open lock file");
    }

    @Test
    void testGetTokenWaitsForLockToBeReleased(@TempDir Path tempDir)
            throws Exception
    {
        Process holder = holdInAnotherProcess(tempDir.resolve(".token.lock"));
        Files.writeString(tempDir.resolve(".token"), "the-token");

        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        CountDownLatch getTokenStarted = new CountDownLatch(1);

        ExecutorService executor = newSingleThreadExecutor();
        try {
            Future<Optional<Token>> future = executor.submit(() -> {
                getTokenStarted.countDown();
                return knownToken.getToken();
            });

            getTokenStarted.await();
            // Give getToken time to enter the retry loop
            Thread.sleep(300);
            assertThat(future.isDone()).isFalse();

            // Kill the process holding the lock, as when an IDE is killed during a login. getToken should now complete
            holder.destroyForcibly().waitFor();

            Optional<Token> result = future.get();
            assertThat(result)
                    .isPresent()
                    .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("the-token"));
        }
        finally {
            executor.shutdownNow();
            holder.destroyForcibly();
        }
    }
}
