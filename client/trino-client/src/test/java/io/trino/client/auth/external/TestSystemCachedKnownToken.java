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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

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
            throws IOException
    {
        Files.writeString(tempDir.resolve(".token"), "old-token");
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);

        assertThatThrownBy(() -> knownToken.setupToken(() -> {
            throw new RuntimeException("Auth is expected to fail");
        })).hasMessage("Auth is expected to fail");

        // Old token should be cleared, lock file should be cleaned up
        assertThat(knownToken.getToken()).isEmpty();
        assertThat(Files.exists(tempDir.resolve(".token.lck"))).isFalse();
    }

    @Test
    void testSetupTokenRemovesLockFileAfterSuccess(@TempDir Path tempDir)
    {
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);
        knownToken.setupToken(() -> Optional.of(new Token("token")));
        assertThat(Files.exists(tempDir.resolve(".token.lck"))).isFalse();
    }

    @Test
    void testSetupTokenSkipsWhenLockFileAlreadyExists(@TempDir Path tempDir)
            throws IOException
    {
        Files.writeString(tempDir.resolve(".token"), "existing-token");
        Files.createFile(tempDir.resolve(".token.lck"));

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

    @Test
    void testSetupTokenSetsOwnerOnlyPermissions(@TempDir Path tempDir)
            throws IOException
    {
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir);

        knownToken.setupToken(() -> Optional.of(new Token("secret-token")));

        assertThat(Files.getPosixFilePermissions(tempDir.resolve(".token")))
                .containsExactlyInAnyOrder(OWNER_READ, OWNER_WRITE);
    }

    @Test
    void testGetTokenThrowsWhenLockFileExistsAfterMaxWait(@TempDir Path tempDir)
            throws IOException
    {
        Files.createFile(tempDir.resolve(".token.lck"));
        SystemCachedKnownToken knownToken = new SystemCachedKnownToken(tempDir, Duration.ofMillis(1001));

        assertThatThrownBy(knownToken::getToken)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lock file")
                .hasMessageContaining("still exists after waiting");
    }

    @Test
    void testGetTokenWaitsForLockFileToBeRemoved(@TempDir Path tempDir)
            throws Exception
    {
        Files.createFile(tempDir.resolve(".token.lck"));
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

            // Remove lock file — getToken should now complete
            Files.delete(tempDir.resolve(".token.lck"));

            Optional<Token> result = future.get();
            assertThat(result)
                    .isPresent()
                    .hasValueSatisfying(token -> assertThat(token.token()).isEqualTo("the-token"));
        }
        finally {
            executor.shutdownNow();
        }
    }
}
