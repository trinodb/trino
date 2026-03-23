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

import com.google.common.collect.ImmutableSet;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

/**
 * This KnownToken instance persists the token to ~/.trino/.token on the filesystem,
 * allowing it to be reused across separate CLI invocations.
 * A lock file (~/.trino/.token.lck) is used to coordinate token acquisition
 * across processes — its atomic creation acts as a cross-process tryLock.
 * The implementation is similar to MemoryCachedKnownToken, but with LOCK_FILE acting as a Lock
 */
class SystemCachedKnownToken
        implements KnownToken
{
    private static final Path DEFAULT_TRINO_DIR = Path.of(System.getProperty("user.home"), ".trino");

    public static final SystemCachedKnownToken INSTANCE = new SystemCachedKnownToken(DEFAULT_TRINO_DIR);

    private final Path trinoDir;
    private final Path tokenFile;
    private final Path lockFile;

    SystemCachedKnownToken(Path trinoDir)
    {
        this.trinoDir = requireNonNull(trinoDir, "trinoDir is null");
        this.tokenFile = trinoDir.resolve(".token");
        this.lockFile = trinoDir.resolve(".token.lck");
    }

    @Override
    public Optional<Token> getToken()
    {
        // Wait while lock file exists, mimicking readLock blocking while writeLock is held
        Failsafe.with(RetryPolicy.builder()
                        .handleResultIf(Boolean.TRUE::equals)
                        .withMaxAttempts(-1)
                        .withDelay(100, 500, MILLIS)
                        .withMaxDuration(Duration.ofMinutes(10))
                        .build())
                .get(() -> Files.exists(lockFile));

        if (!Files.exists(tokenFile)) {
            return Optional.empty();
        }
        try {
            String content = Files.readString(tokenFile).trim();
            if (content.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new Token(content));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read token from " + tokenFile, e);
        }
    }

    @Override
    public void setupToken(Supplier<Optional<Token>> tokenSource)
    {
        try {
            Files.createDirectories(trinoDir);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create directory " + trinoDir, e);
        }

        // Atomically create the lock file. If it already exists, another process
        // is obtaining a token — skip, just like MemoryCachedKnownToken's tryLock.
        try {
            Files.createFile(lockFile);
        }
        catch (FileAlreadyExistsException e) {
            return;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create lock file " + lockFile, e);
        }

        try {
            // Clear token before obtaining new one, as it might fail leaving old invalid token.
            Files.deleteIfExists(tokenFile);
            Optional<Token> token = tokenSource.get();
            token.ifPresent(this::writeTokenToFile);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to update token file " + tokenFile, e);
        }
        finally {
            try {
                Files.deleteIfExists(lockFile);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to delete lock file " + lockFile, e);
            }
        }
    }

    private void writeTokenToFile(Token token)
    {
        try {
            Files.writeString(tokenFile, token.token());
            Files.setPosixFilePermissions(tokenFile, ImmutableSet.of(OWNER_READ, OWNER_WRITE));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write token to " + tokenFile, e);
        }
    }
}
