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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
 * A lock on ~/.trino/.token.lock coordinates token acquisition across processes.
 * The operating system releases it when the process holding it exits.
 * The implementation is similar to MemoryCachedKnownToken, but with the file lock acting as a Lock
 */
class SystemCachedKnownToken
        implements KnownToken
{
    private static final Path DEFAULT_TRINO_DIR = Path.of(System.getProperty("user.home"), ".trino");
    // duration for the user to authenticate within IDP. It involves clicking and typing from a real person within a browser, so should be counted in minutes.
    private static final Duration DEFAULT_LOCK_MAX_WAIT = Duration.ofMinutes(10);
    // getToken in other processes briefly takes a shared lock to check the lock, which must not be mistaken for a login in progress
    private static final Duration DEFAULT_LOCK_ACQUIRE_TIMEOUT = Duration.ofMillis(500);

    public static final SystemCachedKnownToken INSTANCE = new SystemCachedKnownToken(DEFAULT_TRINO_DIR);

    private final Path trinoDir;
    private final Path tokenFile;
    private final Path lockFile;
    private final InterProcessLock lock;
    private final Duration lockMaxWait;
    private final Duration lockAcquireTimeout;

    SystemCachedKnownToken(Path trinoDir)
    {
        this(trinoDir, DEFAULT_LOCK_MAX_WAIT);
    }

    SystemCachedKnownToken(Path trinoDir, Duration lockMaxWait)
    {
        this(trinoDir, lockMaxWait, DEFAULT_LOCK_ACQUIRE_TIMEOUT);
    }

    SystemCachedKnownToken(Path trinoDir, Duration lockMaxWait, Duration lockAcquireTimeout)
    {
        this.trinoDir = requireNonNull(trinoDir, "trinoDir is null");
        this.tokenFile = trinoDir.resolve(".token");
        this.lockFile = trinoDir.resolve(".token.lock");
        this.lock = new InterProcessLock(lockFile);
        this.lockMaxWait = requireNonNull(lockMaxWait, "lockMaxWait is null");
        this.lockAcquireTimeout = requireNonNull(lockAcquireTimeout, "lockAcquireTimeout is null");
    }

    @Override
    public Optional<Token> getToken()
    {
        // Wait while the lock is held, mimicking readLock blocking while writeLock is held
        boolean locked = Failsafe.with(RetryPolicy.<Boolean>builder()
                        .handleResultIf(Boolean.TRUE::equals)
                        .abortOn(UncheckedIOException.class)
                        .withMaxAttempts(-1)
                        .withDelay(100, 1000, MILLIS)
                        .withMaxDuration(lockMaxWait)
                        .build())
                .get(lock::isLocked);
        if (locked) {
            throw new IllegalStateException("Lock file " + lockFile + " for System Cached token is still locked after waiting " + lockMaxWait + ". " +
                    "Another authentication is still in progress.");
        }

        return readTokenFile().map(Token::new);
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

        // If another connection or process is obtaining a token, skip, just like MemoryCachedKnownToken's tryLock
        Optional<String> tokenBeforeLock = readTokenFile();
        if (!lock.tryLock(lockAcquireTimeout)) {
            return;
        }

        try {
            // The token changed while waiting for the lock, so another connection or process already tried to obtain a new one
            if (!readTokenFile().equals(tokenBeforeLock)) {
                return;
            }
            // Clear token before obtaining new one, as it might fail leaving old invalid token.
            Files.deleteIfExists(tokenFile);
            Optional<Token> token = tokenSource.get();
            token.ifPresent(this::writeTokenToFile);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to update token file " + tokenFile, e);
        }
        finally {
            lock.unlock();
        }
    }

    private Optional<String> readTokenFile()
    {
        try {
            String content = Files.readString(tokenFile).trim();
            if (content.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(content);
        }
        catch (NoSuchFileException e) {
            // Another connection or process may delete it at any time
            return Optional.empty();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read token from " + tokenFile, e);
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
