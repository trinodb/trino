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

import com.google.common.io.Closer;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

/**
 * This KnownToken instance persists the token to ~/.trino/.token on the filesystem,
 * allowing it to be reused across separate CLI invocations.
 * A lock file (~/.trino/.token.lck) is used to coordinate token acquisition
 * across processes — its atomic creation acts as a cross-process tryLock.
 * A lock file older than the time allowed for authentication is considered abandoned,
 * for example by a process killed during authentication, and is replaced.
 * The implementation is similar to MemoryCachedKnownToken, but with LOCK_FILE acting as a Lock
 */
class SystemCachedKnownToken
        implements KnownToken
{
    private static final Path DEFAULT_TRINO_DIR = Path.of(System.getProperty("user.home"), ".trino");
    // duration for the user to authenticate within IDP. It involves clicking and typing from a real person within a browser, so should be counted in minutes.
    private static final Duration DEFAULT_LOCK_MAX_WAIT = Duration.ofMinutes(10);

    public static final SystemCachedKnownToken INSTANCE = new SystemCachedKnownToken(DEFAULT_TRINO_DIR);

    private final Path trinoDir;
    private final Path tokenFile;
    private final Path lockFile;
    private final Duration lockMaxWait;

    SystemCachedKnownToken(Path trinoDir)
    {
        this(trinoDir, DEFAULT_LOCK_MAX_WAIT);
    }

    SystemCachedKnownToken(Path trinoDir, Duration lockMaxWait)
    {
        this.trinoDir = requireNonNull(trinoDir, "trinoDir is null");
        this.tokenFile = trinoDir.resolve(".token");
        this.lockFile = trinoDir.resolve(".token.lck");
        this.lockMaxWait = requireNonNull(lockMaxWait, "lockMaxWait is null");
    }

    @Override
    public Optional<Token> getToken()
    {
        // Wait while lock file exists and is not abandoned, mimicking readLock blocking while writeLock is held
        boolean lockFileExists = Failsafe.with(RetryPolicy.<Boolean>builder()
                        .handleResultIf(Boolean.TRUE::equals)
                        .withMaxAttempts(-1)
                        .withDelay(100, 1000, MILLIS)
                        .withMaxDuration(lockMaxWait)
                        .build())
                .get(this::isLockFileInEffect);
        if (lockFileExists) {
            throw new IllegalStateException("Lock file " + lockFile + " for System Cached token still exists after waiting " + lockMaxWait + ". " +
                    "It may be created by another concurrent authentication, which is still in progress.");
        }

        try {
            String content = Files.readString(tokenFile).trim();
            if (content.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new Token(content));
        }
        catch (NoSuchFileException e) {
            // also when a concurrent setupToken deletes the token file after the lock file check
            return Optional.empty();
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
            if (isLockFileAbandoned()) {
                Files.deleteIfExists(lockFile);
            }
            Files.createFile(lockFile);
        }
        catch (FileAlreadyExistsException e) {
            return;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create lock file " + lockFile, e);
        }

        try (Closer closer = Closer.create()) {
            closer.register(() -> Files.deleteIfExists(lockFile));
            try {
                // Clear token before obtaining new one, as it might fail leaving old invalid token.
                Files.deleteIfExists(tokenFile);
                Optional<Token> token = tokenSource.get();
                token.ifPresent(this::writeTokenToFile);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to update token file " + tokenFile, e);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete lock file " + lockFile, e);
        }
    }

    private boolean isLockFileInEffect()
    {
        return Files.exists(lockFile) && !isLockFileAbandoned();
    }

    private boolean isLockFileAbandoned()
    {
        try {
            Instant lockTime = Files.getLastModifiedTime(lockFile).toInstant();
            return lockTime.isBefore(Instant.now().minus(lockMaxWait));
        }
        catch (NoSuchFileException e) {
            return false;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read modification time of lock file " + lockFile, e);
        }
    }

    private void writeTokenToFile(Token token)
    {
        // Write to a temporary file, created readable only by the owner, then rename it,
        // so that a concurrent reader never sees a partially written token
        try (Closer closer = Closer.create()) {
            Path temporaryFile = Files.createTempFile(trinoDir, ".token", ".tmp", ownerOnlyAttributes());
            // no-op once the file is renamed
            closer.register(() -> Files.deleteIfExists(temporaryFile));
            Files.writeString(temporaryFile, token.token());
            Files.move(temporaryFile, tokenFile, ATOMIC_MOVE, REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write token to " + tokenFile, e);
        }
    }

    private FileAttribute<?>[] ownerOnlyAttributes()
    {
        // POSIX permissions are not supported on Windows, where the file inherits the user-private ACL of the home directory
        if (!trinoDir.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            return new FileAttribute<?>[0];
        }
        return new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))};
    }
}
