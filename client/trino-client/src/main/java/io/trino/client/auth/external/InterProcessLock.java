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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

/**
 * Exclusive lock on a file, held either by a thread of this process or by another process.
 * The operating system releases the lock when the process holding it exits.
 * <p>
 * On Unix, closing any descriptor of a file releases all locks the process holds on it, so the
 * channel to the file is kept open. The JDK still closes it when the instance is garbage collected,
 * so all threads of a process should use one instance for the lifetime of the process.
 */
@ThreadSafe
final class InterProcessLock
{
    private final Path file;
    @GuardedBy("this")
    private FileChannel channel;
    @GuardedBy("this")
    private Object channelFileKey;
    @GuardedBy("this")
    private FileLock lock;

    InterProcessLock(Path file)
    {
        this.file = requireNonNull(file, "file is null");
    }

    public synchronized boolean tryLock()
    {
        if (lock != null) {
            return false;
        }
        lock = tryLockChannel(channel(CREATE, READ, WRITE), false);
        return lock != null;
    }

    public boolean tryLock(Duration timeout)
    {
        return Failsafe.with(RetryPolicy.<Boolean>builder()
                        .handleResultIf(Boolean.FALSE::equals)
                        .abortOn(UncheckedIOException.class)
                        .withMaxAttempts(-1)
                        .withDelay(10, 100, MILLIS)
                        .withMaxDuration(timeout)
                        .build())
                .get(() -> tryLock());
    }

    public synchronized void unlock()
    {
        checkState(lock != null, "Lock on %s is not held", file);
        try {
            release(lock);
        }
        finally {
            lock = null;
        }
    }

    public synchronized boolean isLocked()
    {
        if (lock != null) {
            return true;
        }
        FileLock probe;
        try {
            // A shared lock, so that checks in different processes do not conflict with each other
            probe = tryLockChannel(channel(READ, WRITE), true);
        }
        catch (UncheckedIOException e) {
            if (e.getCause() instanceof NoSuchFileException) {
                // Nobody can hold a lock on a file that does not exist
                return false;
            }
            throw e;
        }
        if (probe == null) {
            return true;
        }
        release(probe);
        return false;
    }

    @GuardedBy("this")
    private FileChannel channel(OpenOption... options)
    {
        if (channel != null && !Objects.equals(channelFileKey, fileKey())) {
            // The file was deleted or replaced, so other processes lock a different file now.
            // A channel is only needed while this instance does not hold the lock, so closing it releases nothing
            try {
                channel.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to close lock file " + file, e);
            }
            channel = null;
        }
        if (channel == null) {
            try {
                channel = FileChannel.open(file, ImmutableSet.copyOf(options), ownerOnly());
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to open lock file " + file, e);
            }
            channelFileKey = fileKey();
        }
        return channel;
    }

    // Anyone who can read the file can hold a shared lock on it and so block the exclusive one
    private FileAttribute<?>[] ownerOnly()
    {
        if (!file.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            return new FileAttribute<?>[0];
        }
        return new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(ImmutableSet.of(OWNER_READ, OWNER_WRITE))};
    }

    private Object fileKey()
    {
        try {
            return Files.readAttributes(file, BasicFileAttributes.class).fileKey();
        }
        catch (NoSuchFileException e) {
            return null;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read attributes of lock file " + file, e);
        }
    }

    private FileLock tryLockChannel(FileChannel channel, boolean shared)
    {
        try {
            return channel.tryLock(0, Long.MAX_VALUE, shared);
        }
        catch (OverlappingFileLockException e) {
            // Held through another instance in this process
            return null;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to lock file " + file, e);
        }
    }

    private void release(FileLock lock)
    {
        try {
            lock.release();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to release lock on " + file, e);
        }
    }
}
