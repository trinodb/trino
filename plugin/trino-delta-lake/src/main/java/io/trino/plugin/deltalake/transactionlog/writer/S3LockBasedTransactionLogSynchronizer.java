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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.connector.ConnectorSession;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;

/**
 * The S3 lock-based synchronizer is a {@link TransactionLogSynchronizer} for S3-compatible storage that doesn't support conditional writes
 */
public class S3LockBasedTransactionLogSynchronizer
        implements TransactionLogSynchronizer
{
    public static final Logger LOG = Logger.get(S3LockBasedTransactionLogSynchronizer.class);
    private static final Duration EXPIRATION_DURATION = Duration.of(5, MINUTES);
    private static final String LOCK_DIRECTORY = "_sb_lock";
    private static final String LOCK_INFIX = "sb-lock_";
    private static final Pattern LOCK_FILENAME_PATTERN = Pattern.compile("(.*)\\." + LOCK_INFIX + ".*");

    private final TrinoFileSystemFactory fileSystemFactory;
    private final JsonCodec<LockFileContents> lockFileContentsJsonCodec;

    @Inject
    S3LockBasedTransactionLogSynchronizer(TrinoFileSystemFactory fileSystemFactory, JsonCodec<LockFileContents> lockFileContentsJsonCodec)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.lockFileContentsJsonCodec = requireNonNull(lockFileContentsJsonCodec, "lockFileContentsJsonCodec is null");
    }

    @Override
    public boolean isUnsafe()
    {
        return true;
    }

    @Override
    public void write(ConnectorSession session, String clusterId, Location newLogEntryPath, byte[] entryContents)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        Location locksDirectory = newLogEntryPath.sibling(LOCK_DIRECTORY);
        String newEntryFilename = newLogEntryPath.fileName();
        Optional<LockInfo> myLockInfo = Optional.empty();

        try {
            if (fileSystem.newInputFile(newLogEntryPath).exists()) {
                throw new TransactionConflictException("Target file already exists: " + newLogEntryPath);
            }

            List<LockInfo> lockInfos = listLockInfos(fileSystem, locksDirectory);

            Optional<LockInfo> currentLock = Optional.empty();
            for (LockInfo lockInfo : lockInfos) {
                if (lockInfo.getExpirationTime().isBefore(Instant.now())) {
                    deleteLock(fileSystem, locksDirectory, lockInfo);
                }
                else {
                    if (lockInfo.getEntryFilename().equals(newEntryFilename)) {
                        if (currentLock.isPresent()) {
                            throw new TransactionConflictException(format(
                                    "Multiple live locks found for: %s; lock1: %s; lock2: %s",
                                    newLogEntryPath,
                                    currentLock.get().getLockFilename(),
                                    lockInfo.getLockFilename()));
                        }
                        currentLock = Optional.of(lockInfo);
                    }
                }
            }

            currentLock.ifPresent(lock -> {
                throw new TransactionConflictException(format(
                        "Transaction log locked(1); lockingCluster=%s; lockingQuery=%s; expires=%s",
                        lock.getClusterId(),
                        lock.getOwningQuery(),
                        lock.getExpirationTime()));
            });

            myLockInfo = Optional.of(writeNewLockInfo(fileSystem, locksDirectory, newEntryFilename, clusterId, session.getQueryId()));

            // final check if our lock file is only one
            lockInfos = listLockInfos(fileSystem, locksDirectory);
            String myLockFilename = myLockInfo.get().getLockFilename();
            currentLock = lockInfos.stream()
                    .filter(lockInfo -> lockInfo.getEntryFilename().equals(newEntryFilename))
                    .filter(lockInfo -> !lockInfo.getLockFilename().equals(myLockFilename))
                    .findFirst();

            if (currentLock.isPresent()) {
                throw new TransactionConflictException(format(
                        "Transaction log locked(2); lockingCluster=%s; lockingQuery=%s; expires=%s",
                        currentLock.get().getClusterId(),
                        currentLock.get().getOwningQuery(),
                        currentLock.get().getExpirationTime()));
            }

            // extra check if target file did not appear concurrently; e.g. due to conflict with TL writer which uses different synchronization mechanism (like DB)
            if (fileSystem.newInputFile(newLogEntryPath).exists()) {
                throw new TransactionConflictException("Target file was created during locking: " + newLogEntryPath);
            }

            // write transaction log entry atomically by keeping in mind that S3 does not support creating files exclusively
            fileSystem.newOutputFile(newLogEntryPath).createOrOverwrite(entryContents);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Internal error while writing " + newLogEntryPath, e);
        }
        finally {
            if (myLockInfo.isPresent()) {
                try {
                    deleteLock(fileSystem, locksDirectory, myLockInfo.get());
                }
                catch (IOException e) {
                    // Transaction already committed here; we should not throw.
                    LOG.warn(e, "Could not delete lockfile %s", myLockInfo.get().lockFilename);
                }
            }
        }
    }

    private LockInfo writeNewLockInfo(TrinoFileSystem fileSystem, Location lockDirectory, String logEntryFilename, String clusterId, String queryId)
            throws IOException
    {
        String lockFilename = logEntryFilename + "." + LOCK_INFIX + queryId;
        Instant expiration = Instant.now().plus(EXPIRATION_DURATION);
        LockFileContents contents = new LockFileContents(clusterId, queryId, expiration.toEpochMilli());
        Location lockPath = lockDirectory.appendPath(lockFilename);
        TrinoOutputFile lockFile = fileSystem.newOutputFile(lockPath);
        byte[] contentsBytes = lockFileContentsJsonCodec.toJsonBytes(contents);
        try (OutputStream outputStream = lockFile.create()) {
            outputStream.write(contentsBytes);
        }
        return new LockInfo(lockFilename, contents);
    }

    private static void deleteLock(TrinoFileSystem fileSystem, Location lockDirectoryPath, LockInfo lockInfo)
            throws IOException
    {
        fileSystem.deleteFile(lockDirectoryPath.appendPath(lockInfo.getLockFilename()));
    }

    private List<LockInfo> listLockInfos(TrinoFileSystem fileSystem, Location lockDirectoryPath)
            throws IOException
    {
        FileIterator files = fileSystem.listFiles(lockDirectoryPath);
        ImmutableList.Builder<LockInfo> lockInfos = ImmutableList.builder();

        while (files.hasNext()) {
            FileEntry entry = files.next();
            String name = entry.location().fileName();
            if (LOCK_FILENAME_PATTERN.matcher(name).matches()) {
                TrinoInputFile file = fileSystem.newInputFile(entry.location());
                parseLockFile(file, name).ifPresent(lockInfos::add);
            }
        }

        return lockInfos.build();
    }

    private Optional<LockInfo> parseLockFile(TrinoInputFile file, String name)
            throws IOException
    {
        byte[] bytes = null;
        try (InputStream inputStream = file.newStream()) {
            bytes = inputStream.readAllBytes();
            LockFileContents lockFileContents = lockFileContentsJsonCodec.fromJson(bytes);
            return Optional.of(new LockInfo(name, lockFileContents));
        }
        catch (IllegalArgumentException e) {
            String content = null;
            if (bytes != null) {
                content = Base64.getEncoder().encodeToString(bytes);
            }
            LOG.warn(e, "Could not parse lock file: %s; contents=%s", file.location(), content);
            return Optional.empty();
        }
        catch (FileNotFoundException e) {
            return Optional.empty();
        }
    }

    public static String parseEntryFilename(String lockFilename)
    {
        Matcher matcher = LOCK_FILENAME_PATTERN.matcher(lockFilename);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Lock filename " + lockFilename + " does not match expected pattern");
        }
        return matcher.group(1);
    }

    private static class LockInfo
    {
        private final String lockFilename;
        private final String entryFilename;
        private final LockFileContents contents;

        public LockInfo(String lockFilename, LockFileContents contents)
        {
            this.lockFilename = requireNonNull(lockFilename, "lockFilename is null");
            this.entryFilename = parseEntryFilename(lockFilename);
            this.contents = requireNonNull(contents, "contents is null");
        }

        public String getLockFilename()
        {
            return lockFilename;
        }

        public String getEntryFilename()
        {
            return entryFilename;
        }

        public String getClusterId()
        {
            return contents.clusterId();
        }

        public String getOwningQuery()
        {
            return contents.owningQuery();
        }

        public Instant getExpirationTime()
        {
            return Instant.ofEpochMilli(contents.expirationEpochMillis());
        }
    }

    public record LockFileContents(String clusterId, String owningQuery, long expirationEpochMillis)
    {
        public LockFileContents
        {
            requireNonNull(clusterId, "clusterId is null");
            requireNonNull(owningQuery, "owningQuery is null");
        }
    }
}
