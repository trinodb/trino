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
package io.trino.spi.filesystem;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.spi.filesystem.util.SecurePathWhiteList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.StandardOpenOption.CREATE_NEW;

/**
 * A File-based lock for {@link HetuFileSystemClient}
 * The lock is DESIGNED to be NOT REUSABLE.
 * Every time a lock is used and unlocked, it should be disposed.
 * If the same client need a lock again it should get another lock instance.
 *
 * @since 2020-03-30
 */
public class FileBasedLock
        implements Lock
{
    private static final Logger LOG = Logger.get(FileBasedLock.class);

    public static final String LOCK_FILE_NAME = ".lockFile";
    public static final String LOCK_INFO_NAME = ".lockInfo";
    public static final String LOCK_TIMEOUT_CONFIG = "lock.timeout";
    public static final String LOCK_DIR_CONFIG = "lock.dir";
    public static final String LOCK_RETRY_INTERVAL_CONFIG = "lock.retry.millis";
    public static final String LOCK_REFRESH_RATE_CONFIG = "lock.refresh.rate";

    public static final long DEFAULT_LOCK_FILE_TIMEOUT = 5000L;
    public static final long DEFAULT_RETRY_INTERVAL = 1000L;
    public static final long DEFAULT_REFRESH_RATE = 2L;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private HetuFileSystemClient fs;
    private Path lockFilePath;
    private Path lockInfoPath;
    private long lockFileTimeout;
    private long retryInterval;
    private long refreshRate;
    private ScheduledFuture<?> heartBeat;
    private UUID uuid;
    private boolean lockAlreadyUsed;

    /**
     * Utility function for the factory to create locks.
     * This is a temporary solution and will be replace with a lock factory or similar infrastructures in the future.
     *
     * @param fs A filesystem the lock acts on
     * @param lockProperties A Properties objects holding lock configs, in which a lockDir is a must-have.
     * @return A lock object on the given filesystem client.
     */
    public static FileBasedLock getLock(HetuFileSystemClient fs, Properties lockProperties)
            throws IOException
    {
        String lockDir = checkProperty(lockProperties, LOCK_DIR_CONFIG);
        String timeoutRead = lockProperties.getProperty(LOCK_TIMEOUT_CONFIG);
        String retryIntervalRead = lockProperties.getProperty(LOCK_RETRY_INTERVAL_CONFIG);
        String refreshRateRead = lockProperties.getProperty(LOCK_REFRESH_RATE_CONFIG);

        try {
            checkArgument(!lockDir.contains("../"),
                    "Lock directory path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(lockDir),
                    "Lock directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }
        Path lockFileDir = Paths.get(lockDir);
        long timeout = (timeoutRead == null) ? DEFAULT_LOCK_FILE_TIMEOUT : Long.parseLong(timeoutRead);
        long retryInterval1 = (retryIntervalRead == null) ? DEFAULT_RETRY_INTERVAL : Long.parseLong(retryIntervalRead);
        long refreshRate1 = (refreshRateRead == null) ? DEFAULT_REFRESH_RATE : Long.parseLong(refreshRateRead);

        return new FileBasedLock(fs, lockFileDir, timeout, retryInterval1, refreshRate1);
    }

    /**
     * Simpler constructor of the lock with default configs
     *
     * @param fs A filesystem the lock acts on
     * @param lockFileDir A directory where the lock files are placed.
     * This is usually the directory which the client tries to modify.
     */
    public FileBasedLock(HetuFileSystemClient fs, Path lockFileDir)
            throws IOException
    {
        this(fs, lockFileDir, DEFAULT_LOCK_FILE_TIMEOUT, DEFAULT_RETRY_INTERVAL, DEFAULT_REFRESH_RATE);
    }

    /**
     * Full constructor of the lock with user-defined lock properties
     * Please ensure that the lockFileTimeout is set to be the same
     * across ALL lock instances based on the same directory of the same file system.
     * Otherwise the lock may function in an unexpected way.
     *
     * @param fs A filesystem the lock acts on
     * @param lockFileDir A directory where the lock files are placed
     * @param lockFileTimeout Timeout for the lock to expire.
     */
    public FileBasedLock(HetuFileSystemClient fs,
                         Path lockFileDir,
                         long lockFileTimeout,
                         long retryInterval,
                         long refreshRate)
            throws IOException
    {
        try {
            checkArgument(!lockFileDir.toString().contains("../"),
                    "Lock directory path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(lockFileDir.toString()),
                    "Lock directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }
        fs.createDirectories(lockFileDir);
        this.fs = fs;
        this.uuid = UUID.randomUUID();
        this.lockFilePath = lockFileDir.resolve(LOCK_FILE_NAME);
        this.lockInfoPath = lockFileDir.resolve(LOCK_INFO_NAME);
        this.lockFileTimeout = lockFileTimeout;
        this.retryInterval = retryInterval;
        this.refreshRate = refreshRate;
        this.lockAlreadyUsed = false;
    }

    public static boolean isLockUtilFile(Path path)
    {
        return LOCK_FILE_NAME.equals(path.getFileName().toString())
                || LOCK_INFO_NAME.equals(path.getFileName().toString());
    }

    /**
     * Keeping trying to Acquire the lock for exclusive access to the filesystem.
     * If the lock is not acquired it keeps trying forever.
     * To limit the trials use {@link FileBasedLock#tryLock(long, TimeUnit)} instead.
     */
    @Override
    public void lock()
    {
        // A lock instance is not reusable as it will have same uuid and executor service
        // This will cause problems sometimes
        if (lockAlreadyUsed) {
            throw new IllegalStateException("This lock has already been used. Please get another lock instance.");
        }

        while (isLocked() || !acquiredLock()) {
            try {
                LOG.info("Lock Waiting for file lock ...");
                Thread.sleep(retryInterval);
            }
            catch (InterruptedException e) {
                throw new UncheckedExecutionException("waiting for lock file to be released was interrupted", e);
            }
        }

        // Successfully acquired the lock, keep updating lockFile to declare possession
        try {
            addHeartBeatScheduler();
            lockAlreadyUsed = true;
        }
        catch (RejectedExecutionException e) {
            LOG.debug("Failed to add lock heart beat. Abort.");
        }
    }

    @Override
    public void lockInterruptibly()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Try ONLY ONCE to acquire the lock for exclusive access to the filesystem.
     * If the lock is not acquired return {@code false}.
     * For more trials use {@link FileBasedLock#tryLock(long, TimeUnit)} instead.
     */
    @Override
    public boolean tryLock()
    {
        // A lock instance is not reusable as it will have same uuid and executor service
        // This will cause problems sometimes
        if (lockAlreadyUsed) {
            throw new IllegalStateException("This lock has already been used. Please get another lock instance.");
        }

        if (isLocked() || !acquiredLock()) {
            return false;
        }

        // Successfully acquired the lock, keep updating lockFile to declare possession
        try {
            addHeartBeatScheduler();
            lockAlreadyUsed = true;
        }
        catch (RejectedExecutionException e) {
            LOG.debug("Failed to add lock heart beat. Abort.");
            return false;
        }
        return true;
    }

    /**
     * Keep trying for given period of time to acquire the lock for exclusive access to the filesystem.
     * If the lock is not acquired during this time return {@code false}.
     *
     * @param time Length of time to keep trying.
     * @param unit Unit of the length.
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit)
            throws InterruptedException
    {
        // A lock instance is not reusable as it will have same uuid and executor service
        // This will cause problems sometimes
        if (lockAlreadyUsed) {
            throw new IllegalStateException("This lock has already been used. Please get another lock instance.");
        }

        long tryPeriod = unit.toMillis(time);
        long startTime = System.currentTimeMillis();
        while (isLocked() || !acquiredLock()) {
            if (System.currentTimeMillis() - startTime > tryPeriod) {
                LOG.debug("Trial time limit reached. Unable to get the lock.");
                return false;
            }
            LOG.info("Waiting for file lock ...");
            Thread.sleep(retryInterval);
        }

        // Successfully acquired the lock, keep updating lockFile to declare possession
        try {
            addHeartBeatScheduler();
            lockAlreadyUsed = true;
        }
        catch (RejectedExecutionException e) {
            LOG.debug("Failed to add lock heart beat. Abort.");
            return false;
        }
        return true;
    }

    /**
     * Release the lock for others to acquire.
     */
    @Override
    public void unlock()
    {
        if (heartBeat != null) {
            heartBeat.cancel(true);
            heartBeat = null;
            scheduler.shutdown();
        }
        Set<Path> failedDeletions = new HashSet<>(1);
        failedDeletions.add(lockFilePath);
        failedDeletions.add(lockInfoPath);
        try {
            // Wait for the last write into lockFile to finish
            Thread.sleep(50);

            if (!fs.exists(lockFilePath) || fs.deleteIfExists(lockFilePath)) {
                failedDeletions.remove(lockFilePath);
            }
            if (!fs.exists(lockInfoPath) || fs.deleteIfExists(lockInfoPath)) {
                failedDeletions.remove(lockInfoPath);
            }
            if (!failedDeletions.isEmpty()) {
                throw new IllegalStateException("File lock failed to delete, manual deletion is required: "
                        + failedDeletions.toString());
            }
        }
        catch (IOException e) {
            if (!failedDeletions.isEmpty()) {
                throw new IllegalStateException("File lock failed to delete, manual deletion is required: "
                        + failedDeletions.toString(), e);
            }
            LOG.warn("{} {}", "Exception thrown during lock release, but lock files are deleted successfully.",
                    "No manual deletion is required.");
            LOG.debug("Exception thrown during lock.release(): %s", e.getMessage());
        }
        catch (InterruptedException e) {
            LOG.error("Load pdbo table error : ", e.getMessage());
        }
    }

    @Override
    public Condition newCondition()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isLocked()
    {
        try {
            if (fs.exists(lockFilePath)) {
                if (isLockExpired(lockFilePath)) {
                    fs.delete(lockFilePath);
                    LOG.debug("lockFile expired. Deleted.");
                }
                else {
                    return true;
                }
            }
        }
        // Already deleted, others are trying to lock, return false
        catch (NoSuchFileException | FileNotFoundException e) {
            // When trying to delete expired lock file it may already been deleted by another client.
            LOG.debug("Failed to delete lockFile: not found. Deleted by others?");
            return true;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error accessing lockFile during isLocked():", e);
        }
        return false;
    }

    public boolean acquiredLock()
    {
        try {
            if (fs.exists(lockInfoPath)) {
                if (isLockExpired(lockInfoPath)) {
                    try {
                        fs.delete(lockInfoPath);
                        LOG.debug("lockInfo expired. Deleted.");
                    }
                    catch (NoSuchFileException | FileNotFoundException e) {
                        // When trying to delete expired lock file it may already been deleted by another client.
                        LOG.debug("Failed to delete lockFile: not found. Deleted by others?");
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Error accessing lockFile during isLocked():", e);
                    }
                }
                else {
                    try (InputStream is = fs.newInputStream(lockInfoPath);
                            InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                        int idLength = uuid.toString().length();
                        char[] firstIdChars = new char[idLength];
                        return reader.read(firstIdChars, 0, idLength) > 0
                                && uuid.toString().equals(new String(firstIdChars));
                    }
                }
            }
            writeToFile(lockInfoPath, uuid.toString(), false);
            return true;
        }
        catch (IllegalArgumentException e) {
            LOG.debug("Error writing to lockInfo. Abort.");
            return false;
        }
        // Overwrite failure
        catch (FileAlreadyExistsException e) {
            LOG.debug("Error writing to lockInfo: file exists. Lock acquired by others?");
            return false;
        }
        catch (NoSuchFileException | FileNotFoundException e) {
            LOG.debug("Error accessing from lockInfo: no such file.");
            return false;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unchecked exception during accessing lockInfo:", e);
        }
    }

    private boolean isLockExpired(Path lockPath)
    {
        try {
            // get current filesystem time
            Path tmp = Paths.get(String.format("%s.%s.tmp", this.lockFilePath, this.uuid));
            writeToFile(tmp, "checkTime", true);
            long cur = Long.parseLong(fs.getAttribute(tmp, "lastModifiedTime").toString());
            fs.delete(tmp);

            long lockTime = Long.parseLong(fs.getAttribute(lockPath, "lastModifiedTime").toString());
            if (cur - lockTime >= lockFileTimeout) {
                return true;
            }
        }
        catch (IllegalArgumentException e) {
            LOG.debug("Writing to tmp file for expiry check failed. Abort.");
        }
        catch (FileNotFoundException | NoSuchFileException e) {
            LOG.debug("File not found. Deleted by others?");
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unchecked exception during expiry checking:", e);
        }
        return false;
    }

    private void addHeartBeatScheduler()
    {
        Runnable heartBeatTask = (() -> {
            try {
                writeToFile(lockFilePath, "update", true);
            }
            // hdfs write operation might throw IllegalArgumentException (self-suppress not permitted)
            catch (IllegalArgumentException e) {
                LOG.debug("Error updating the lockFile, will retry.");
            }
            catch (FileNotFoundException | NoSuchFileException e) {
                LOG.debug("File not found. Deleted by others?");
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unchecked exception updating the lock file: " + lockFilePath, e);
            }
        });
        heartBeatTask.run();
        // then, schedule it with scheduler
        heartBeat = scheduler.scheduleAtFixedRate(heartBeatTask, 0,
                lockFileTimeout / refreshRate, TimeUnit.MILLISECONDS);
    }

    private void writeToFile(Path file, String content, boolean overwrite)
            throws IOException
    {
        try (OutputStream os = (overwrite) ? fs.newOutputStream(file) : fs.newOutputStream(file, CREATE_NEW)) {
            os.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String checkProperty(Properties properties, String key)
    {
        String val = properties.getProperty(key);
        if (val == null) {
            throw new IllegalArgumentException(String.format("Configuration entry '%s' must be specified", key));
        }
        return val;
    }
}
