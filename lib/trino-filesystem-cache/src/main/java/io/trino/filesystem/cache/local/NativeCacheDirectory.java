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
package io.trino.filesystem.cache.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.cache.local.NativeCacheDirectoryWalker.DeletionStats;
import io.trino.filesystem.cache.local.NativeCacheDirectoryWalker.FileGroup;
import io.trino.filesystem.cache.local.NativeCacheDirectoryWalker.ScanResult;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.cache.local.CacheFileLayout.DATA_DIRECTORY;
import static io.trino.filesystem.cache.local.CacheFileLayout.VERSION_DIRECTORY;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

final class NativeCacheDirectory
{
    private static final int EVICTION_SAMPLE_SIZE = 8192;
    private static final double LOW_WATERMARK_FACTOR = 0.9;

    private final Path root;
    private final CacheFileLayout layout;
    private final NativeCacheDirectoryWalker walker;
    private final NativeFileSystemCacheStats stats;
    private final long maxBytes;
    private final long lowWatermarkBytes;
    private final long maxFiles;
    private final long lowWatermarkFiles;
    private final int evictionSampleSize;
    private final LongSupplier usableSpaceSupplier;
    private final BloomFilterAccessTracker accessTracker;
    private final AtomicLong currentBytes = new AtomicLong();
    private final AtomicLong currentFiles = new AtomicLong();
    private final AtomicBoolean initializationStarted = new AtomicBoolean();
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final Object initializationLock = new Object();
    // Pages written by this process during the initial scan are accounted separately, so the scan does not double count them.
    private final Set<Path> initializationWrites = ConcurrentHashMap.newKeySet();
    private final Set<Path> activeTemporaryFiles = ConcurrentHashMap.newKeySet();
    @GuardedBy("initializationLock")
    private Path initializationScanCursor;
    @GuardedBy("this")
    private Path evictionCursor;

    NativeCacheDirectory(Path root, long maxBytes, long maxFiles, Optional<Duration> ttl, BloomFilterAccessTracker accessTracker, NativeFileSystemCacheStats stats)
            throws IOException
    {
        this(root, maxBytes, maxFiles, ttl, accessTracker, Clock.systemUTC(), stats, EVICTION_SAMPLE_SIZE, () -> root.toAbsolutePath().normalize().toFile().getUsableSpace());
    }

    @VisibleForTesting
    NativeCacheDirectory(Path root, long maxBytes, long maxFiles, Optional<Duration> ttl, BloomFilterAccessTracker accessTracker, Clock clock, NativeFileSystemCacheStats stats, int evictionSampleSize, LongSupplier usableSpaceSupplier)
            throws IOException
    {
        checkArgument(evictionSampleSize > 0, "evictionSampleSize must be positive");
        this.root = requireNonNull(root, "root is null").toAbsolutePath().normalize();
        this.layout = new CacheFileLayout(this.root);
        this.stats = requireNonNull(stats, "stats is null");
        this.maxBytes = maxBytes;
        this.lowWatermarkBytes = (long) (maxBytes * LOW_WATERMARK_FACTOR);
        this.maxFiles = maxFiles;
        this.lowWatermarkFiles = (long) (maxFiles * LOW_WATERMARK_FACTOR);
        this.evictionSampleSize = evictionSampleSize;
        this.usableSpaceSupplier = requireNonNull(usableSpaceSupplier, "usableSpaceSupplier is null");
        Optional<Long> ttlMillis = requireNonNull(ttl, "ttl is null")
                .map(duration -> duration.roundTo(TimeUnit.MILLISECONDS));
        this.accessTracker = requireNonNull(accessTracker, "accessTracker is null");
        Clock cacheClock = requireNonNull(clock, "clock is null");
        Path dataRoot = this.root.resolve(VERSION_DIRECTORY).resolve(DATA_DIRECTORY);
        Files.createDirectories(dataRoot);
        verifyAtomicMove(dataRoot);
        this.walker = new NativeCacheDirectoryWalker(dataRoot, ttlMillis, cacheClock, this.accessTracker, this.stats, initializationWrites, activeTemporaryFiles);
    }

    CacheFileLayout.CacheFile cacheFile(Location location, String cacheKey)
    {
        return layout.cacheFile(location, cacheKey);
    }

    boolean readPage(CacheFileLayout.CacheFile cacheFile, long pageIndex, int pageOffset, int length, int expectedPageLength, byte[] buffer, int bufferOffset)
            throws IOException
    {
        Path pagePath = cacheFile.pagePath(pageIndex);
        try {
            if (Files.size(pagePath) != expectedPageLength) {
                deleteCachedPage(pagePath);
                return false;
            }
            try (InputStream input = Files.newInputStream(pagePath)) {
                input.skipNBytes(pageOffset);
                if (input.readNBytes(buffer, bufferOffset, length) != length) {
                    deleteCachedPage(pagePath);
                    return false;
                }
            }
            stats.recordCacheRead(length);
            accessTracker.record(cacheFile.fileHash());
            return true;
        }
        catch (NoSuchFileException | EOFException _) {
            return false;
        }
    }

    void writePage(CacheFileLayout.CacheFile cacheFile, long pageIndex, byte[] page, int offset, int length)
    {
        Path pagePath = cacheFile.pagePath(pageIndex);
        Path temporaryPath = cacheFile.temporaryPagePath(pageIndex, randomUUID().toString());
        boolean initializationWrite = false;
        boolean pageWritten = false;
        try {
            if (Files.exists(pagePath)) {
                stats.recordCacheWriteSkip();
                return;
            }
            initializationWrite = beginInitializationWrite(pagePath);
            if (!reserve(length) || !hasUsableSpace(length)) {
                stats.recordCacheWriteSkip();
                return;
            }
            Files.createDirectories(pagePath.getParent());
            activeTemporaryFiles.add(temporaryPath);
            try (OutputStream output = Files.newOutputStream(temporaryPath, CREATE_NEW, WRITE)) {
                output.write(page, offset, length);
            }
            Files.move(temporaryPath, pagePath, ATOMIC_MOVE);
            pageWritten = true;
            recordCachedPage(length);
            stats.recordCacheWrite();
        }
        catch (FileAlreadyExistsException _) {
            deleteIfExists(temporaryPath);
        }
        catch (IOException | RuntimeException e) {
            stats.recordCacheWriteFailure();
            deleteIfExists(temporaryPath);
        }
        finally {
            activeTemporaryFiles.remove(temporaryPath);
            if (initializationWrite && !pageWritten) {
                initializationWrites.remove(pagePath);
            }
        }
    }

    void expire(Location location)
            throws IOException
    {
        deleteRecursively(layout.locationPath(location), false);
    }

    void expire(Collection<Location> locations)
            throws IOException
    {
        for (Location location : locations) {
            expire(location);
        }
    }

    void maintenance()
    {
        if (!initialized.get()) {
            initialize();
            return;
        }
        walker.cleanTemporaryFiles(evictionSampleSize);
        for (FileGroup fileGroup : sampleFileGroups()) {
            if (!fileGroup.expired()) {
                continue;
            }
            try {
                deleteRecursively(fileGroup.path(), true);
            }
            catch (IOException _) {
            }
        }
    }

    void initialize()
    {
        if (!initializationStarted.compareAndSet(false, true)) {
            return;
        }

        try {
            ScanResult scanResult = scan();
            synchronized (initializationLock) {
                setCurrentUsage(new ScanResult(
                        scanResult.files() + currentFiles.get(),
                        scanResult.bytes() + currentBytes.get()));
                initialized.set(true);
                initializationWrites.clear();
                initializationScanCursor = null;
            }
        }
        catch (RuntimeException e) {
            initializationStarted.set(false);
            throw e;
        }
    }

    private static void deleteIfExists(Path path)
    {
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException _) {
        }
    }

    private static void verifyAtomicMove(Path directory)
            throws IOException
    {
        Path source = Files.createTempFile(directory, ".atomic-move-check-", ".tmp");
        Path target = directory.resolve(source.getFileName() + ".moved");
        try {
            Files.move(source, target, ATOMIC_MOVE);
        }
        finally {
            deleteIfExists(source);
            deleteIfExists(target);
        }
    }

    private boolean reserve(long bytes)
    {
        if (bytes > maxBytes) {
            return false;
        }
        if (!initialized.get()) {
            return true;
        }
        if (fits(bytes, 1)) {
            return true;
        }
        evictToFit(bytes, 1);
        return fits(bytes, 1);
    }

    private boolean fits(long bytes, long files)
    {
        return currentBytes.get() + bytes <= maxBytes && currentFiles.get() + files <= maxFiles;
    }

    @VisibleForTesting
    boolean isInitialized()
    {
        return initialized.get();
    }

    private boolean hasUsableSpace(long bytes)
    {
        return usableSpaceSupplier.getAsLong() >= bytes;
    }

    private void evictToFit(long bytes, long files)
    {
        while (!belowLowWatermark(bytes, files)) {
            List<FileGroup> candidates = new ArrayList<>(sampleFileGroups());
            if (candidates.isEmpty()) {
                reconcileAccounting();
                return;
            }
            candidates.sort(comparing(FileGroup::expired).reversed()
                    .thenComparing(FileGroup::accessTimeForEviction));
            boolean deleted = false;
            for (FileGroup candidate : candidates) {
                DeletionStats deletionStats;
                try {
                    deletionStats = deleteRecursively(candidate.path(), candidate.expired());
                }
                catch (IOException _) {
                    continue;
                }
                if (deletionStats.files() > 0) {
                    stats.recordEviction(deletionStats.files(), deletionStats.bytes());
                    deleted = true;
                    if (belowLowWatermark(bytes, files)) {
                        return;
                    }
                }
            }
            if (!deleted) {
                reconcileAccounting();
                return;
            }
        }
    }

    private boolean belowLowWatermark(long bytes, long files)
    {
        return currentBytes.get() + bytes <= lowWatermarkBytes && currentFiles.get() + files <= lowWatermarkFiles;
    }

    private void reconcileAccounting()
    {
        if (initialized.get()) {
            setCurrentUsage(scan());
        }
    }

    private void setCurrentUsage(ScanResult scanResult)
    {
        long previousBytes = currentBytes.getAndSet(scanResult.bytes());
        long previousFiles = currentFiles.getAndSet(scanResult.files());
        stats.adjustCachedFiles(scanResult.files() - previousFiles, scanResult.bytes() - previousBytes);
    }

    private ScanResult scan()
    {
        return walker.scan(this::markInitializationScanned);
    }

    private synchronized List<FileGroup> sampleFileGroups()
    {
        List<FileGroup> sample = collectFileGroupsAfterCursor();
        if (sample.size() < evictionSampleSize) {
            List<FileGroup> wrappedSample = new ArrayList<>(sample);
            wrappedSample.addAll(collectFileGroupsFromBeginning(evictionSampleSize - sample.size()));
            sample = List.copyOf(wrappedSample);
        }
        if (!sample.isEmpty()) {
            evictionCursor = sample.getLast().path();
        }
        return sample;
    }

    @GuardedBy("this")
    private List<FileGroup> collectFileGroupsAfterCursor()
    {
        return walker.collectFileGroups(evictionCursor, true, evictionSampleSize);
    }

    @GuardedBy("this")
    private List<FileGroup> collectFileGroupsFromBeginning(int limit)
    {
        if (evictionCursor == null) {
            return List.of();
        }
        return walker.collectFileGroups(evictionCursor, false, limit);
    }

    private boolean beginInitializationWrite(Path pagePath)
    {
        Path fileGroupPath = requireNonNull(pagePath.getParent(), "pagePath parent is null");
        synchronized (initializationLock) {
            if (initialized.get()) {
                return false;
            }
            if (initializationScanCursor != null && fileGroupPath.compareTo(initializationScanCursor) <= 0) {
                return false;
            }
            initializationWrites.add(pagePath);
            return true;
        }
    }

    private void markInitializationScanned(Path fileGroupPath)
    {
        synchronized (initializationLock) {
            if (initialized.get()) {
                return;
            }
            if (initializationScanCursor == null || fileGroupPath.compareTo(initializationScanCursor) > 0) {
                initializationScanCursor = fileGroupPath;
            }
            Path scannedThrough = initializationScanCursor;
            initializationWrites.removeIf(pagePath -> {
                Path pageFileGroupPath = pagePath.getParent();
                return pageFileGroupPath != null && pageFileGroupPath.compareTo(scannedThrough) <= 0;
            });
        }
    }

    private void recordCachedPage(long bytes)
    {
        currentBytes.addAndGet(bytes);
        currentFiles.incrementAndGet();
        stats.addCachedFile(bytes);
    }

    private void deleteCachedPage(Path path)
    {
        try {
            if (!Files.exists(path)) {
                return;
            }
            long bytes = Files.size(path);
            if (Files.deleteIfExists(path)) {
                if (initialized.get()) {
                    currentBytes.addAndGet(-bytes);
                    currentFiles.decrementAndGet();
                    stats.removeCachedFiles(1, bytes);
                }
            }
        }
        catch (IOException _) {
        }
    }

    private DeletionStats deleteRecursively(Path path, boolean expired)
            throws IOException
    {
        DeletionStats deletionStats = walker.deleteRecursively(path);
        if (deletionStats.files() > 0) {
            if (initialized.get()) {
                currentBytes.addAndGet(-deletionStats.bytes());
                currentFiles.addAndGet(-deletionStats.files());
                stats.removeCachedFiles(deletionStats.files(), deletionStats.bytes());
            }
            if (expired) {
                for (long index = 0; index < deletionStats.files(); index++) {
                    stats.recordExpiredFile();
                }
            }
        }
        return deletionStats;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("root", root)
                .add("maxBytes", maxBytes)
                .add("maxFiles", maxFiles)
                .toString();
    }
}
