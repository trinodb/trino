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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

final class NativeCacheDirectoryWalker
{
    private static final int FILE_GROUP_DEPTH = 4;
    private static final long STALE_TEMPORARY_FILE_MILLIS = TimeUnit.HOURS.toMillis(1);

    private final Path dataRoot;
    private final NativeFileSystemCacheStats stats;
    private final Optional<Long> ttlMillis;
    private final Clock clock;
    private final BloomFilterAccessTracker accessTracker;
    private final Set<Path> initializationWrites;
    private final Set<Path> activeTemporaryFiles;

    NativeCacheDirectoryWalker(
            Path dataRoot,
            Optional<Long> ttlMillis,
            Clock clock,
            BloomFilterAccessTracker accessTracker,
            NativeFileSystemCacheStats stats,
            Set<Path> initializationWrites,
            Set<Path> activeTemporaryFiles)
    {
        this.dataRoot = requireNonNull(dataRoot, "dataRoot is null");
        this.ttlMillis = requireNonNull(ttlMillis, "ttlMillis is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.accessTracker = requireNonNull(accessTracker, "accessTracker is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.initializationWrites = requireNonNull(initializationWrites, "initializationWrites is null");
        this.activeTemporaryFiles = requireNonNull(activeTemporaryFiles, "activeTemporaryFiles is null");
    }

    ScanResult scan(Consumer<Path> fileGroupScanned)
    {
        if (!Files.exists(dataRoot)) {
            return new ScanResult(0, 0);
        }
        cleanTemporaryFiles(Integer.MAX_VALUE);
        return scanFileGroups(dataRoot, 0, requireNonNull(fileGroupScanned, "fileGroupScanned is null"));
    }

    void cleanTemporaryFiles(int limit)
    {
        if (!Files.exists(dataRoot)) {
            return;
        }
        try (var paths = Files.find(dataRoot, 5, this::isStaleTemporaryFile)) {
            for (Path path : paths.limit(limit).toList()) {
                if (Files.deleteIfExists(path)) {
                    stats.recordStaleTemporaryFile();
                }
            }
        }
        catch (IOException _) {
        }
    }

    List<FileGroup> collectFileGroups(Path cursor, boolean afterCursor, int limit)
    {
        if (!Files.isDirectory(dataRoot) || limit <= 0) {
            return List.of();
        }
        List<FileGroup> fileGroups = new ArrayList<>();
        try {
            collectFileGroups(dataRoot, 0, cursor, afterCursor, limit, fileGroups);
        }
        catch (IOException _) {
        }
        return List.copyOf(fileGroups);
    }

    DeletionStats deleteRecursively(Path path)
            throws IOException
    {
        DeletionStats deletionStats = new DeletionStats();
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                        throws IOException
                {
                    if (Files.deleteIfExists(file)) {
                        if (isCachePage(file)) {
                            deletionStats.addCachedFile(attributes.size());
                        }
                        else if (isTemporary(file)) {
                            stats.recordStaleTemporaryFile();
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exception)
                        throws IOException
                {
                    if (exception instanceof NoSuchFileException) {
                        return FileVisitResult.CONTINUE;
                    }
                    throw exception;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path directory, IOException exception)
                        throws IOException
                {
                    if (exception != null && !(exception instanceof NoSuchFileException)) {
                        throw exception;
                    }
                    Files.deleteIfExists(directory);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (NoSuchFileException _) {
        }
        return deletionStats;
    }

    private ScanResult scanFileGroups(Path directory, int depth, Consumer<Path> fileGroupScanned)
    {
        if (depth == FILE_GROUP_DEPTH) {
            Optional<FileGroup> fileGroup = fileGroup(directory);
            fileGroupScanned.accept(directory);
            if (fileGroup.isEmpty()) {
                return new ScanResult(0, 0);
            }
            FileGroup group = fileGroup.orElseThrow();
            if (group.expired()) {
                try {
                    DeletionStats deletionStats = deleteRecursively(group.path());
                    for (long index = 0; index < deletionStats.files(); index++) {
                        stats.recordExpiredFile();
                    }
                }
                catch (IOException _) {
                }
                return new ScanResult(0, 0);
            }
            return new ScanResult(group.files(), group.bytes());
        }

        long bytes = 0;
        long files = 0;
        try {
            for (Path child : sortedDirectories(directory)) {
                ScanResult scanResult = scanFileGroups(child, depth + 1, fileGroupScanned);
                bytes += scanResult.bytes();
                files += scanResult.files();
            }
        }
        catch (IOException _) {
        }
        return new ScanResult(files, bytes);
    }

    private void collectFileGroups(Path directory, int depth, Path cursor, boolean afterCursor, int limit, List<FileGroup> fileGroups)
            throws IOException
    {
        if (fileGroups.size() >= limit) {
            return;
        }
        if (depth == FILE_GROUP_DEPTH) {
            if (cursor == null || (afterCursor == (directory.compareTo(cursor) > 0))) {
                fileGroup(directory).ifPresent(fileGroups::add);
            }
            return;
        }
        for (Path child : sortedDirectories(directory)) {
            collectFileGroups(child, depth + 1, cursor, afterCursor, limit, fileGroups);
            if (fileGroups.size() >= limit) {
                return;
            }
        }
    }

    private Optional<FileGroup> fileGroup(Path path)
    {
        long bytes = 0;
        long files = 0;
        Instant lastModified = Instant.EPOCH;
        try (var pages = Files.list(path)) {
            for (Path page : pages.toList()) {
                if (!Files.isRegularFile(page) || !isCachePage(page)) {
                    continue;
                }
                if (initializationWrites.contains(page)) {
                    continue;
                }
                files++;
                bytes += Files.size(page);
                Instant modified = Files.getLastModifiedTime(page).toInstant();
                if (modified.isAfter(lastModified)) {
                    lastModified = modified;
                }
            }
        }
        catch (IOException _) {
            return Optional.empty();
        }
        if (files == 0) {
            return Optional.empty();
        }
        String fileHash = path.getFileName().toString();
        return Optional.of(new FileGroup(path, fileHash, files, bytes, lastModified, isExpired(lastModified), accessTracker.lastAccessTime(fileHash)));
    }

    private boolean isExpired(Instant lastModified)
    {
        return ttlMillis
                .map(ttl -> clock.millis() - lastModified.toEpochMilli() > ttl)
                .orElse(false);
    }

    private boolean isStaleTemporaryFile(Path path, BasicFileAttributes attributes)
    {
        return attributes.isRegularFile()
                && isTemporary(path)
                && !activeTemporaryFiles.contains(path)
                && clock.millis() - attributes.lastModifiedTime().toMillis() > STALE_TEMPORARY_FILE_MILLIS;
    }

    private static List<Path> sortedDirectories(Path directory)
            throws IOException
    {
        try (var children = Files.list(directory)) {
            return children
                    .filter(Files::isDirectory)
                    .sorted()
                    .toList();
        }
    }

    private static boolean isCachePage(Path path)
    {
        return path.getFileName().toString().endsWith(".cache");
    }

    private static boolean isTemporary(Path path)
    {
        return path.getFileName().toString().contains(".tmp.");
    }

    record FileGroup(Path path, String fileHash, long files, long bytes, Instant lastModified, boolean expired, OptionalLong lastAccessTime)
    {
        private static final long NO_ACCESS_TIME = Long.MIN_VALUE;

        FileGroup
        {
            requireNonNull(path, "path is null");
            requireNonNull(fileHash, "fileHash is null");
            requireNonNull(lastModified, "lastModified is null");
            requireNonNull(lastAccessTime, "lastAccessTime is null");
        }

        long accessTimeForEviction()
        {
            return lastAccessTime.orElse(NO_ACCESS_TIME);
        }
    }

    record ScanResult(long files, long bytes) {}

    static final class DeletionStats
    {
        private long files;
        private long bytes;

        void addCachedFile(long bytes)
        {
            files++;
            this.bytes += bytes;
        }

        long files()
        {
            return files;
        }

        long bytes()
        {
            return bytes;
        }
    }
}
