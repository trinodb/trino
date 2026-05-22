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

import io.trino.filesystem.Location;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

final class NativeCacheDirectory
{
    private final Path root;
    private final CacheFileLayout layout;
    private final NativeFileSystemCacheStats stats;

    NativeCacheDirectory(Path root, NativeFileSystemCacheStats stats)
            throws IOException
    {
        this.root = requireNonNull(root, "root is null").toAbsolutePath().normalize();
        this.layout = new CacheFileLayout(this.root);
        this.stats = requireNonNull(stats, "stats is null");
        Path dataRoot = this.root.resolve(CacheFileLayout.VERSION_DIRECTORY).resolve(CacheFileLayout.DATA_DIRECTORY);
        Files.createDirectories(dataRoot);
        verifyAtomicMove(dataRoot);
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
                deleteIfExists(pagePath);
                return false;
            }
            try (InputStream input = Files.newInputStream(pagePath)) {
                input.skipNBytes(pageOffset);
                if (input.readNBytes(buffer, bufferOffset, length) != length) {
                    deleteIfExists(pagePath);
                    return false;
                }
            }
            stats.recordCacheRead(length);
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
        try {
            Files.createDirectories(pagePath.getParent());
            try (OutputStream output = Files.newOutputStream(temporaryPath, CREATE_NEW, WRITE)) {
                output.write(page, offset, length);
            }
            Files.move(temporaryPath, pagePath, ATOMIC_MOVE);
            stats.recordCacheWrite();
        }
        catch (FileAlreadyExistsException _) {
            deleteIfExists(temporaryPath);
        }
        catch (IOException | RuntimeException e) {
            stats.recordCacheWriteFailure();
            deleteIfExists(temporaryPath);
        }
    }

    void expire(Location location)
            throws IOException
    {
        deleteRecursively(layout.locationPath(location));
    }

    void expire(Collection<Location> locations)
            throws IOException
    {
        for (Location location : locations) {
            expire(location);
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

    private static void deleteRecursively(Path path)
            throws IOException
    {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                        throws IOException
                {
                    Files.deleteIfExists(file);
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
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("root", root)
                .toString();
    }
}
