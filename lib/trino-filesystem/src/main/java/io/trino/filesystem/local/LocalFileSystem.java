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
package io.trino.filesystem.local;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.filesystem.local.LocalUtils.handleException;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.UUID.randomUUID;

/**
 * A hierarchical file system for testing.
 */
public class LocalFileSystem
        implements TrinoFileSystem
{
    private final Path rootPath;

    public LocalFileSystem(Path rootPath)
    {
        this.rootPath = rootPath;
        checkArgument(Files.isDirectory(rootPath), "root is not a directory: %s", rootPath);
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new LocalInputFile(location, toFilePath(location));
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new LocalInputFile(location, toFilePath(location), length, null);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return new LocalInputFile(location, toFilePath(location), length, lastModified);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return new LocalOutputFile(location, toFilePath(location));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        Path filePath = toFilePath(location);
        try {
            Files.delete(filePath);
        }
        catch (NoSuchFileException _) {
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        Path directoryPath = toDirectoryPath(location);
        if (!Files.exists(directoryPath)) {
            return;
        }
        if (!Files.isDirectory(directoryPath)) {
            throw new IOException("Location is not a directory: " + location);
        }

        try {
            Files.walkFileTree(
                    directoryPath,
                    new SimpleFileVisitor<>()
                    {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                throws IOException
                        {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path directory, IOException exception)
                                throws IOException
                        {
                            if (exception != null) {
                                throw exception;
                            }
                            // do not delete the root of this file system
                            if (!directory.equals(rootPath)) {
                                Files.delete(directory);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        Path sourcePath = toFilePath(source);
        Path targetPath = toFilePath(target);
        try {
            if (!Files.exists(sourcePath)) {
                throw new IOException("Source does not exist: " + source);
            }
            if (!Files.isRegularFile(sourcePath)) {
                throw new IOException("Source is not a file: " + source);
            }

            Files.createDirectories(targetPath.getParent());

            // Do not specify atomic move, as unix overwrites when atomic is enabled
            Files.move(sourcePath, targetPath);
        }
        catch (IOException e) {
            throw new IOException("File rename from %s to %s failed: %s".formatted(source, target, e.getMessage()), e);
        }
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return new LocalFileIterator(location, rootPath, toDirectoryPath(location));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
    {
        return Optional.of(Files.isDirectory(toDirectoryPath(location)));
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        validateLocalLocation(location);
        try {
            Files.createDirectories(toDirectoryPath(location));
        }
        catch (IOException e) {
            throw new IOException("Failed to create directory: " + location, e);
        }
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        Path sourcePath = toDirectoryPath(source);
        Path targetPath = toDirectoryPath(target);
        try {
            if (!Files.exists(sourcePath)) {
                throw new IOException("Source does not exist: " + source);
            }
            if (!Files.isDirectory(sourcePath)) {
                throw new IOException("Source is not a directory: " + source);
            }

            Files.createDirectories(targetPath.getParent());

            // Do not specify atomic move, as unix overwrites when atomic is enabled
            Files.move(sourcePath, targetPath);
        }
        catch (IOException e) {
            throw new IOException("Directory rename from %s to %s failed: %s".formatted(source, target, e.getMessage()), e);
        }
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        Path path = toDirectoryPath(location);
        if (Files.isRegularFile(path)) {
            throw new IOException("Location is a file: " + location);
        }
        if (!Files.isDirectory(path)) {
            return ImmutableSet.of();
        }
        try (Stream<Path> stream = Files.list(path)) {
            return stream
                    .filter(file -> Files.isDirectory(file, NOFOLLOW_LINKS))
                    .map(file -> file.getFileName() + "/")
                    .map(location::appendPath)
                    .collect(toImmutableSet());
        }
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        // allow for absolute or relative temporary prefix
        Location temporary;
        if (temporaryPrefix.startsWith("/")) {
            String prefix = temporaryPrefix;
            while (prefix.startsWith("/")) {
                prefix = prefix.substring(1);
            }
            temporary = Location.of("local:///").appendPath(prefix);
        }
        else {
            temporary = targetPath.appendPath(temporaryPrefix);
        }

        temporary = temporary.appendPath(randomUUID().toString());

        createDirectory(temporary);
        return Optional.of(temporary);
    }

    private Path toFilePath(Location location)
    {
        validateLocalLocation(location);
        location.verifyValidFileLocation();

        Path localPath = toPath(location);

        // local file path can not be empty as this would create a file for the root entry
        checkArgument(!localPath.equals(rootPath), "Local file location must contain a path: %s", localPath);
        return localPath;
    }

    private Path toDirectoryPath(Location location)
    {
        validateLocalLocation(location);
        return toPath(location);
    }

    private static void validateLocalLocation(Location location)
    {
        checkArgument(location.scheme().equals(Optional.of("local")) || location.scheme().equals(Optional.of("file")), "Only 'local' and 'file' scheme is supported: %s", location);
        checkArgument(location.userInfo().isEmpty(), "Local location cannot contain user info: %s", location);
        checkArgument(location.host().isEmpty(), "Local location cannot contain a host: %s", location);
    }

    private Path toPath(Location location)
    {
        // ensure path isn't something like '../../data'
        Path localPath = rootPath.resolve(location.path()).normalize();
        checkArgument(localPath.startsWith(rootPath), "Location references data outside of the root: %s", location);
        return localPath;
    }
}
