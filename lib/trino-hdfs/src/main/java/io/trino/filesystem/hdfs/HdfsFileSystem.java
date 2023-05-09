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
package io.trino.filesystem.hdfs;

import io.airlift.stats.TimeStat;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.hdfs.FileSystemWithBatchDelete;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;

import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static io.trino.hdfs.FileSystemUtils.getRawFileSystem;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

class HdfsFileSystem
        implements TrinoFileSystem
{
    private final HdfsEnvironment environment;
    private final HdfsContext context;
    private final TrinoHdfsFileSystemStats stats;

    private final Map<FileSystem, Boolean> hierarchicalFileSystemCache = new IdentityHashMap<>();

    public HdfsFileSystem(HdfsEnvironment environment, HdfsContext context, TrinoHdfsFileSystemStats stats)
    {
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new HdfsInputFile(location, null, environment, context, stats.getOpenFileCalls());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new HdfsInputFile(location, length, environment, context, stats.getOpenFileCalls());
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return new HdfsOutputFile(location, environment, context, stats.getCreateFileCalls());
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        stats.getDeleteFileCalls().newCall();
        Path file = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, file);
        environment.doAs(context.getIdentity(), () -> {
            try (TimeStat.BlockTimer ignored = stats.getDeleteFileCalls().time()) {
                if (!fileSystem.delete(file, false)) {
                    throw new IOException("Failed to delete file: " + file);
                }
                return null;
            }
            catch (IOException e) {
                stats.getDeleteFileCalls().recordException(e);
                throw e;
            }
        });
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        Map<Path, List<Path>> pathsGroupedByDirectory = locations.stream().collect(
                groupingBy(
                        location -> hadoopPath(location.parentDirectory()),
                        mapping(HadoopPaths::hadoopPath, toList())));
        for (Entry<Path, List<Path>> directoryWithPaths : pathsGroupedByDirectory.entrySet()) {
            FileSystem rawFileSystem = getRawFileSystem(environment.getFileSystem(context, directoryWithPaths.getKey()));
            environment.doAs(context.getIdentity(), () -> {
                if (rawFileSystem instanceof FileSystemWithBatchDelete fileSystemWithBatchDelete) {
                    fileSystemWithBatchDelete.deleteFiles(directoryWithPaths.getValue());
                }
                else {
                    for (Path path : directoryWithPaths.getValue()) {
                        stats.getDeleteFileCalls().newCall();
                        try (TimeStat.BlockTimer ignored = stats.getDeleteFileCalls().time()) {
                            rawFileSystem.delete(path, false);
                        }
                        catch (IOException e) {
                            stats.getDeleteFileCalls().recordException(e);
                            throw e;
                        }
                    }
                }
                return null;
            });
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        stats.getDeleteDirectoryCalls().newCall();
        Path directory = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, directory);
        environment.doAs(context.getIdentity(), () -> {
            try (TimeStat.BlockTimer ignored = stats.getDeleteDirectoryCalls().time()) {
                if (!fileSystem.delete(directory, true) && fileSystem.exists(directory)) {
                    throw new IOException("Failed to delete directory: " + directory);
                }
                return null;
            }
            catch (IOException e) {
                stats.getDeleteDirectoryCalls().recordException(e);
                throw e;
            }
        });
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        stats.getRenameFileCalls().newCall();
        Path sourcePath = hadoopPath(source);
        Path targetPath = hadoopPath(target);
        FileSystem fileSystem = environment.getFileSystem(context, sourcePath);
        environment.doAs(context.getIdentity(), () -> {
            try (TimeStat.BlockTimer ignored = stats.getRenameFileCalls().time()) {
                if (!fileSystem.rename(sourcePath, targetPath)) {
                    throw new IOException(format("Failed to rename [%s] to [%s]", source, target));
                }
                return null;
            }
            catch (IOException e) {
                stats.getRenameFileCalls().recordException(e);
                throw e;
            }
        });
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        stats.getListFilesCalls().newCall();
        Path directory = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, directory);
        return environment.doAs(context.getIdentity(), () -> {
            try (TimeStat.BlockTimer ignored = stats.getListFilesCalls().time()) {
                return new HdfsFileIterator(location, directory, fileSystem.listFiles(directory, true));
            }
            catch (IOException e) {
                stats.getListFilesCalls().recordException(e);
                if (e instanceof FileNotFoundException) {
                    return FileIterator.empty();
                }
                throw e;
            }
        });
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        stats.getDirectoryExistsCalls().newCall();
        Path directory = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, directory);

        return environment.doAs(context.getIdentity(), () -> {
            if (!hierarchical(fileSystem, location)) {
                return Optional.empty();
            }

            try (TimeStat.BlockTimer ignored = stats.getDirectoryExistsCalls().time()) {
                FileStatus fileStatus = fileSystem.getFileStatus(directory);
                return Optional.of(fileStatus.isDirectory());
            }
            catch (FileNotFoundException e) {
                return Optional.of(false);
            }
            catch (IOException e) {
                stats.getListFilesCalls().recordException(e);
                throw e;
            }
        });
    }

    private boolean hierarchical(FileSystem fileSystem, Location rootLocation)
    {
        Boolean cachedResult = hierarchicalFileSystemCache.get(fileSystem);
        if (cachedResult != null) {
            return cachedResult;
        }

        // Hierarchical file systems will fail to list directories which do not exist.
        // Object store file systems like S3 will allow these kinds of operations.
        // Attempt to list a path which does not exist to know which one we have.
        try {
            fileSystem.listStatus(hadoopPath(rootLocation.appendPath(UUID.randomUUID().toString())));
            hierarchicalFileSystemCache.putIfAbsent(fileSystem, false);
            return false;
        }
        catch (IOException e) {
            // Being overly broad to avoid throwing an exception with the random UUID path in it.
            // Instead, defer to later calls to fail with a more appropriate message.
            hierarchicalFileSystemCache.putIfAbsent(fileSystem, true);
            return true;
        }
    }
}
