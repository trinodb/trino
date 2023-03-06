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

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.hdfs.FileSystemWithBatchDelete;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

    public HdfsFileSystem(HdfsEnvironment environment, HdfsContext context)
    {
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public TrinoInputFile newInputFile(String location)
    {
        return new HdfsInputFile(location, null, environment, context);
    }

    @Override
    public TrinoInputFile newInputFile(String location, long length)
    {
        return new HdfsInputFile(location, length, environment, context);
    }

    @Override
    public TrinoOutputFile newOutputFile(String location)
    {
        return new HdfsOutputFile(location, environment, context);
    }

    @Override
    public void deleteFile(String location)
            throws IOException
    {
        Path file = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, file);
        environment.doAs(context.getIdentity(), () -> {
            if (!fileSystem.delete(file, false)) {
                throw new IOException("Failed to delete file: " + file);
            }
            return null;
        });
    }

    @Override
    public void deleteFiles(Collection<String> locations)
            throws IOException
    {
        Map<Path, List<Path>> pathsGroupedByDirectory = locations.stream().collect(
                groupingBy(
                        path -> hadoopPath(path.replaceFirst("/[^/]*$", "")),
                        mapping(HadoopPaths::hadoopPath, toList())));
        for (Entry<Path, List<Path>> directoryWithPaths : pathsGroupedByDirectory.entrySet()) {
            FileSystem rawFileSystem = getRawFileSystem(environment.getFileSystem(context, directoryWithPaths.getKey()));
            environment.doAs(context.getIdentity(), () -> {
                if (rawFileSystem instanceof FileSystemWithBatchDelete fileSystemWithBatchDelete) {
                    fileSystemWithBatchDelete.deleteFiles(directoryWithPaths.getValue());
                }
                else {
                    for (Path path : directoryWithPaths.getValue()) {
                        rawFileSystem.delete(path, false);
                    }
                }
                return null;
            });
        }
    }

    @Override
    public void deleteDirectory(String location)
            throws IOException
    {
        Path directory = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, directory);
        environment.doAs(context.getIdentity(), () -> {
            if (!fileSystem.delete(directory, true) && fileSystem.exists(directory)) {
                throw new IOException("Failed to delete directory: " + directory);
            }
            return null;
        });
    }

    @Override
    public void renameFile(String source, String target)
            throws IOException
    {
        Path sourcePath = hadoopPath(source);
        Path targetPath = hadoopPath(target);
        FileSystem fileSystem = environment.getFileSystem(context, sourcePath);
        environment.doAs(context.getIdentity(), () -> {
            if (!fileSystem.rename(sourcePath, targetPath)) {
                throw new IOException(format("Failed to rename [%s] to [%s]", source, target));
            }
            return null;
        });
    }

    @Override
    public FileIterator listFiles(String location)
            throws IOException
    {
        Path directory = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, directory);
        return environment.doAs(context.getIdentity(), () -> {
            try {
                return new HdfsFileIterator(location, fileSystem, fileSystem.listFiles(directory, true));
            }
            catch (FileNotFoundException e) {
                return FileIterator.empty();
            }
        });
    }
}
