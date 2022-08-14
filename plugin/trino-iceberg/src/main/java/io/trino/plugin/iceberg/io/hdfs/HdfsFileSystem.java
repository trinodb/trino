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
package io.trino.plugin.iceberg.io.hdfs;

import io.trino.plugin.hive.HdfsContext;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.io.FileIterator;
import io.trino.plugin.iceberg.io.TrinoFileSystem;
import io.trino.plugin.iceberg.io.TrinoInputFile;
import io.trino.plugin.iceberg.io.TrinoOutputFile;
import io.trino.plugin.iceberg.io.fileio.ForwardingFileIo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.FileIO;

import java.io.FileNotFoundException;
import java.io.IOException;

import static io.trino.plugin.iceberg.io.hdfs.HadoopPaths.hadoopPath;
import static java.util.Objects.requireNonNull;

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
    public TrinoInputFile newInputFile(String path)
    {
        return new HdfsInputFile(path, null, environment, context);
    }

    @Override
    public TrinoInputFile newInputFile(String path, long length)
    {
        return new HdfsInputFile(path, length, environment, context);
    }

    @Override
    public TrinoOutputFile newOutputFile(String path)
    {
        return new HdfsOutputFile(path, environment, context);
    }

    @Override
    public void deleteFile(String path)
            throws IOException
    {
        Path file = hadoopPath(path);
        FileSystem fileSystem = environment.getFileSystem(context, file);
        environment.doAs(context.getIdentity(), () -> {
            if (!fileSystem.delete(file, false)) {
                throw new IOException("Failed to delete file: " + file);
            }
            return null;
        });
    }

    @Override
    public void deleteDirectory(String path)
            throws IOException
    {
        Path directory = hadoopPath(path);
        FileSystem fileSystem = environment.getFileSystem(context, directory);
        environment.doAs(context.getIdentity(), () -> {
            if (!fileSystem.delete(directory, true) && fileSystem.exists(directory)) {
                throw new IOException("Failed to delete directory: " + directory);
            }
            return null;
        });
    }

    @Override
    public FileIterator listFiles(String path)
            throws IOException
    {
        Path directory = hadoopPath(path);
        FileSystem fileSystem = environment.getFileSystem(context, directory);
        return environment.doAs(context.getIdentity(), () -> {
            try {
                return new HdfsFileIterator(path, fileSystem, fileSystem.listFiles(directory, true));
            }
            catch (FileNotFoundException e) {
                return FileIterator.empty();
            }
        });
    }

    @Override
    public FileIO toFileIo()
    {
        return new ForwardingFileIo(this);
    }
}
