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

import io.trino.filesystem.TrinoOutputFile;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.MemoryAwareFileSystem;
import io.trino.memory.context.AggregatedMemoryContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static io.trino.hdfs.FileSystemUtils.getRawFileSystem;
import static java.util.Objects.requireNonNull;

class HdfsOutputFile
        implements TrinoOutputFile
{
    private final String path;
    private final HdfsEnvironment environment;
    private final HdfsContext context;

    public HdfsOutputFile(String path, HdfsEnvironment environment, HdfsContext context)
    {
        this.path = requireNonNull(path, "path is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return create(false, memoryContext);
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return create(true, memoryContext);
    }

    private OutputStream create(boolean overwrite, AggregatedMemoryContext memoryContext)
            throws IOException
    {
        Path file = hadoopPath(path);
        FileSystem fileSystem = environment.getFileSystem(context, file);
        FileSystem rawFileSystem = getRawFileSystem(fileSystem);
        if (rawFileSystem instanceof MemoryAwareFileSystem memoryAwareFileSystem) {
            return environment.doAs(context.getIdentity(), () -> memoryAwareFileSystem.create(file, memoryContext));
        }
        return environment.doAs(context.getIdentity(), () -> fileSystem.create(file, overwrite));
    }

    @Override
    public String location()
    {
        return path;
    }

    @Override
    public String toString()
    {
        return location();
    }
}
