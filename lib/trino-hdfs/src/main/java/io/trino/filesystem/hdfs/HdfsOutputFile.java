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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.hdfs.CallStats;
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
    private final Location location;
    private final HdfsEnvironment environment;
    private final HdfsContext context;
    private final CallStats createFileCallStat;

    public HdfsOutputFile(Location location, HdfsEnvironment environment, HdfsContext context, CallStats createFileCallStat)
    {
        this.location = requireNonNull(location, "location is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
        this.createFileCallStat = requireNonNull(createFileCallStat, "createFileCallStat is null");
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
        createFileCallStat.newCall();
        Path file = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, file);
        FileSystem rawFileSystem = getRawFileSystem(fileSystem);
        try (TimeStat.BlockTimer ignored = createFileCallStat.time()) {
            if (rawFileSystem instanceof MemoryAwareFileSystem memoryAwareFileSystem) {
                return environment.doAs(context.getIdentity(), () -> memoryAwareFileSystem.create(file, memoryContext));
            }
            return environment.doAs(context.getIdentity(), () -> fileSystem.create(file, overwrite));
        }
        catch (IOException e) {
            createFileCallStat.recordException(e);
            throw e;
        }
    }

    @Override
    public Location location()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return location().toString();
    }
}
