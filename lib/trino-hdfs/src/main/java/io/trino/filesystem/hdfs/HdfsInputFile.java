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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.hdfs.CallStats;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.time.Instant;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static java.util.Objects.requireNonNull;

class HdfsInputFile
        implements TrinoInputFile
{
    private final Location location;
    private final HdfsEnvironment environment;
    private final HdfsContext context;
    private final Path file;
    private Long length;
    private FileStatus status;
    private CallStats openFileCallStat;

    public HdfsInputFile(Location location, Long length, HdfsEnvironment environment, HdfsContext context, CallStats openFileCallStat)
    {
        this.location = requireNonNull(location, "location is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
        this.file = hadoopPath(location);
        this.length = length;
        this.openFileCallStat = requireNonNull(openFileCallStat, "openFileCallStat is null");
        checkArgument(length == null || length >= 0, "length is negative");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new HdfsInput(openFile(), this);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new HdfsTrinoInputStream(openFile());
    }

    @Override
    public long length()
            throws IOException
    {
        if (length == null) {
            length = lazyStatus().getLen();
        }
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return Instant.ofEpochMilli(lazyStatus().getModificationTime());
    }

    @Override
    public boolean exists()
            throws IOException
    {
        FileSystem fileSystem = environment.getFileSystem(context, file);
        return environment.doAs(context.getIdentity(), () -> fileSystem.exists(file));
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

    private FSDataInputStream openFile()
            throws IOException
    {
        openFileCallStat.newCall();
        FileSystem fileSystem = environment.getFileSystem(context, file);
        return environment.doAs(context.getIdentity(), () -> {
            try (TimeStat.BlockTimer ignored = openFileCallStat.time()) {
                return fileSystem.open(file);
            }
            catch (IOException e) {
                openFileCallStat.recordException(e);
                throw e;
            }
        });
    }

    private FileStatus lazyStatus()
            throws IOException
    {
        if (status == null) {
            FileSystem fileSystem = environment.getFileSystem(context, file);
            status = environment.doAs(context.getIdentity(), () -> fileSystem.getFileStatus(file));
        }
        return status;
    }
}
