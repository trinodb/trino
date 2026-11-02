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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputStream;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static java.util.Objects.requireNonNull;

class HdfsOutputStream
        extends TrinoOutputStream
{
    private final Location location;
    private final FSDataOutputStream out;
    private final HdfsEnvironment environment;
    private final HdfsContext context;
    private boolean closed;

    public HdfsOutputStream(Location location, FSDataOutputStream out, HdfsEnvironment environment, HdfsContext context)
    {
        this.location = requireNonNull(location, "location is null");
        this.out = requireNonNull(out, "out is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        // handle Kerberos ticket refresh during long write operations
        environment.doAs(context.getIdentity(), () -> {
            out.write(b);
            return null;
        });
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        ensureOpen();
        // handle Kerberos ticket refresh during long write operations
        environment.doAs(context.getIdentity(), () -> {
            out.write(b, off, len);
            return null;
        });
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        out.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            out.close();
        }
    }

    @Override
    public void abort()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        // close fails if another writer replaced the file, so the file is deleted only after a successful close
        out.close();
        // the file is created exclusively when this stream is opened
        Path file = hadoopPath(location);
        FileSystem fileSystem = environment.getFileSystem(context, file);
        environment.doAs(context.getIdentity(), () -> fileSystem.delete(file, false));
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }
}
