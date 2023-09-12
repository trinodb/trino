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
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

class HdfsOutputStream
        extends FSDataOutputStream
{
    private final Location location;
    private final HdfsEnvironment environment;
    private final ConnectorIdentity identity;
    private boolean closed;

    public HdfsOutputStream(Location location, FSDataOutputStream out, HdfsEnvironment environment, HdfsContext context)
    {
        super(out, null, out.getPos());
        this.location = requireNonNull(location, "location is null");
        this.environment = environment;
        this.identity = context.getIdentity();
    }

    @Override
    public OutputStream getWrappedStream()
    {
        // return the originally wrapped stream, not the delegate
        return ((FSDataOutputStream) super.getWrappedStream()).getWrappedStream();
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        // handle Kerberos ticket refresh during long write operations
        environment.doAs(identity, () -> {
            super.write(b);
            return null;
        });
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        ensureOpen();
        // handle Kerberos ticket refresh during long write operations
        environment.doAs(identity, () -> {
            super.write(b, off, len);
            return null;
        });
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        super.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        super.close();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }
}
