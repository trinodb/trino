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

import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.security.ConnectorIdentity;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public class TrinoOutputStream
        extends FilterOutputStream
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ConnectorIdentity connectorIdentity;

    public TrinoOutputStream(OutputStream out, HdfsEnvironment hdfsEnvironment, ConnectorIdentity connectorIdentity)
    {
        super(out);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.connectorIdentity = requireNonNull(connectorIdentity, "connectorIdentity is null");
    }

    @Override
    public void write(int b)
            throws IOException
    {
        hdfsEnvironment.doAs(connectorIdentity, () -> {
            out.write(b);
            return null;
        });
    }

    @Override
    public void write(byte[] b)
            throws IOException
    {
        hdfsEnvironment.doAs(connectorIdentity, () -> {
            out.write(b);
            return null;
        });
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        hdfsEnvironment.doAs(connectorIdentity, () -> {
            out.write(b, off, len);
            return null;
        });
    }

    @Override
    public void flush()
            throws IOException
    {
        hdfsEnvironment.doAs(connectorIdentity, () -> {
            out.flush();
            return null;
        });
    }

    @Override
    public void close()
            throws IOException
    {
        hdfsEnvironment.doAs(connectorIdentity, () -> {
            out.close();
            return null;
        });
    }
}
