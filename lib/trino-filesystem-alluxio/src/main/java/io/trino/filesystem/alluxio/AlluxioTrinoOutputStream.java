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
package io.trino.filesystem.alluxio;

import io.trino.filesystem.Location;

import java.io.IOException;
import java.io.OutputStream;

public final class AlluxioTrinoOutputStream
        extends OutputStream
{
    private final Location location;
    private final OutputStream delegate;

    private volatile boolean closed;

    public AlluxioTrinoOutputStream(Location location, OutputStream delegate)
    {
        this.location = location;
        this.delegate = delegate;
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        delegate.write(b);
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        delegate.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        delegate.close();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream for %s closed: ".formatted(location));
        }
    }
}
