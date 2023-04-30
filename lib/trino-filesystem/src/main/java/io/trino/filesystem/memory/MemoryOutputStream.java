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
package io.trino.filesystem.memory;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class MemoryOutputStream
        extends OutputStream
{
    public interface OnStreamClose
    {
        void onClose(Slice data)
                throws IOException;
    }

    private final Location location;
    private final OnStreamClose onStreamClose;
    private ByteArrayOutputStream stream = new ByteArrayOutputStream();

    public MemoryOutputStream(Location location, OnStreamClose onStreamClose)
    {
        this.location = requireNonNull(location, "location is null");
        this.onStreamClose = requireNonNull(onStreamClose, "onStreamClose is null");
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        stream.write(b);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);

        ensureOpen();
        stream.write(buffer, offset, length);
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
    }

    private void ensureOpen()
            throws IOException
    {
        if (stream == null) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (stream != null) {
            byte[] data = stream.toByteArray();
            stream = null;
            onStreamClose.onClose(Slices.wrappedBuffer(data));
        }
    }
}
