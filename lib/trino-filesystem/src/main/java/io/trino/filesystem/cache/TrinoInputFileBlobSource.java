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
package io.trino.filesystem.cache;

import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.cache.BlobSource;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class TrinoInputFileBlobSource
        implements BlobSource
{
    private final TrinoInputFile delegate;
    private TrinoInput input;
    private boolean closed;

    public TrinoInputFileBlobSource(TrinoInputFile delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public long length()
            throws IOException
    {
        return delegate.length();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        input().readFully(position, buffer, offset, length);
    }

    // The input is held open across reads: pass-through blobs (uncached or oversized files)
    // read repeatedly, and opening a fresh input per read would issue one remote request per
    // call. The cache or the wrapping blob closes this source when done.
    private TrinoInput input()
            throws IOException
    {
        // Close is terminal: reopening here would leak an input that nothing closes, so a read
        // by an owner that already released the source fails instead
        if (closed) {
            throw new IOException("Blob source is closed: " + this);
        }
        if (input == null) {
            input = delegate.newInput();
        }
        return input;
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        if (input != null) {
            input.close();
            input = null;
        }
    }

    @Override
    public String toString()
    {
        return delegate.location().toString();
    }
}
