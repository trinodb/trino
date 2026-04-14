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

import io.trino.spi.cache.BlobSource;
import io.trino.spi.filesystem.TrinoInput;
import io.trino.spi.filesystem.TrinoInputFile;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class TrinoInputFileBlobSource
        implements BlobSource
{
    private final TrinoInputFile delegate;

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
        try (TrinoInput input = delegate.newInput()) {
            input.readFully(position, buffer, offset, length);
        }
    }

    @Override
    public String toString()
    {
        return delegate.location().toString();
    }
}
