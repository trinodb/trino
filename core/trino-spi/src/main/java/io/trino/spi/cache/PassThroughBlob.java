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
package io.trino.spi.cache;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class PassThroughBlob
        implements Blob
{
    private final BlobSource source;

    public PassThroughBlob(BlobSource source)
    {
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public long length()
            throws IOException
    {
        return source.length();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        source.readFully(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        return source.readTail(buffer, offset, length);
    }

    @Override
    public void close() {}

    @Override
    public String toString()
    {
        return source.toString();
    }
}
