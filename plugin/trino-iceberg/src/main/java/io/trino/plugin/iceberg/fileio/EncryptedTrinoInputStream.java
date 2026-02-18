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
package io.trino.plugin.iceberg.fileio;

import io.trino.filesystem.TrinoInputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class EncryptedTrinoInputStream
        extends TrinoInputStream
{
    private final SeekableInputStream input;

    public EncryptedTrinoInputStream(SeekableInputStream input)
    {
        this.input = requireNonNull(input, "input is null");
    }

    @Override
    public long getPosition()
            throws IOException
    {
        return input.getPos();
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        input.seek(position);
    }

    @Override
    public int read()
            throws IOException
    {
        return input.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        return input.read(buffer, offset, length);
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }
}
