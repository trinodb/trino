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
import io.trino.filesystem.SeekableInputStream;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;

import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class MemoryInputFile
        implements TrinoInputFile
{
    private final String location;
    private final Slice data;

    public MemoryInputFile(String location, Slice data)
    {
        this.location = requireNonNull(location, "location is null");
        this.data = requireNonNull(data, "data is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new MemoryInput(location, data);
    }

    @Override
    public SeekableInputStream newStream()
            throws IOException
    {
        return new MemorySeekableInputStream(data);
    }

    @Override
    public long length()
            throws IOException
    {
        return data.length();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return Instant.EPOCH;
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return true;
    }

    @Override
    public String location()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return location();
    }
}
