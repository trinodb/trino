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
import io.trino.filesystem.TrinoInput;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class MemoryInput
        implements TrinoInput
{
    private final String location;
    private final Slice data;

    public MemoryInput(String location, Slice data)
    {
        this.location = requireNonNull(location, "location is null");
        this.data = requireNonNull(data, "data is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        data.getBytes(toIntExact(position), buffer, bufferOffset, bufferLength);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
    {
        int readSize = min(data.length(), bufferLength);
        readFully(data.length() - readSize, buffer, bufferOffset, readSize);
        return readSize;
    }

    @Override
    public void close() {}

    @Override
    public String toString()
    {
        return location;
    }
}
