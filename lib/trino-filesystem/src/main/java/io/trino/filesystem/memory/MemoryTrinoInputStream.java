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
import io.airlift.slice.SliceInput;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;

public class MemoryTrinoInputStream
        extends TrinoInputStream
{
    private final SliceInput input;

    public MemoryTrinoInputStream(Slice data)
    {
        input = data.getInput();
    }

    @Override
    public long getPosition()
    {
        return input.position();
    }

    @Override
    public void seek(long position)
    {
        input.setPosition(position);
    }

    @Override
    public int read()
            throws IOException
    {
        return input.read();
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        return input.read(destination, destinationIndex, length);
    }

    @Override
    public long skip(long length)
    {
        return input.skip(length);
    }
}
