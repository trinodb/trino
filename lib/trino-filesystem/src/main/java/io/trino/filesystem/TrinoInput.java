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
package io.trino.filesystem;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.Closeable;
import java.io.IOException;

public interface TrinoInput
        extends Closeable
{
    void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

    int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

    default Slice readFully(long position, int length)
            throws IOException
    {
        byte[] buffer = new byte[length];
        readFully(position, buffer, 0, length);
        return Slices.wrappedBuffer(buffer);
    }

    default Slice readTail(int length)
            throws IOException
    {
        byte[] buffer = new byte[length];
        int read = readTail(buffer, 0, length);
        return Slices.wrappedBuffer(buffer, 0, read);
    }
}
