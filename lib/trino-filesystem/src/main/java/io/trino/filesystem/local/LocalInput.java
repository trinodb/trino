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
package io.trino.filesystem.local;

import io.trino.filesystem.TrinoInput;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

class LocalInput
        implements TrinoInput
{
    private final File file;
    private final RandomAccessFile input;

    public LocalInput(File file)
            throws IOException
    {
        this.file = requireNonNull(file, "file is null");
        this.input = new RandomAccessFile(file, "r");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        input.seek(position);
        input.readFully(buffer, bufferOffset, bufferLength);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        int readSize = (int) min(file.length(), bufferLength);
        readFully(file.length() - readSize, buffer, bufferOffset, readSize);
        return readSize;
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    public String toString()
    {
        return file.getPath();
    }
}
