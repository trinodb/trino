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

import com.google.common.primitives.Ints;
import io.trino.filesystem.TrinoInputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

class FileTrinoInputStream
        extends TrinoInputStream
{
    private final RandomAccessFile input;

    public FileTrinoInputStream(File file)
            throws FileNotFoundException
    {
        this.input = new RandomAccessFile(file, "r");
    }

    @Override
    public long getPosition()
            throws IOException
    {
        return input.getFilePointer();
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
    public int read(byte[] b)
            throws IOException
    {
        return input.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        return input.read(b, off, len);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        return input.skipBytes(Ints.saturatedCast(n));
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }
}
