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
package io.trino.client;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;

import static com.google.common.base.Verify.verify;

class MaterializingReader
        extends FilterReader
{
    private final char[] initialChars;
    private int currentOffset;

    protected MaterializingReader(Reader reader, int initialChars)
    {
        super(reader);
        verify(initialChars > 0 && initialChars <= 128 * 1024, "initialChars must be between 1 and 128 KB");
        this.initialChars = new char[initialChars];
    }

    @Override
    public int read()
            throws IOException
    {
        int value = super.read();
        if (value != -1 && currentOffset < initialChars.length) {
            initialChars[currentOffset++] = (char) value;
        }
        return value;
    }

    @Override
    public int read(char[] cbuf, int off, int len)
            throws IOException
    {
        int read = super.read(cbuf, off, len);
        if (read > 0) {
            int copyLength = Math.min(read, initialChars.length - currentOffset);
            if (copyLength > 0) {
                System.arraycopy(cbuf, off, initialChars, currentOffset, copyLength);
                currentOffset += copyLength;
            }
        }
        return read;
    }

    public String getInitalString()
    {
        return String.valueOf(initialChars, 0, currentOffset);
    }
}
