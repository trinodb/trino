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
import static java.lang.String.format;

class MaterializingReader
        extends FilterReader
{
    private final char[] headChars;
    private int remaining;
    private int currentOffset;

    protected MaterializingReader(Reader reader, int maxHeadChars)
    {
        super(reader);
        verify(maxHeadChars > 0 && maxHeadChars <= 128 * 1024, "maxHeadChars must be between 1 and 128 KB");
        this.headChars = new char[maxHeadChars];
    }

    @Override
    public int read()
            throws IOException
    {
        int value = super.read();
        if (value != -1) {
            if (currentOffset < headChars.length) {
                headChars[currentOffset++] = (char) value;
            }
            else {
                remaining++;
            }
        }
        return value;
    }

    @Override
    public int read(char[] cbuf, int off, int len)
            throws IOException
    {
        int read = super.read(cbuf, off, len);
        if (read > 0) {
            int copyLength = Math.min(read, headChars.length - currentOffset);
            if (read > copyLength) {
                remaining += read - copyLength;
            }
            if (copyLength > 0) {
                System.arraycopy(cbuf, off, headChars, currentOffset, copyLength);
                currentOffset += copyLength;
            }
        }
        return read;
    }

    public String getHeadString()
    {
        return String.valueOf(headChars, 0, currentOffset) + (remaining > 0 ? format("... [" + bytesOmitted(remaining) + "]", remaining) : "");
    }

    private String bytesOmitted(long bytes)
    {
        if (bytes == 1) {
            return "1 more byte";
        }
        return format("%d more bytes", bytes);
    }
}
