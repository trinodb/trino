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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

class MaterializingInputStream
        extends FilterInputStream
{
    private final byte[] head;
    private int remaining;
    private int currentOffset;

    protected MaterializingInputStream(InputStream stream, int maxBytes)
    {
        super(stream);
        verify(maxBytes > 0 && maxBytes <= 8 * 1024, "maxBytes must be between 1B and 8 KB");
        this.head = new byte[maxBytes];
    }

    @Override
    public int read()
            throws IOException
    {
        int value = super.read();
        if (value != -1) {
            if (currentOffset < head.length) {
                head[currentOffset++] = (byte) value;
            }
            else {
                remaining++;
            }
        }
        return value;
    }

    @Override
    public int read(byte[] buffer, int off, int len)
            throws IOException
    {
        int read = super.read(buffer, off, len);
        if (read > 0) {
            int copyLength = Math.min(read, head.length - currentOffset);
            if (read > copyLength) {
                remaining += read - copyLength;
            }
            if (copyLength > 0) {
                System.arraycopy(buffer, off, head, currentOffset, copyLength);
                currentOffset += copyLength;
            }
        }
        return read;
    }

    @Override
    public int read(byte[] buffer)
            throws IOException
    {
        return read(buffer, 0, buffer.length);
    }

    public String getHeadString()
    {
        return new String(head, 0, currentOffset, UTF_8) + (remaining > 0 ? format("... [" + bytesOmitted(remaining) + "]", remaining) : "");
    }

    private String bytesOmitted(long bytes)
    {
        if (bytes == 1) {
            return "1 more byte";
        }
        return format("%d more bytes", bytes);
    }
}
