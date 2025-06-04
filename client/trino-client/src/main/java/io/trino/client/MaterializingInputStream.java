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

import io.airlift.units.DataSize;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.CodingErrorAction.IGNORE;
import static java.nio.charset.StandardCharsets.UTF_8;

class MaterializingInputStream
        extends FilterInputStream
{
    private byte[] head;
    private int remaining;
    private int currentOffset;

    protected MaterializingInputStream(InputStream stream, DataSize maxBytes)
    {
        super(stream);
        this.head = new byte[toIntExact(maxBytes.toBytes())]; // caller is responsible for reasonable sizing
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
            int copyLength = min(read, head.length - currentOffset);
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
        return getHeadString(UTF_8);
    }

    public String getHeadString(Charset charset)
    {
        if (head == null) {
            return "<empty>";
        }

        CharsetDecoder charsetDecoder = firstNonNull(charset, UTF_8).newDecoder()
                .onMalformedInput(IGNORE)
                .onUnmappableCharacter(IGNORE);
        try {
            return charsetDecoder.decode(ByteBuffer.wrap(head, 0, currentOffset)) + (remaining > 0 ? format("... [" + bytesOmitted(remaining) + "]", remaining) : "");
        }
        catch (CharacterCodingException e) {
            return format("<error:%s>", e.getMessage());
        }
    }

    private String bytesOmitted(long bytes)
    {
        if (bytes == 1) {
            return "1 more byte";
        }
        return format("%d more bytes", bytes);
    }

    @Override
    public void close()
            throws IOException
    {
        super.close();
        head = null;
    }
}
