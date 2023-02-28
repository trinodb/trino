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
package io.trino.hive.formats.line.text;

import com.google.common.io.CountingOutputStream;
import io.airlift.slice.Slice;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.LineWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.LongSupplier;

import static io.airlift.slice.SizeOf.instanceSize;

public class TextLineWriter
        implements LineWriter
{
    private static final int INSTANCE_SIZE = instanceSize(TextLineWriter.class);
    private final LongSupplier writtenBytes;
    private final OutputStream outputStream;

    public TextLineWriter(OutputStream outputStream, Optional<CompressionKind> compressionKind)
            throws IOException
    {
        CountingOutputStream countingOutputStream = new CountingOutputStream(outputStream);
        writtenBytes = countingOutputStream::getCount;
        if (compressionKind.isPresent()) {
            this.outputStream = compressionKind.get().createCodec().createStreamCompressor(countingOutputStream);
        }
        else {
            this.outputStream = outputStream;
        }
    }

    @Override
    public long getWrittenBytes()
    {
        return writtenBytes.getAsLong();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public void write(Slice value)
            throws IOException
    {
        value.getBytes(0, outputStream, value.length());
        outputStream.write('\n');
    }

    @Override
    public void close()
            throws IOException
    {
        outputStream.close();
    }
}
