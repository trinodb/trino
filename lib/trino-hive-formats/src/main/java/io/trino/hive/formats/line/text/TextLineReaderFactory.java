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

import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.compression.Codec;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.FooterAwareLineReader;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;
import io.trino.hive.formats.line.LineReaderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class TextLineReaderFactory
        implements LineReaderFactory
{
    private final int fileBufferSize;
    private final int initialLineBufferSize;
    private final int maxLineLength;

    public TextLineReaderFactory(int fileBufferSize, int initialLineBufferSize, int maxLineLength)
    {
        this.fileBufferSize = fileBufferSize;
        this.initialLineBufferSize = initialLineBufferSize;
        this.maxLineLength = maxLineLength;
    }

    @Override
    public String getHiveOutputFormatClassName()
    {
        return "org.apache.hadoop.mapred.TextInputFormat";
    }

    @Override
    public LineBuffer createLineBuffer()
    {
        return new LineBuffer(initialLineBufferSize, maxLineLength);
    }

    @Override
    public LineReader createLineReader(
            TrinoInputFile inputFile,
            long start,
            long length,
            int headerCount,
            int footerCount)
            throws IOException
    {
        InputStream inputStream = inputFile.newStream();
        try {
            Optional<Codec> codec = CompressionKind.forFile(inputFile.location())
                    .map(CompressionKind::createCodec);
            if (codec.isPresent()) {
                checkArgument(start == 0, "Compressed files are not splittable");
                // for compressed input, we do not know the length of the uncompressed text
                length = Long.MAX_VALUE;
                inputStream = codec.get().createStreamDecompressor(inputStream);
            }

            LineReader lineReader = new TextLineReader(inputStream, fileBufferSize, start, length);

            //  Only skip header rows when the split is at the beginning of the file
            if (headerCount > 0) {
                skipHeader(lineReader, headerCount);
            }

            if (footerCount > 0) {
                lineReader = new FooterAwareLineReader(lineReader, footerCount, this::createLineBuffer);
            }
            return lineReader;
        }
        catch (Throwable throwable) {
            try (Closeable ignored = inputStream) {
                throw throwable;
            }
        }
    }

    private void skipHeader(LineReader lineReader, int headerCount)
            throws IOException
    {
        LineBuffer lineBuffer = createLineBuffer();
        for (int i = 0; i < headerCount; i++) {
            if (!lineReader.readLine(lineBuffer)) {
                return;
            }
        }
    }
}
