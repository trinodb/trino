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
package io.trino.hive.formats.line.sequence;

import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.line.FooterAwareLineReader;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;
import io.trino.hive.formats.line.LineReaderFactory;

import java.io.IOException;

public class SequenceFileReaderFactory
        implements LineReaderFactory
{
    private final int initialLineBufferSize;
    private final int maxLineLength;

    public SequenceFileReaderFactory(int initialLineBufferSize, int maxLineLength)
    {
        this.initialLineBufferSize = initialLineBufferSize;
        this.maxLineLength = maxLineLength;
    }

    @Override
    public String getHiveOutputFormatClassName()
    {
        return "org.apache.hadoop.mapred.SequenceFileInputFormat";
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
        LineReader lineReader = new SequenceFileReader(inputFile, start, length);

        //  Only skip header rows when the split is at the beginning of the file
        if (headerCount > 0) {
            skipHeader(lineReader, headerCount);
        }

        if (footerCount > 0) {
            lineReader = new FooterAwareLineReader(lineReader, footerCount, this::createLineBuffer);
        }
        return lineReader;
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
