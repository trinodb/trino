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

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.line.FooterAwareLineReader;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;
import io.trino.hive.formats.line.LineReaderFactory;

import java.io.IOException;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.formats.HiveClassNames.SEQUENCEFILE_INPUT_FORMAT_CLASS;

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
    public Set<String> getHiveInputFormatClassNames()
    {
        return ImmutableSet.of(SEQUENCEFILE_INPUT_FORMAT_CLASS);
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

        if (headerCount > 0) {
            checkArgument(start == 0 || headerCount == 1, "file cannot be split when there is more than one header row");
            // header is only skipped at the beginning of the file
            if (start == 0) {
                skipHeader(lineReader, headerCount);
            }
        }

        if (footerCount > 0) {
            checkArgument(start == 0, "file cannot be split when there are footer rows");
            lineReader = new FooterAwareLineReader(lineReader, footerCount, this::createLineBuffer);
        }
        return lineReader;
    }

    @Override
    public TrinoInputFile newInputFile(TrinoFileSystem trinoFileSystem, Location path, long estimatedFileSize, long fileModifiedTime)
    {
        // estimatedFileSize contains padded bytes
        // The reads in SequenceFileReader rely on non-padded file length to detect end of file
        return trinoFileSystem.newInputFile(path);
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
