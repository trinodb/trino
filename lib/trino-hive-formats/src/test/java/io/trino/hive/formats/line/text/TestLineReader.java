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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.LineBuffer;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPOutputStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLineReader
{
    private static final int LINE_READER_INSTANCE_SIZE = instanceSize(TextLineReader.class);
    private static final int[] SKIP_SIZES = new int[] {1, 2, 3, 13, 101, 331, 443, 701, 853, 1021};

    @Test
    public void testSimple()
            throws IOException
    {
        assertLines(ImmutableList.<String>builder()
                .add("Line one")
                .add("Line two")
                .add("Line three")
                .add("Line four")
                .add("Line five")
                .build());
    }

    @Test
    public void testIncreasingLineLength()
            throws IOException
    {
        ImmutableList.Builder<String> lines = ImmutableList.builder();
        StringBuilder line = new StringBuilder();
        for (int length = 0; length < 1025; length++) {
            randomLine(line, length);
            lines.add(line.toString());
        }
        assertLines(lines.build());
    }

    @Test
    public void testDecreasingLineLength()
            throws IOException
    {
        ImmutableList.Builder<String> lines = ImmutableList.builder();
        StringBuilder line = new StringBuilder();
        for (int length = 1025; length >= 0; length--) {
            randomLine(line, length);
            lines.add(line.toString());
        }
        assertLines(lines.build());
    }

    @Test
    public void testEmptyLine()
            throws IOException
    {
        for (int lineCount = 1; lineCount < 100; lineCount++) {
            assertLines(Collections.nCopies(lineCount, ""));
        }
    }

    private static void assertLines(List<String> lines)
            throws IOException
    {
        assertThat(lines).isNotEmpty();

        for (String delimiter : ImmutableList.of("\n", "\r", "\r\n")) {
            for (int bufferSize : ImmutableList.of(16, 17, 61, 1024, 1024 * 1024)) {
                for (boolean delimiterAtEndOfFile : ImmutableList.of(true, false)) {
                    for (boolean bom : ImmutableList.of(true, false)) {
                        // if the last line is empty then there must be a delimiter at the end of the file
                        if (getLast(lines).isEmpty() && !delimiterAtEndOfFile) {
                            continue;
                        }
                        TestData testData = createInputData(lines, delimiter, delimiterAtEndOfFile, bom);
                        LineBuffer lineBuffer = createLineBuffer(lines);

                        assertLines(testData, lineBuffer, bufferSize, false);
                        assertLines(testData, lineBuffer, bufferSize, true);
                        assertSplitRead(testData, lineBuffer, bufferSize, bom);
                        assertSkipLines(testData, lineBuffer, bufferSize);
                    }
                }
            }
        }
    }

    private static void assertLines(TestData testData, LineBuffer lineBuffer, int bufferSize, boolean compressed)
            throws IOException
    {
        byte[] inputData = testData.inputData();

        TextLineReader lineReader;
        if (compressed) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(inputData.length);
            try (OutputStream compress = new GZIPOutputStream(out)) {
                compress.write(inputData);
            }
            inputData = out.toByteArray();
            lineReader = TextLineReader.createCompressedReader(new ByteArrayInputStream(inputData), bufferSize, CompressionKind.GZIP.createCodec());
        }
        else {
            lineReader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(inputData), bufferSize);
        }

        assertThat(lineReader.getRetainedSize()).isEqualTo(LINE_READER_INSTANCE_SIZE + sizeOfByteArray(bufferSize));
        for (ExpectedLine expectedLine : testData.expectedLines()) {
            assertThat(lineReader.readLine(lineBuffer)).isTrue();
            assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8)).isEqualTo(expectedLine.line());
            if (!compressed) {
                assertThat(lineReader.getCurrentPosition()).isEqualTo(expectedLine.endExclusive());
            }
            assertThat(lineReader.getRetainedSize()).isEqualTo(LINE_READER_INSTANCE_SIZE + sizeOfByteArray(bufferSize));
        }

        assertThat(lineReader.readLine(lineBuffer)).isFalse();
        assertThat(lineBuffer.isEmpty()).isTrue();
        assertThat(lineReader.isClosed()).isTrue();
        assertThat(lineReader.getRetainedSize()).isEqualTo(LINE_READER_INSTANCE_SIZE + sizeOfByteArray(bufferSize));
        assertThat(lineReader.getBytesRead()).isEqualTo(inputData.length);
    }

    private static void assertSplitRead(TestData testData, LineBuffer lineBuffer, int bufferSize, boolean bom)
            throws IOException
    {
        for (int i = 0; i < testData.expectedLines().size(); i++) {
            // only test the first and last few positions
            if (i > 3 && i < testData.expectedLines().size() - 3) {
                continue;
            }
            ExpectedLine expectedLine = testData.expectedLines().get(i);
            ImmutableSet<Integer> testPositions = ImmutableSet.<Integer>builder()
                    .add(expectedLine.start())
                    .add(expectedLine.start() + expectedLine.line().length())
                    .add(min(expectedLine.start() + expectedLine.line().length() + 1, expectedLine.endExclusive()))
                    .add(expectedLine.endExclusive())
                    .build();
            for (int testPosition : testPositions) {
                assertSplitRead(testData, lineBuffer, bufferSize, testPosition, bom);
            }
        }
    }

    private static void assertSplitRead(TestData testData, LineBuffer lineBuffer, int bufferSize, int splitPosition, boolean bom)
            throws IOException
    {
        if (splitPosition == 0 || splitPosition == testData.inputData().length) {
            return;
        }

        int lineIndex = 0;

        // read up to the first split
        TextLineReader lineReader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(testData.inputData()), bufferSize, 0, splitPosition);
        assertThat(lineReader.getCurrentPosition()).isEqualTo(bom ? 3 : 0);
        while (lineReader.readLine(lineBuffer)) {
            ExpectedLine expectedLine = testData.expectedLines().get(lineIndex++);
            assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8)).isEqualTo(expectedLine.line());
            assertThat(lineReader.getCurrentPosition()).isEqualTo(expectedLine.endExclusive());
        }
        assertThat(lineReader.getCurrentPosition() > splitPosition).isTrue();
        assertThat(lineBuffer.isEmpty()).isTrue();
        assertThat(lineReader.isClosed()).isTrue();

        lineReader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(testData.inputData()), bufferSize, splitPosition, testData.inputData().length - splitPosition);
        assertThat(lineReader.getCurrentPosition()).isEqualTo(testData.expectedLines().get(lineIndex - 1).endExclusive());
        while (lineReader.readLine(lineBuffer)) {
            ExpectedLine expectedLine = testData.expectedLines().get(lineIndex++);
            assertThat(lineReader.getCurrentPosition()).isEqualTo(expectedLine.endExclusive());
            assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8)).isEqualTo(expectedLine.line());
        }
        assertThat(lineBuffer.isEmpty()).isTrue();
        assertThat(lineReader.isClosed()).isTrue();
    }

    private static void assertSkipLines(TestData testData, LineBuffer lineBuffer, int bufferSize)
            throws IOException
    {
        List<String> lines = testData.expectedLines().stream()
                .map(ExpectedLine::line)
                .collect(toImmutableList());
        for (int skipLines : SKIP_SIZES) {
            skipLines = min(skipLines, lines.size());

            TextLineReader lineReader = TextLineReader.createUncompressedReader(new ByteArrayInputStream(testData.inputData()), bufferSize);
            assertThat(lineReader.getRetainedSize()).isEqualTo(LINE_READER_INSTANCE_SIZE + sizeOfByteArray(bufferSize));
            lineReader.skipLines(skipLines);
            for (String line : lines.subList(skipLines, lines.size())) {
                assertThat(lineReader.readLine(lineBuffer)).isTrue();
                assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8)).isEqualTo(line);
            }
            assertThat(lineReader.readLine(lineBuffer)).isFalse();
            assertThat(lineBuffer.isEmpty()).isTrue();
            assertThat(lineReader.isClosed()).isTrue();
            assertThat(lineReader.getRetainedSize()).isEqualTo(LINE_READER_INSTANCE_SIZE + sizeOfByteArray(bufferSize));

            if (skipLines == lines.size()) {
                break;
            }
        }
    }

    @SuppressWarnings("unused")
    private record TestData(byte[] inputData, List<ExpectedLine> expectedLines) {}

    @SuppressWarnings("unused")
    private record ExpectedLine(String line, int start, int endExclusive) {}

    private static TestData createInputData(List<String> lines, String delimiter, boolean delimiterAtEndOfFile, boolean bom)
    {
        assertThat(lines).isNotEmpty();

        if (!delimiterAtEndOfFile && lines.size() > 1 && getLast(lines).isEmpty()) {
            // there can not be an empty line at the end of the file unless there is a final delimiter
            lines = lines.subList(0, lines.size() - 1);
        }

        String inputString = join(delimiter, lines);
        if (delimiterAtEndOfFile) {
            inputString += delimiter;
        }
        byte[] inputBytes = inputString.getBytes(UTF_8);
        if (bom) {
            byte[] inputWithBom = new byte[inputBytes.length + 3];
            inputWithBom[0] = (byte) 0xEF;
            inputWithBom[1] = (byte) 0xBB;
            inputWithBom[2] = (byte) 0xBF;
            System.arraycopy(inputBytes, 0, inputWithBom, 3, inputBytes.length);
            inputBytes = inputWithBom;
        }

        ImmutableList.Builder<ExpectedLine> expectedLines = ImmutableList.builder();
        int startPosition = bom ? 3 : 0;
        for (String line : lines) {
            int endPosition = min(inputBytes.length, startPosition + line.length() + delimiter.length());
            expectedLines.add(new ExpectedLine(line, startPosition, endPosition));
            startPosition = endPosition;
        }
        return new TestData(inputBytes, expectedLines.build());
    }

    private static LineBuffer createLineBuffer(List<String> lines)
    {
        int maxLineLength = lines.stream()
                .mapToInt(String::length)
                .sum();
        LineBuffer lineBuffer = new LineBuffer(1, max(1, maxLineLength));
        return lineBuffer;
    }

    private static void randomLine(StringBuilder line, int length)
    {
        line.setLength(0);
        for (int i = 0; i < length; i++) {
            line.append((char) ThreadLocalRandom.current().nextLong(' ', '~'));
        }
    }
}
