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
package io.trino.hive.formats.line;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFooterAwareLineReader
{
    @Test
    public void test()
            throws IOException
    {
        assertThat(readAllValues(new TestingLineReader(10))).containsExactly(10, 9, 8, 7, 6, 5, 4, 3, 2, 1);
        testFooterRead(10, 1, ImmutableList.of(10, 9, 8, 7, 6, 5, 4, 3, 2));
        testFooterRead(10, 2, ImmutableList.of(10, 9, 8, 7, 6, 5, 4, 3));
        testFooterRead(10, 8, ImmutableList.of(10, 9));
        testFooterRead(10, 9, ImmutableList.of(10));
        testFooterRead(10, 10, ImmutableList.of());
        testFooterRead(10, 11, ImmutableList.of());
        testFooterRead(10, 100, ImmutableList.of());

        testFooterRead(0, 1, ImmutableList.of());
        testFooterRead(0, 2, ImmutableList.of());
    }

    private static void testFooterRead(int fileLines, int footerCount, List<Integer> expectedValues)
            throws IOException
    {
        FooterAwareLineReader reader = new FooterAwareLineReader(new TestingLineReader(fileLines), footerCount, TestFooterAwareLineReader::createLineBuffer);
        assertThat(ImmutableList.of(readAllValues(reader))).containsExactly(expectedValues);
        assertThat(reader.readLine(createLineBuffer())).isFalse();
    }

    private static List<Integer> readAllValues(LineReader reader)
            throws IOException
    {
        LineBuffer lineBuffer = createLineBuffer();
        List<Integer> values = new ArrayList<>();
        while (true) {
            if (!reader.readLine(lineBuffer)) {
                return values;
            }
            String value = new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8);
            values.add(Integer.parseInt(value));
        }
    }

    private static class TestingLineReader
            implements LineReader
    {
        private int linesRemaining;

        public TestingLineReader(int fileLines)
        {
            checkArgument(fileLines >= 0, "fileLines is negative");
            this.linesRemaining = fileLines;
        }

        @Override
        public boolean isClosed()
        {
            return linesRemaining == 0;
        }

        @Override
        public long getRetainedSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getBytesRead()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getReadTimeNanos()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean readLine(LineBuffer lineBuffer)
                throws IOException
        {
            if (linesRemaining == 0) {
                return false;
            }
            lineBuffer.reset();
            lineBuffer.write(String.valueOf(linesRemaining).getBytes(UTF_8));
            linesRemaining--;
            return true;
        }

        @Override
        public void close()
                throws IOException
        {
            linesRemaining = 0;
        }
    }

    private static LineBuffer createLineBuffer()
    {
        return new LineBuffer(20, 20);
    }
}
