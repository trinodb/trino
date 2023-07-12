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

import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.trueFalse;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTextLineReaderFactory
{
    @Test(dataProvider = "testSkipHeaderFooterDataProvider")
    public void testSkipHeaderFooter(boolean header, boolean footer)
            throws Exception
    {
        TextLineReaderFactory lineReaderFactory = new TextLineReaderFactory(1024, 1024, 8096);
        StringBuilder contentBuilder = new StringBuilder();
        if (header) {
            contentBuilder.append("header\n");
        }
        int linesCount = 100;
        IntStream.rangeClosed(1, linesCount).forEach(index -> contentBuilder.append("line" + index + "\n"));
        if (footer) {
            contentBuilder.append("footer\n");
        }

        byte[] content = contentBuilder.toString().getBytes(UTF_8);
        TrinoInputFile inputFile = getMemoryInputFile(content);

        LineReader readerSplit1 = lineReaderFactory.createLineReader(inputFile, 0, content.length / 3, header ? 1 : 0, footer ? 1 : 0);
        LineReader readerSplit2 = lineReaderFactory.createLineReader(inputFile, content.length / 3, content.length / 3, header ? 1 : 0, footer ? 1 : 0);
        LineReader readerSplit3 = lineReaderFactory.createLineReader(inputFile, content.length / 3 + content.length / 3, content.length - (content.length / 3 + content.length / 3), header ? 1 : 0, footer ? 1 : 0);

        int linesRead = 0;
        for (LineReader reader : List.of(readerSplit1, readerSplit2, readerSplit3)) {
            LineBuffer lineBuffer = lineReaderFactory.createLineBuffer();
            while (reader.readLine(lineBuffer)) {
                linesRead++;
            }
        }

        assertThat(linesRead).isEqualTo(linesCount);
    }

    @DataProvider
    public Object[][] testSkipHeaderFooterDataProvider()
    {
        return cartesianProduct(trueFalse(), trueFalse());
    }

    private static TrinoInputFile getMemoryInputFile(byte[] bytes)
    {
        return new MemoryInputFile(Location.of("memory:///test"), Slices.wrappedBuffer(bytes));
    }
}
