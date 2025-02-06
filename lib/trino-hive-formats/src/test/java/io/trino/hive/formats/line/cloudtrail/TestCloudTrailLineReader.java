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
package io.trino.hive.formats.line.cloudtrail;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCloudTrailLineReader
{
    @Test
    public void testBasicRowParsing()
            throws IOException
    {
        String data = "{\"Records\":[" +
                // try different spacing, should all work
                "{ \"data\":\"1\" }," +
                "{\"data\":\"2\" }," +
                "{\"data\":  \"3\"}" +
                "]}";

        testAllValues(data, ImmutableList.of("{\"data\":\"1\"}", "{\"data\":\"2\"}", "{\"data\":\"3\"}"));
    }

    @Test
    public void testSkipMalformedDocument()
            throws IOException
    {
        String data = "{randomcontent}";

        testAllValues(data, ImmutableList.of());
    }

    @Test
    public void testCloudTrailTransform()
            throws IOException
    {
        String data = "{\"Records\":[" +
                // try different spacing, should all work
                "{ \"requestParameters\": {\"data\": \"1\"} }," +
                "{ \"responseElements\": {\"data\": \"2\"} }," +
                "{ \"additionalEventData\": {\"data\": \"3\"} }," +
                "{ \"requestParameters\": {\"data\": \"4\"}, \"additionalEventData\": {\"data\": \"5\"} }" +
                "]}";

        testAllValues(data, ImmutableList.of("{\"requestParameters\":\"{\\\"data\\\":\\\"1\\\"}\"}",
                "{\"responseElements\":\"{\\\"data\\\":\\\"2\\\"}\"}",
                "{\"additionalEventData\":\"{\\\"data\\\":\\\"3\\\"}\"}",
                "{\"requestParameters\":\"{\\\"data\\\":\\\"4\\\"}\",\"additionalEventData\":\"{\\\"data\\\":\\\"5\\\"}\"}"));
    }

    @Test
    public void testCloudTrailAsStringColumn()
            throws IOException
    {
        String data = "{\"Records\":[" +
                // try different spacing, should all work
                "{ \"userIdentity\": {\"data\": \"1\"} }" +
                "]}";

        testAllValues(data, ImmutableList.of("{\"userIdentity\":{\"data\":\"1\"},\"userIdentity_as_string\":\"{\\\"data\\\":\\\"1\\\"}\"}"));
    }

    private static void testAllValues(String data, List<String> expectedValues)
            throws IOException
    {
        CloudTrailLineReaderFactory readerFactory = new CloudTrailLineReaderFactory(1024, 1024, 8096);
        TrinoInputFile file = new MemoryInputFile(Location.of("memory:///test"), utf8Slice(data));
        LineReader lineReader = readerFactory.createLineReader(file, 0, 1, 0, 0);
        LineBuffer lineBuffer = new LineBuffer(1, 1024);
        for (String expectedValue : expectedValues) {
            lineReader.readLine(lineBuffer);
            assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), StandardCharsets.UTF_8)).isEqualTo(expectedValue);
        }
        assertThat(lineReader.readLine(lineBuffer)).isFalse();
    }
}
