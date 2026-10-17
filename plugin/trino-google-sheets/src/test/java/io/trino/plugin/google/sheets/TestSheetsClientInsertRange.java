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
package io.trino.plugin.google.sheets;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.sheets.v4.Sheets;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.google.sheets.SheetsClient.APPEND_TABLE_SEARCH_RANGE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The range passed to {@code spreadsheets.values.append} selects the table to append after, it is not a
 * row limit. These tests pin the range each expression appends with, so that the read limit
 * ({@code gsheets.max-rows}) cannot leak into the append path and retarget existing inserts.
 */
public class TestSheetsClientInsertRange
{
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final List<List<Object>> ROWS = ImmutableList.of(ImmutableList.of("a", "b"));

    @Test
    public void testAppendSearchRangeIsUnchanged()
    {
        // Pinned on purpose: the value decides which block an insert lands in on a sheet holding more than
        // one, so it must not be swapped for a "tidier" range without re-checking the append target.
        assertThat(APPEND_TABLE_SEARCH_RANGE).isEqualTo("$1:$10000");
    }

    @Test
    public void testExpressionWithoutRangeAppendsWithSearchRange()
    {
        RecordingTransport transport = new RecordingTransport();
        newClient(transport).insertIntoSheet("sheetId", ROWS);

        assertThat(transport.appendedRange()).isEqualTo(APPEND_TABLE_SEARCH_RANGE);
    }

    @Test
    public void testExpressionWithTabNameAppendsToThatTab()
    {
        RecordingTransport transport = new RecordingTransport();
        newClient(transport).insertIntoSheet("sheetId#Sheet1", ROWS);

        assertThat(transport.appendedRange()).isEqualTo("Sheet1");
    }

    @Test
    public void testAppendRangeIsIndependentOfMaxRows()
    {
        RecordingTransport withLimit = new RecordingTransport();
        newClient(withLimit, 10).insertIntoSheet("sheetId", ROWS);

        RecordingTransport withoutLimit = new RecordingTransport();
        newClient(withoutLimit, 0).insertIntoSheet("sheetId", ROWS);

        assertThat(withLimit.appendedRange()).isEqualTo(APPEND_TABLE_SEARCH_RANGE);
        assertThat(withoutLimit.appendedRange()).isEqualTo(APPEND_TABLE_SEARCH_RANGE);
    }

    private static SheetsClient newClient(RecordingTransport transport)
    {
        return newClient(transport, 10_000);
    }

    private static SheetsClient newClient(RecordingTransport transport, int maxRows)
    {
        Sheets sheetsService = new Sheets.Builder(transport, JSON_FACTORY, null)
                .setApplicationName("test")
                .build();
        return new SheetsClient(new SheetsConfig().setMaxRows(maxRows), sheetsService);
    }

    private static final class RecordingTransport
            extends MockHttpTransport
    {
        private Optional<String> appendUrl = Optional.empty();

        @Override
        public LowLevelHttpRequest buildRequest(String method, String url)
        {
            if (url.contains(":append")) {
                appendUrl = Optional.of(url);
            }
            MockLowLevelHttpRequest request = new MockLowLevelHttpRequest(url);
            request.setResponse(new MockLowLevelHttpResponse()
                    .setStatusCode(200)
                    .setContentType("application/json")
                    .setContent("{}"));
            return request;
        }

        /**
         * Reads the range back out of {@code /v4/spreadsheets/<id>/values/<range>:append}.
         */
        public String appendedRange()
        {
            String path = URI.create(appendUrl.orElseThrow(() -> new IllegalStateException("no append request was made"))).getPath();
            String afterValues = path.substring(path.indexOf("/values/") + "/values/".length());
            return afterValues.substring(0, afterValues.lastIndexOf(":append"));
        }
    }
}
