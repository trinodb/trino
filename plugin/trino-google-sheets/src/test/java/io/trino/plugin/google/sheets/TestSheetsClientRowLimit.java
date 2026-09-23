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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.google.sheets.SheetsClient.defaultRange;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_EXCEEDED_ROW_LIMIT;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSheetsClientRowLimit
{
    private static final String SHEET_ID = "sheetId";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    @Test
    public void testDefaultRange()
    {
        assertThat(defaultRange(OptionalInt.of(10_000))).isEqualTo("$1:$10001");
        assertThat(defaultRange(OptionalInt.of(1))).isEqualTo("$1:$2");
        assertThat(defaultRange(OptionalInt.of(Integer.MAX_VALUE))).isEqualTo("$1:$2147483648");
        assertThat(defaultRange(OptionalInt.empty())).isEqualTo("A:ZZZ");
    }

    @Test
    public void testSheetWithinLimit()
    {
        TestingSheetsTransport transport = new TestingSheetsTransport(10);
        SheetsClient client = createClient(transport, 10);

        assertThat(client.readAllValuesFromSheet(SHEET_ID)).hasSize(10);
        assertThat(transport.requestedRanges()).containsExactly("$1:$11");
    }

    @Test
    public void testSheetExceedingLimitFails()
    {
        TestingSheetsTransport transport = new TestingSheetsTransport(12);
        SheetsClient client = createClient(transport, 10);

        assertThatThrownBy(() -> client.readAllValuesFromSheet(SHEET_ID))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(SHEETS_EXCEEDED_ROW_LIMIT.toErrorCode()))
                .hasMessage("Sheet sheetId has more than 10 rows. Specify a range with fewer rows or increase 'gsheets.max-rows'");
        // Only one row beyond the limit is requested to detect the overflow
        assertThat(transport.requestedRanges()).containsExactly("$1:$11");
    }

    @Test
    public void testExplicitRangeWithinLimit()
    {
        TestingSheetsTransport transport = new TestingSheetsTransport(12);
        SheetsClient client = createClient(transport, 20);

        assertThat(client.readAllValuesFromSheet(SHEET_ID + "#Sheet1!A1:B12")).hasSize(12);
        // Explicit ranges are passed to the API unchanged
        assertThat(transport.requestedRanges()).containsExactly("Sheet1!A1:B12");
    }

    @Test
    public void testExplicitRangeExceedingLimitFails()
    {
        TestingSheetsTransport transport = new TestingSheetsTransport(12);
        SheetsClient client = createClient(transport, 10);

        assertThatThrownBy(() -> client.readAllValuesFromSheet(SHEET_ID + "#Sheet1"))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(SHEETS_EXCEEDED_ROW_LIMIT.toErrorCode()))
                .hasMessageContaining("Sheet sheetId#Sheet1 has more than 10 rows");
        assertThat(transport.requestedRanges()).containsExactly("Sheet1");
    }

    @Test
    public void testDisabledLimit()
    {
        TestingSheetsTransport transport = new TestingSheetsTransport(12);
        SheetsClient client = createClient(transport, 0);

        assertThat(client.readAllValuesFromSheet(SHEET_ID)).hasSize(12);
        assertThat(client.readAllValuesFromSheet(SHEET_ID + "#Sheet1")).hasSize(12);
        assertThat(transport.requestedRanges()).containsExactly("A:ZZZ", "Sheet1");
    }

    private static SheetsClient createClient(TestingSheetsTransport transport, int maxRows)
    {
        Sheets sheetsService = new Sheets.Builder(transport, JSON_FACTORY, null)
                .setApplicationName("test")
                .build();
        return new SheetsClient(new SheetsConfig().setMaxRows(maxRows), sheetsService);
    }

    /**
     * Serves a single sheet with a header row followed by data rows. Like the Sheets API, it returns
     * no more rows than the requested range covers.
     */
    private static final class TestingSheetsTransport
            extends MockHttpTransport
    {
        private static final Pattern ROW_ONLY_RANGE = Pattern.compile("^\\$?\\d+:\\$?(\\d+)$");

        private final int rowCount;
        private final List<String> requestedRanges = new ArrayList<>();

        private TestingSheetsTransport(int rowCount)
        {
            this.rowCount = rowCount;
        }

        @Override
        public LowLevelHttpRequest buildRequest(String method, String url)
        {
            // GET https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{range}
            List<String> pathParts = new GenericUrl(url).getPathParts();
            String range = pathParts.getLast();
            requestedRanges.add(range);

            int rows = rowCount;
            Matcher matcher = ROW_ONLY_RANGE.matcher(range);
            if (matcher.matches()) {
                rows = min(rows, parseInt(matcher.group(1)));
            }

            MockLowLevelHttpRequest request = new MockLowLevelHttpRequest(url);
            request.setResponse(new MockLowLevelHttpResponse()
                    .setStatusCode(200)
                    .setContentType("application/json")
                    .setContent(valueRangeJson(rows)));
            return request;
        }

        public List<String> requestedRanges()
        {
            return ImmutableList.copyOf(requestedRanges);
        }

        private static String valueRangeJson(int rows)
        {
            ImmutableList.Builder<List<Object>> values = ImmutableList.builder();
            values.add(ImmutableList.of("id", "name"));
            for (int row = 1; row < rows; row++) {
                values.add(ImmutableList.of(String.valueOf(row), "name " + row));
            }
            try {
                return JSON_FACTORY.toString(new ValueRange()
                        .setRange("Sheet1!A1:B" + rows)
                        .setValues(values.build()));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
