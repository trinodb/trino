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

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestSheetsConnectorTableHandle
{
    private final JsonCodec<SheetsNamedTableHandle> namedCodec = JsonCodec.jsonCodec(SheetsNamedTableHandle.class);
    private final JsonCodec<SheetsSheetTableHandle> sheetCodec = JsonCodec.jsonCodec(SheetsSheetTableHandle.class);

    @Test
    public void testRoundTripWithNamedTable()
    {
        SheetsNamedTableHandle expected = new SheetsNamedTableHandle("schema", "table");

        String json = namedCodec.toJson(expected);
        SheetsNamedTableHandle actual = namedCodec.fromJson(json);

        assertEquals(actual, expected);
    }

    @Test
    public void testRoundTripWithSheetTable()
    {
        SheetsSheetTableHandle expected = new SheetsSheetTableHandle("sheetID", "$1:$10000");

        String json = sheetCodec.toJson(expected);
        SheetsSheetTableHandle actual = sheetCodec.fromJson(json);

        assertEquals(actual, expected);
    }
}
