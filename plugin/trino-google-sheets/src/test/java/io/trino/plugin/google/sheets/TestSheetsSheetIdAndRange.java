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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSheetsSheetIdAndRange
{
    private static final String DEFAULT_RANGE = "$1:$100";

    @Test
    public void testExpressionWithoutRange()
    {
        SheetsSheetIdAndRange sheetIdAndRange = new SheetsSheetIdAndRange("sheetId", DEFAULT_RANGE);

        assertThat(sheetIdAndRange.getSheetId()).isEqualTo("sheetId");
        assertThat(sheetIdAndRange.getRange()).isEqualTo(DEFAULT_RANGE);
    }

    @Test
    public void testExpressionWithTabName()
    {
        SheetsSheetIdAndRange sheetIdAndRange = new SheetsSheetIdAndRange("sheetId#Sheet1", DEFAULT_RANGE);

        assertThat(sheetIdAndRange.getSheetId()).isEqualTo("sheetId");
        assertThat(sheetIdAndRange.getRange()).isEqualTo("Sheet1");
    }

    @Test
    public void testExpressionWithTabNameAndCellRange()
    {
        SheetsSheetIdAndRange sheetIdAndRange = new SheetsSheetIdAndRange("sheetId#Sheet1!A1:B4", DEFAULT_RANGE);

        assertThat(sheetIdAndRange.getSheetId()).isEqualTo("sheetId");
        assertThat(sheetIdAndRange.getRange()).isEqualTo("Sheet1!A1:B4");
    }

    @Test
    public void testExpressionWithTrailingSeparator()
    {
        SheetsSheetIdAndRange sheetIdAndRange = new SheetsSheetIdAndRange("sheetId#", DEFAULT_RANGE);

        assertThat(sheetIdAndRange.getSheetId()).isEqualTo("sheetId");
        assertThat(sheetIdAndRange.getRange()).isEqualTo(DEFAULT_RANGE);
    }

    @Test
    public void testNullArguments()
    {
        assertThatThrownBy(() -> new SheetsSheetIdAndRange(null, DEFAULT_RANGE))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("sheetExpression is null");

        assertThatThrownBy(() -> new SheetsSheetIdAndRange("sheetId", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("defaultRange is null");
    }
}
