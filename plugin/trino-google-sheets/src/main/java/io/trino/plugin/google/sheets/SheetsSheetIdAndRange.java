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

public class SheetsSheetIdAndRange
{
    // By default, loading up to 10k rows from the first tab of the sheet
    private static final String DEFAULT_RANGE = "$1:$10000";
    private static final String DELIMITER_HASH = "#";

    private final String sheetId;
    private final String range;

    public SheetsSheetIdAndRange(String sheetExpression)
    {
        String[] tableOptions = sheetExpression.split(DELIMITER_HASH);
        this.sheetId = tableOptions[0];
        if (tableOptions.length > 1) {
            this.range = tableOptions[1];
        }
        else {
            this.range = DEFAULT_RANGE;
        }
    }

    public String getSheetId()
    {
        return sheetId;
    }

    public String getRange()
    {
        return range;
    }
}
