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

import static io.trino.plugin.google.sheets.SheetsClient.RANGE_SEPARATOR;
import static java.util.Objects.requireNonNull;

/**
 * Splits a {@code <sheet id>#<range>} expression, as stored in the metadata sheet.
 * <p>
 * The range is optional, and the caller supplies the one to fall back to, because reading and appending
 * need different defaults: a read is bounded by {@code gsheets.max-rows}, while an append uses the range
 * only to select the table to append after, and so keeps its own fixed range.
 */
public class SheetsSheetIdAndRange
{
    private final String sheetId;
    private final String range;

    public SheetsSheetIdAndRange(String sheetExpression, String defaultRange)
    {
        requireNonNull(sheetExpression, "sheetExpression is null");
        requireNonNull(defaultRange, "defaultRange is null");

        String[] tableOptions = sheetExpression.split(RANGE_SEPARATOR);
        this.sheetId = tableOptions[0];
        if (tableOptions.length > 1) {
            this.range = tableOptions[1];
        }
        else {
            this.range = defaultRange;
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
