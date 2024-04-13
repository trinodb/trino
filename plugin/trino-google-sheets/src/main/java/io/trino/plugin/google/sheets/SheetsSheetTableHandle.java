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

import com.fasterxml.jackson.annotation.JsonIgnore;

import static io.trino.plugin.google.sheets.SheetsClient.RANGE_SEPARATOR;
import static java.util.Objects.requireNonNull;

public record SheetsSheetTableHandle(
        String sheetId,
        String sheetRange)
        implements SheetsConnectorTableHandle
{
    public SheetsSheetTableHandle
    {
        requireNonNull(sheetId, "sheetId is null");
        requireNonNull(sheetRange, "sheetRange is null");
    }

    @JsonIgnore
    public String getSheetExpression()
    {
        return "%s%s%s".formatted(sheetId, RANGE_SEPARATOR, sheetRange);
    }
}
