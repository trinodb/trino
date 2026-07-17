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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;

import static io.trino.plugin.google.sheets.TestSheetsPlugin.DATA_SHEET_ID;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
@ResourceLock("google-sheets")
public class TestGoogleSheets
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SheetsQueryRunner.builder()
                .addConnectorProperty("gsheets.metadata-sheet-id", TEST_METADATA_SHEET_ID)
                .addConnectorProperty("gsheets.connection-timeout", "1m")
                .addConnectorProperty("gsheets.read-timeout", "1m")
                .build();
    }

    @Test
    public void testListTable()
    {
        List<String> expectedTableNames = List.of("metadata_table", "number_text", "table_with_duplicate_and_missing_column_names");
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .containsExactlyInAnyOrderElementsOf(expectedTableNames);
        assertThat(computeActual("SELECT table_name FROM gsheets.information_schema.tables WHERE table_schema <> 'information_schema'").getOnlyColumnAsSet())
                .containsExactlyInAnyOrderElementsOf(expectedTableNames);
        assertThat(computeActual("select table_name from gsheets.information_schema.tables WHERE table_schema <> 'information_schema' LIMIT 1000").getOnlyColumnAsSet())
                .containsExactlyInAnyOrderElementsOf(expectedTableNames);

        assertQueryReturnsEmptyResult("SHOW TABLES IN gsheets.information_schema LIKE 'number_text'");
        assertThat(getQueryRunner().execute("SELECT table_name FROM gsheets.information_schema.tables WHERE table_schema = 'unknown_schema'").getRowCount())
                .isEqualTo(0);
    }

    @Test
    public void testDescTable()
    {
        assertQuery("desc number_text", "SELECT * FROM (VALUES('number','varchar','',''), ('text','varchar','',''))");
        assertQuery("desc metadata_table", "SELECT * FROM (VALUES('table name','varchar','',''), ('sheet id','varchar','',''), "
                + "('owner','varchar','',''), ('notes','varchar','',''))");
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM number_text", "SELECT 5");
        assertQuery("SELECT number FROM number_text", "SELECT * FROM (VALUES '1','2','3','4','5')");
        assertQuery("SELECT text FROM number_text", "SELECT * FROM (VALUES 'one','two','three','four','five')");
        assertQuery("SELECT * FROM number_text", "SELECT * FROM (VALUES ('1','one'), ('2','two'), ('3','three'), ('4','four'), ('5','five'))");
    }

    @Test
    public void testSelectFromTableIgnoreCase()
    {
        assertQuery("SELECT count(*) FROM NUMBER_TEXT", "SELECT 5");
        assertQuery("SELECT number FROM Number_Text", "SELECT * FROM (VALUES '1','2','3','4','5')");
    }

    @Test
    public void testQueryingUnknownSchemaAndTable()
    {
        assertQueryFails("select * from gsheets.foo.bar", "line 1:15: Schema 'foo' does not exist");
        assertQueryFails("select * from gsheets.default.foo_bar_table", "Sheet expression not found for table foo_bar_table");
    }

    @Test
    public void testTableWithRepeatedAndMissingColumnNames()
    {
        assertQuery("desc table_with_duplicate_and_missing_column_names", "SELECT * FROM (VALUES('a','varchar','','')," +
                " ('column_1','varchar','',''), ('column_2','varchar','',''), ('c','varchar','',''))");
    }

    @Test
    public void testSheetQuerySimple()
    {
        assertQuery(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s'))".formatted(DATA_SHEET_ID),
                "VALUES " +
                        "('1', 'one')," +
                        "('2', 'two')," +
                        "('3', 'three')," +
                        "('4', 'four')," +
                        "('5', 'five')");
    }

    @Test
    public void testSheetQueryFilter()
    {
        assertQuery(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s'))".formatted(DATA_SHEET_ID) +
                        "WHERE number = '1' and text = 'one'",
                "VALUES " +
                        "('1', 'one')");
    }

    @Test
    public void testSheetQueryWithSheet()
    {
        assertQuery(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s', range => '%s'))".formatted(DATA_SHEET_ID, "number_text"),
                "VALUES " +
                        "('1', 'one')," +
                        "('2', 'two')," +
                        "('3', 'three')," +
                        "('4', 'four')," +
                        "('5', 'five')");
    }

    @Test
    public void testSheetQueryWithSheetAndRangeWithoutHeader()
    {
        // The range skips the header row, the first row of the range is treated as a header
        assertQuery(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s', range => '%s'))".formatted(DATA_SHEET_ID, "number_text!A2:B6") +
                        "WHERE \"1\" = \"1\" and \"one\" = \"one\"",
                "VALUES " +
                        "('2', 'two')," +
                        "('3', 'three')," +
                        "('4', 'four')," +
                        "('5', 'five')");
    }

    @Test
    public void testSheetQueryWithSheetAndRowRange()
    {
        assertQuery(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s', range => '%s'))".formatted(DATA_SHEET_ID, "number_text!A1:B4") +
                        "WHERE number = number and text = text",
                "VALUES " +
                        "('1', 'one')," +
                        "('2', 'two')," +
                        "('3', 'three')");
    }

    @Test
    public void testSheetQueryWithSheetAndColumnRange()
    {
        assertQuery(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s', range => '%s'))".formatted(DATA_SHEET_ID, "number_text!A1:A6") +
                        "WHERE number = number",
                "VALUES " +
                        "('1')," +
                        "('2')," +
                        "('3')," +
                        "('4')," +
                        "('5')");
    }

    @Test
    public void testSheetQueryWithSheetAndRowAndColumnRange()
    {
        assertQuery(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s', range => '%s'))".formatted(DATA_SHEET_ID, "number_text!B3:B5") +
                        "WHERE \"two\" = \"two\"",
                "VALUES " +
                        "('three')," +
                        "('four')");
    }

    @Test
    public void testSheetQueryWithSheetRangeInIdFails()
    {
        // Sheet ids with "#" are explicitly forbidden since "#" is the sheet separator
        assertThat(query(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s#%s'))".formatted(DATA_SHEET_ID, "number_text")))
                .failure().hasMessageContaining("Google sheet ID %s cannot contain '#'. Provide a range through the 'range' argument.".formatted(DATA_SHEET_ID + "#number_text"));

        // Attempting to put a sheet range in the id fails since the sheet id is invalid
        assertThat(query(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s%s'))".formatted(DATA_SHEET_ID, "number_text")))
                .failure().hasMessageContaining("Failed reading data from sheet: %snumber_text#$1:$10000".formatted(DATA_SHEET_ID));
    }

    @Test
    public void testSheetQueryWithNoDataInRangeFails()
    {
        assertThat(query(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s', range => '%s'))".formatted(DATA_SHEET_ID, "number_text!D1:D1")))
                .failure().hasMessageContaining("No non-empty cells found in sheet: %s#number_text!D1:D1".formatted(DATA_SHEET_ID));

        assertThat(query(
                "SELECT * FROM TABLE(gsheets.system.sheet(id => '%s', range => '%s'))".formatted(DATA_SHEET_ID, "number_text!D12:E13")))
                .failure().hasMessageContaining("No non-empty cells found in sheet: %s#number_text!D12:E13".formatted(DATA_SHEET_ID));
    }

    @Test
    public void testSheetQueryWithInvalidSheetId()
    {
        assertThat(query("SELECT * FROM TABLE(gsheets.system.sheet(id => 'DOESNOTEXIST'))"))
                .failure().hasMessageContaining("Failed reading data from sheet: DOESNOTEXIST");
    }
}
