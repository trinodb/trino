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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.google.sheets.SheetsQueryRunner.createSheetsQueryRunner;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.DATA_SHEET_ID;

public class TestGoogleSheetsWithoutMetadataSheetId
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createSheetsQueryRunner(ImmutableMap.of(), ImmutableMap.of("gsheets.read-timeout", "1m"));
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
}
