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
package io.trino.plugin.deltalake;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeTableType.DATA;
import static io.trino.plugin.deltalake.DeltaLakeTableType.HISTORY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeltaLakeTableName
{
    @Test
    public void testParse()
    {
        assertParseNameAndType("abc", "abc", DATA);
        assertParseNameAndType("abc$history", "abc", DeltaLakeTableType.HISTORY);

        assertNoValidTableType("abc$data");
        assertInvalid("abc@123", "Invalid Delta Lake table name: abc@123");
        assertInvalid("abc@xyz", "Invalid Delta Lake table name: abc@xyz");
        assertNoValidTableType("abc$what");
        assertInvalid("abc@123$data@456", "Invalid Delta Lake table name: abc@123$data@456");
        assertInvalid("xyz$data@456", "Invalid Delta Lake table name: xyz$data@456");
    }

    @Test
    public void testIsDataTable()
    {
        assertTrue(DeltaLakeTableName.isDataTable("abc"));

        assertFalse(DeltaLakeTableName.isDataTable("abc$data")); // it's invalid
        assertFalse(DeltaLakeTableName.isDataTable("abc$history"));
        assertFalse(DeltaLakeTableName.isDataTable("abc$invalid"));
    }

    @Test
    public void testTableNameFrom()
    {
        assertEquals(DeltaLakeTableName.tableNameFrom("abc"), "abc");
        assertEquals(DeltaLakeTableName.tableNameFrom("abc$data"), "abc");
        assertEquals(DeltaLakeTableName.tableNameFrom("abc$history"), "abc");
        assertEquals(DeltaLakeTableName.tableNameFrom("abc$invalid"), "abc");
    }

    @Test
    public void testTableTypeFrom()
    {
        assertEquals(DeltaLakeTableName.tableTypeFrom("abc"), Optional.of(DATA));
        assertEquals(DeltaLakeTableName.tableTypeFrom("abc$data"), Optional.empty()); // it's invalid
        assertEquals(DeltaLakeTableName.tableTypeFrom("abc$history"), Optional.of(HISTORY));

        assertEquals(DeltaLakeTableName.tableTypeFrom("abc$invalid"), Optional.empty());
    }

    @Test
    public void testTableNameWithType()
    {
        assertEquals(DeltaLakeTableName.tableNameWithType("abc", DATA), "abc$data");
        assertEquals(DeltaLakeTableName.tableNameWithType("abc", HISTORY), "abc$history");
    }

    private static void assertNoValidTableType(String inputName)
    {
        Assertions.assertThat(DeltaLakeTableName.tableTypeFrom(inputName))
                .isEmpty();
    }

    private static void assertInvalid(String inputName, String message)
    {
        assertTrinoExceptionThrownBy(() -> DeltaLakeTableName.tableTypeFrom(inputName))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage(message);
    }

    private static void assertParseNameAndType(String inputName, String tableName, DeltaLakeTableType tableType)
    {
        assertEquals(DeltaLakeTableName.tableNameFrom(inputName), tableName);
        assertEquals(DeltaLakeTableName.tableTypeFrom(inputName), Optional.of(tableType));
    }
}
