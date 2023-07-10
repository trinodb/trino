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
package io.trino.plugin.iceberg;

import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergTableName
{
    @Test
    public void testParse()
    {
        assertParseNameAndType("abc", "abc", TableType.DATA);
        assertParseNameAndType("abc$history", "abc", TableType.HISTORY);
        assertParseNameAndType("abc$snapshots", "abc", TableType.SNAPSHOTS);

        assertNoValidTableType("abc$data");
        assertInvalid("abc@123", "Invalid Iceberg table name: abc@123");
        assertInvalid("abc@xyz", "Invalid Iceberg table name: abc@xyz");
        assertNoValidTableType("abc$what");
        assertInvalid("abc@123$data@456", "Invalid Iceberg table name: abc@123$data@456");
        assertInvalid("abc@123$snapshots", "Invalid Iceberg table name: abc@123$snapshots");
        assertInvalid("abc$snapshots@456", "Invalid Iceberg table name: abc$snapshots@456");
        assertInvalid("xyz$data@456", "Invalid Iceberg table name: xyz$data@456");
        assertInvalid("abc$partitions@456", "Invalid Iceberg table name: abc$partitions@456");
        assertInvalid("abc$manifests@456", "Invalid Iceberg table name: abc$manifests@456");
    }

    @Test
    public void testIsDataTable()
    {
        assertTrue(IcebergTableName.isDataTable("abc"));

        assertFalse(IcebergTableName.isDataTable("abc$data")); // it's invalid
        assertFalse(IcebergTableName.isDataTable("abc$history"));
        assertFalse(IcebergTableName.isDataTable("abc$invalid"));
    }

    @Test
    public void testTableNameFrom()
    {
        assertEquals(IcebergTableName.tableNameFrom("abc"), "abc");
        assertEquals(IcebergTableName.tableNameFrom("abc$data"), "abc");
        assertEquals(IcebergTableName.tableNameFrom("abc$history"), "abc");
        assertEquals(IcebergTableName.tableNameFrom("abc$invalid"), "abc");
    }

    @Test
    public void testTableTypeFrom()
    {
        assertEquals(IcebergTableName.tableTypeFrom("abc"), Optional.of(TableType.DATA));
        assertEquals(IcebergTableName.tableTypeFrom("abc$data"), Optional.empty()); // it's invalid
        assertEquals(IcebergTableName.tableTypeFrom("abc$history"), Optional.of(TableType.HISTORY));

        assertEquals(IcebergTableName.tableTypeFrom("abc$invalid"), Optional.empty());
    }

    @Test
    public void testTableNameWithType()
    {
        assertEquals(IcebergTableName.tableNameWithType("abc", TableType.DATA), "abc$data");
        assertEquals(IcebergTableName.tableNameWithType("abc", TableType.HISTORY), "abc$history");
    }

    private static void assertInvalid(String inputName, String message)
    {
        assertTrinoExceptionThrownBy(() -> IcebergTableName.tableTypeFrom(inputName))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage(message);
    }

    private static void assertNoValidTableType(String inputName)
    {
        assertThat(IcebergTableName.tableTypeFrom(inputName))
                .isEmpty();
    }

    private static void assertParseNameAndType(String inputName, String tableName, TableType tableType)
    {
        assertEquals(IcebergTableName.tableNameFrom(inputName), tableName);
        assertEquals(IcebergTableName.tableTypeFrom(inputName), Optional.of(tableType));
    }
}
