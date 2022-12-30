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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergTableName
{
    @Test
    public void testFrom()
    {
        assertFrom("abc", "abc", TableType.DATA);
        assertFrom("abc$data", "abc", TableType.DATA);
        assertFrom("abc$history", "abc", TableType.HISTORY);
        assertFrom("abc$snapshots", "abc", TableType.SNAPSHOTS);

        assertInvalid("abc@123", "Invalid Iceberg table name: abc@123");
        assertInvalid("abc@xyz", "Invalid Iceberg table name: abc@xyz");
        assertInvalid("abc$what", "Invalid Iceberg table name (unknown type 'what'): abc$what");
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
        assertTrue(IcebergTableName.isDataTable("abc$data"));

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
        assertEquals(IcebergTableName.tableTypeFrom("abc$data"), Optional.of(TableType.DATA));
        assertEquals(IcebergTableName.tableTypeFrom("abc$history"), Optional.of(TableType.HISTORY));

        assertEquals(IcebergTableName.tableTypeFrom("abc$invalid"), Optional.empty());
    }

    @Test
    public void testGetTableNameWithType()
    {
        assertEquals(new IcebergTableName("abc", TableType.DATA).getTableNameWithType(), "abc$data");
        assertEquals(new IcebergTableName("abc", TableType.HISTORY).getTableNameWithType(), "abc$history");
    }

    private static void assertInvalid(String inputName, String message)
    {
        assertTrinoExceptionThrownBy(() -> IcebergTableName.from(inputName))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage(message);
    }

    private static void assertFrom(String inputName, String tableName, TableType tableType)
    {
        IcebergTableName name = IcebergTableName.from(inputName);
        assertEquals(name.getTableName(), tableName);
        assertEquals(name.getTableType(), tableType);
    }
}
