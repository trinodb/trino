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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(IcebergTableName.isDataTable("abc")).isTrue();

        assertThat(IcebergTableName.isDataTable("abc$data")).isFalse(); // it's invalid
        assertThat(IcebergTableName.isDataTable("abc$history")).isFalse();
        assertThat(IcebergTableName.isDataTable("abc$invalid")).isFalse();
    }

    @Test
    public void testTableNameFrom()
    {
        assertThat(IcebergTableName.tableNameFrom("abc")).isEqualTo("abc");
        assertThat(IcebergTableName.tableNameFrom("abc$data")).isEqualTo("abc");
        assertThat(IcebergTableName.tableNameFrom("abc$history")).isEqualTo("abc");
        assertThat(IcebergTableName.tableNameFrom("abc$invalid")).isEqualTo("abc");
    }

    @Test
    public void testTableTypeFrom()
    {
        assertThat(IcebergTableName.tableTypeFrom("abc")).isEqualTo(Optional.of(TableType.DATA));
        assertThat(IcebergTableName.tableTypeFrom("abc$data")).isEqualTo(Optional.empty()); // it's invalid
        assertThat(IcebergTableName.tableTypeFrom("abc$history")).isEqualTo(Optional.of(TableType.HISTORY));

        assertThat(IcebergTableName.tableTypeFrom("abc$invalid")).isEqualTo(Optional.empty());
    }

    @Test
    public void testTableNameWithType()
    {
        assertThat(IcebergTableName.tableNameWithType("abc", TableType.DATA)).isEqualTo("abc$data");
        assertThat(IcebergTableName.tableNameWithType("abc", TableType.HISTORY)).isEqualTo("abc$history");
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
        assertThat(IcebergTableName.tableNameFrom(inputName)).isEqualTo(tableName);
        assertThat(IcebergTableName.tableTypeFrom(inputName)).isEqualTo(Optional.of(tableType));
    }
}
