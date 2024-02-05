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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergTableName
{
    @Test
    public void testParse()
    {
        assertParseNameAndType("abc", "abc", TableType.DATA);
        assertParseNameAndType("abc$history", "abc", TableType.HISTORY);
        assertParseNameAndType("abc$snapshots", "abc", TableType.SNAPSHOTS);

        assertInvalid("abc$data");
        assertInvalid("abc@123");
        assertInvalid("abc@xyz");
        assertInvalid("abc$what");
        assertInvalid("abc@123$data@456");
        assertInvalid("abc@123$snapshots");
        assertInvalid("abc$snapshots@456");
        assertInvalid("xyz$data@456");
        assertInvalid("abc$partitions@456");
        assertInvalid("abc$manifests@456");
    }

    @Test
    public void testIsDataTable()
    {
        assertThat(IcebergTableName.isDataTable("abc")).isTrue();

        assertThatThrownBy(() -> IcebergTableName.isDataTable("abc$data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Iceberg table name: abc$data");

        assertThat(IcebergTableName.isDataTable("abc$history")).isFalse();

        assertThatThrownBy(() -> IcebergTableName.isDataTable("abc$invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Iceberg table name: abc$invalid");
    }

    @Test
    public void testTableNameFrom()
    {
        assertThat(IcebergTableName.tableNameFrom("abc")).isEqualTo("abc");

        assertThatThrownBy(() -> IcebergTableName.tableNameFrom("abc$data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Iceberg table name: abc$data");

        assertThat(IcebergTableName.tableNameFrom("abc$history")).isEqualTo("abc");

        assertThatThrownBy(() -> IcebergTableName.tableNameFrom("abc$invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Iceberg table name: abc$invalid");
    }

    @Test
    public void testTableTypeFrom()
    {
        assertThat(IcebergTableName.tableTypeFrom("abc")).isEqualTo(TableType.DATA);

        assertThatThrownBy(() -> IcebergTableName.tableTypeFrom("abc$data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Iceberg table name: abc$data");

        assertThat(IcebergTableName.tableTypeFrom("abc$history")).isEqualTo(TableType.HISTORY);

        assertThatThrownBy(() -> IcebergTableName.tableTypeFrom("abc$invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Iceberg table name: abc$invalid");
    }

    @Test
    public void testTableNameWithType()
    {
        assertThat(IcebergTableName.tableNameWithType("abc", TableType.DATA)).isEqualTo("abc$data");
        assertThat(IcebergTableName.tableNameWithType("abc", TableType.HISTORY)).isEqualTo("abc$history");
    }

    private static void assertInvalid(String inputName)
    {
        assertThat(IcebergTableName.isIcebergTableName(inputName)).isFalse();

        assertThatThrownBy(() -> IcebergTableName.tableTypeFrom(inputName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Iceberg table name: " + inputName);
    }

    private static void assertParseNameAndType(String inputName, String tableName, TableType tableType)
    {
        assertThat(IcebergTableName.isIcebergTableName(inputName)).isTrue();
        assertThat(IcebergTableName.tableNameFrom(inputName)).isEqualTo(tableName);
        assertThat(IcebergTableName.tableTypeFrom(inputName)).isEqualTo(tableType);
    }
}
