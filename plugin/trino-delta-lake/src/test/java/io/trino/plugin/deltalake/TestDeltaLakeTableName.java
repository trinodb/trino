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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeTableType.DATA;
import static io.trino.plugin.deltalake.DeltaLakeTableType.HISTORY;
import static io.trino.plugin.deltalake.DeltaLakeTableType.PROPERTIES;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeTableName
{
    @Test
    public void testParse()
    {
        assertParseNameAndType("abc", "abc", DATA);
        assertParseNameAndType("abc$history", "abc", DeltaLakeTableType.HISTORY);
        assertParseNameAndType("abc$properties", "abc", DeltaLakeTableType.PROPERTIES);

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
        assertThat(DeltaLakeTableName.isDataTable("abc")).isTrue();

        assertThat(DeltaLakeTableName.isDataTable("abc$data")).isFalse(); // it's invalid
        assertThat(DeltaLakeTableName.isDataTable("abc$history")).isFalse();
        assertThat(DeltaLakeTableName.isDataTable("abc$invalid")).isFalse();
    }

    @Test
    public void testTableNameFrom()
    {
        assertThat(DeltaLakeTableName.tableNameFrom("abc")).isEqualTo("abc");
        assertThat(DeltaLakeTableName.tableNameFrom("abc$data")).isEqualTo("abc");
        assertThat(DeltaLakeTableName.tableNameFrom("abc$history")).isEqualTo("abc");
        assertThat(DeltaLakeTableName.tableNameFrom("abc$properties")).isEqualTo("abc");
        assertThat(DeltaLakeTableName.tableNameFrom("abc$invalid")).isEqualTo("abc");
    }

    @Test
    public void testTableTypeFrom()
    {
        assertThat(DeltaLakeTableName.tableTypeFrom("abc")).isEqualTo(Optional.of(DATA));
        assertThat(DeltaLakeTableName.tableTypeFrom("abc$data")).isEqualTo(Optional.empty()); // it's invalid
        assertThat(DeltaLakeTableName.tableTypeFrom("abc$history")).isEqualTo(Optional.of(HISTORY));
        assertThat(DeltaLakeTableName.tableTypeFrom("abc$properties")).isEqualTo(Optional.of(PROPERTIES));

        assertThat(DeltaLakeTableName.tableTypeFrom("abc$invalid")).isEqualTo(Optional.empty());
    }

    private static void assertNoValidTableType(String inputName)
    {
        assertThat(DeltaLakeTableName.tableTypeFrom(inputName))
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
        assertThat(DeltaLakeTableName.tableNameFrom(inputName)).isEqualTo(tableName);
        assertThat(DeltaLakeTableName.tableTypeFrom(inputName)).isEqualTo(Optional.of(tableType));
    }
}
