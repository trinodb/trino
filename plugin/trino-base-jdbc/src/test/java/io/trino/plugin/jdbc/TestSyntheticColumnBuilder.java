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
package io.trino.plugin.jdbc;

import com.google.common.base.VerifyException;
import org.testng.annotations.Test;

import java.util.OptionalInt;

import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSyntheticColumnBuilder
{
    private static final SyntheticColumnBuilder FORMATTER = new SyntheticColumnBuilder();
    private static final int MAX_SYNTHETIC_ID_LENGTH = String.valueOf(Integer.MAX_VALUE).length();

    @Test
    public void columnNameShorterThanMaximum()
    {
        int maximumLength = 30;
        String columnName = "a".repeat(maximumLength - 4);

        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName(columnName)
                .build();

        JdbcColumnHandle result = FORMATTER.aliasForColumn(column, OptionalInt.of(maximumLength), 456);
        assertThat(result.getColumnName()).isEqualTo(columnName + "_456");
    }

    @Test
    public void columnNameLongerThanMaximum()
    {
        int maximumLength = 30;
        String columnName = "a".repeat(maximumLength);

        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName(columnName)
                .build();

        JdbcColumnHandle result = FORMATTER.aliasForColumn(column, OptionalInt.of(maximumLength), 123);
        assertThat(result.getColumnName()).isEqualTo("a".repeat(maximumLength - 4) + "_123");
    }

    @Test
    public void syntheticIdEqualsTheMaximum()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("aaaa")
                .build();

        JdbcColumnHandle result = FORMATTER.aliasForColumn(column, OptionalInt.of(MAX_SYNTHETIC_ID_LENGTH), Integer.MAX_VALUE);
        assertThat(result.getColumnName()).isEqualTo(String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void syntheticWithSeparatorIdEqualsTheMaximum()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("aaaa")
                .build();

        JdbcColumnHandle result = FORMATTER.aliasForColumn(column, OptionalInt.of(MAX_SYNTHETIC_ID_LENGTH + 1), Integer.MAX_VALUE);
        assertThat(result.getColumnName()).isEqualTo("_" + Integer.MAX_VALUE);
    }

    @Test
    public void syntheticWithSeparatorSmallerThanMaximum()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("abcd")
                .build();

        JdbcColumnHandle result = FORMATTER.aliasForColumn(column, OptionalInt.of(MAX_SYNTHETIC_ID_LENGTH + 2), Integer.MAX_VALUE);
        assertThat(result.getColumnName()).isEqualTo("a_" + Integer.MAX_VALUE);
    }

    @Test
    public void syntheticIdLongerThanMaximum()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("a")
                .build();

        assertThatThrownBy(() -> FORMATTER.aliasForColumn(column, OptionalInt.of(MAX_SYNTHETIC_ID_LENGTH - 1), Integer.MAX_VALUE))
                .isInstanceOf(VerifyException.class)
                        .hasMessage("Maximum allowed identifier length is 9 but synthetic column has length 10");
    }

    private static JdbcColumnHandle.Builder getDefaultColumnHandleBuilder()
    {
        return JdbcColumnHandle.builder()
                .setJdbcTypeHandle(JDBC_VARCHAR)
                .setColumnType(VARCHAR);
    }
}
