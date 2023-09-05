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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Integer.MAX_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSyntheticColumnHandleBuilder
{
    private final SyntheticColumnHandleBuilder syntheticColumnHandleBuilder = new SyntheticColumnHandleBuilder();

    @DataProvider(name = "columns")
    public static Object[][] testData()
    {
        return new Object[][] {
                {"column_0", 999, "column_0_999"},
                {"column_with_over_twenty_characters", 100, "column_with_over_twenty_ch_100"},
                {"column_with_over_twenty_characters", MAX_VALUE, "column_with_over_tw_2147483647"}
        };
    }

    @Test(dataProvider = "columns")
    public void testColumnAliasTruncation(String columnName, int nextSynthenticId, String expectedSyntheticColumnName)
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName(columnName)
                .build();

        JdbcColumnHandle result = syntheticColumnHandleBuilder.get(column, nextSynthenticId);

        assertThat(result.getColumnName()).isEqualTo(expectedSyntheticColumnName);
    }

    @Test
    public void testNegativeSyntheticId()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("column_0")
                .build();

        assertThatThrownBy(() -> syntheticColumnHandleBuilder.get(column, -2147483648)).isInstanceOf(VerifyException.class);
    }

    private static JdbcColumnHandle.Builder getDefaultColumnHandleBuilder()
    {
        return JdbcColumnHandle.builder()
                .setJdbcTypeHandle(JDBC_VARCHAR)
                .setColumnType(VARCHAR);
    }
}
