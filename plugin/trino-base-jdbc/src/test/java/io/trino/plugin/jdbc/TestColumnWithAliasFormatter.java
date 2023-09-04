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

import com.google.inject.Inject;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

@Guice(modules = ColumnWithAliasFormatterModule.class)
public class TestColumnWithAliasFormatter
{
    @Inject
    private ColumnWithAliasFormatter actor;

    private final TestingConnectorSession session = TestingConnectorSession.builder().build();

    @Test
    public void testTooLongName()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("column_with_over_twenty_characters")
                .build();

        JdbcColumnHandle result = actor.format(session, column, 100);

        assertThat(result.getColumnName()).isEqualTo("column_with_over_twenty__00100");
    }

    @Test
    public void testTooShortName()
    {
        JdbcColumnHandle column = getDefaultColumnHandleBuilder()
                .setColumnName("column_0")
                .build();

        JdbcColumnHandle result = actor.format(session, column, 999);

        assertThat(result.getColumnName()).isEqualTo("column_0_00999");
    }

    private static JdbcColumnHandle.Builder getDefaultColumnHandleBuilder()
    {
        return JdbcColumnHandle.builder()
                .setJdbcTypeHandle(JDBC_VARCHAR)
                .setColumnType(VARCHAR);
    }
}
