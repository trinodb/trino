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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.sql.SqlFormatter.formatSql;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlFormatter
{
    @Test
    public void testIdentifiers()
    {
        // Reserved keyword
        assertThat(formatSql(new Identifier("exists", false))).isEqualTo("\"exists\"");
        assertThat(formatSql(new Identifier("exists", true))).isEqualTo("\"exists\"");
        assertThat(formatSql(new Identifier("\"exists\"", true))).isEqualTo("\"\"\"exists\"\"\"");

        // Non-reserved keyword
        assertThat(formatSql(new Identifier("analyze", false))).isEqualTo("analyze");
        assertThat(formatSql(new Identifier("analyze", true))).isEqualTo("\"analyze\"");
        assertThat(formatSql(new Identifier("\"analyze\"", true))).isEqualTo("\"\"\"analyze\"\"\"");

        // ANSI-compliant identifier
        assertThat(formatSql(new Identifier("account", false))).isEqualTo("account");
        assertThat(formatSql(new Identifier("account", true))).isEqualTo("\"account\"");
        assertThat(formatSql(new Identifier("\"account\"", true))).isEqualTo("\"\"\"account\"\"\"");

        // Non-ANSI compliant identifier
        assertThat(formatSql(new Identifier("1", true))).isEqualTo("\"1\"");
        assertThat(formatSql(new Identifier("\"1\"", true))).isEqualTo("\"\"\"1\"\"\"");
    }

    @Test
    public void testCreateTable()
    {
        BiFunction<String, String, CreateTable> createTable = (tableName, columnName) -> {
            NodeLocation location = new NodeLocation(1, 1);
            Identifier type = new Identifier(location, "VARCHAR", false);
            return new CreateTable(
                    QualifiedName.of(ImmutableList.of(new Identifier(tableName, false))),
                    ImmutableList.of(new ColumnDefinition(
                            new Identifier(columnName, false),
                            new GenericDataType(location, type, ImmutableList.of()),
                            true,
                            ImmutableList.of(),
                            Optional.empty())),
                    false,
                    ImmutableList.of(),
                    Optional.empty());
        };
        String createTableSql = "CREATE TABLE %s (\n   %s VARCHAR\n)";

        assertThat(formatSql(createTable.apply("table_name", "column_name")))
                .isEqualTo(String.format(createTableSql, "table_name", "column_name"));
        assertThat(formatSql(createTable.apply("exists", "exists")))
                .isEqualTo(String.format(createTableSql, "\"exists\"", "\"exists\""));
    }
}
