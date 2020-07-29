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
package io.prestosql.plugin.jdbc;

import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

public class BaseSqlDialect
        implements SqlDialect
{
    private static final String DEFAULT_IDENTIFIER_QUOTE = "\"";

    private final String identifierQuote;

    public BaseSqlDialect()
    {
        this(DEFAULT_IDENTIFIER_QUOTE);
    }

    public BaseSqlDialect(String identifierQuote)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
    }

    @Override
    public String quote(String identifier)
    {
        return identifierQuote + identifier.replace(identifierQuote, identifierQuote + identifierQuote) + identifierQuote;
    }

    @Override
    public String getRelation(String catalog, String schema, String table)
    {
        StringBuilder sql = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        return sql.append(quote(table)).toString();
    }

    @Override
    public String getProjection(JdbcColumnHandle jdbcColumnHandle)
    {
        return format("%s AS %s", jdbcColumnHandle.toSqlExpression(name -> quote(name)), quote(jdbcColumnHandle.getColumnName()));
    }

    @Override
    public String getPredicate(JdbcColumnHandle column, String operator)
    {
        return quote(column.getColumnName()) + " " + operator + " ?";
    }

    @Override
    public String createTableSql(String catalog, String remoteSchema, String tableName, List<String> columns)
    {
        return format("CREATE TABLE %s (%s)", getRelation(catalog, remoteSchema, tableName), join(", ", columns));
    }

    @Override
    public String renameTable(String catalogName, String schemaName, String tableName, String newSchemaName, String newTableName)
    {
        return format(
                "ALTER TABLE %s RENAME TO %s",
                getRelation(catalogName, schemaName, tableName),
                getRelation(catalogName, newSchemaName, newTableName));
    }
}
