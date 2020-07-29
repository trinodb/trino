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

import javax.annotation.Nullable;

import java.util.List;

public interface SqlDialect
{
    String quote(String identifier);

    String getRelation(@Nullable String catalog, @Nullable String schema, String table);

    String getProjection(JdbcColumnHandle jdbcColumnHandle);

    String getPredicate(JdbcColumnHandle column, String operator);

    String createTableSql(String catalog, String remoteSchema, String tableName, List<String> columns);

    String renameTable(String catalogName, String schemaName, String tableName, String newSchemaName, String newTableName);
}
