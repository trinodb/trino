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
package io.trino.plugin.doris;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class DorisQueryBuilder
{
    public String buildSelectSql(
            DorisTableHandle tableHandle,
            List<String> columnNames,
            List<Long> tabletIds)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(tabletIds, "tabletIds is null");

        String projection = columnNames.isEmpty()
                ? "1"
                : columnNames.stream()
                  .map(DorisQueryBuilder::quoteIdentifier)
                  .collect(joining(", "));

        StringBuilder sql = new StringBuilder()
                .append("SELECT ")
                .append(projection)
                .append(" FROM ")
                .append(quoteIdentifier(tableHandle.remoteSchemaName()))
                .append(".")
                .append(quoteIdentifier(tableHandle.remoteTableName()));

        if (!tabletIds.isEmpty()) {
            sql.append(" TABLET(")
                    .append(tabletIds.stream()
                            .map(String::valueOf)
                            .collect(joining(",")))
                    .append(")");
        }

        return sql.toString();
    }

    public String buildSplitPlanningSql(DorisTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        return "SELECT * FROM " + quoteIdentifier(tableHandle.remoteSchemaName()) + "." + quoteIdentifier(tableHandle.remoteTableName());
    }

    static String quoteIdentifier(String value)
    {
        requireNonNull(value, "identifier is null");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("identifier is empty");
        }
        return "`" + value.replace("`", "``") + "`";
    }
}
