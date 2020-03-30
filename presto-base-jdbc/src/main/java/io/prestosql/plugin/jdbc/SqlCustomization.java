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
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Encapsulates strategies to use for various parts of SQL generation in {@link QueryBuilder}.
 */
public interface SqlCustomization
{
    String quote(String name);

    default List<String> columnNames(List<JdbcColumnHandle> handles)
    {
        return handles.stream()
                .map(JdbcColumnHandle::getColumnName)
                .map(this::quote)
                .collect(toImmutableList());
    }

    default String fromRelation(String catalog, String schema, String table)
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
}
