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
package io.trino.filesystem.manager;

import com.google.inject.Inject;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Predicate to determine if a table should be cached based on configured include list.
 * Supports wildcards: schema.table, schema.*, or *
 */
public class TableCachingPredicate
{
    private final Predicate<SchemaTableName> predicate;

    @Inject
    public TableCachingPredicate(FileSystemConfig config)
    {
        this(config.getCacheIncludeTables());
    }

    public TableCachingPredicate(List<String> includeTables)
    {
        requireNonNull(includeTables, "includeTables is null");

        if (includeTables.isEmpty()) {
            this.predicate = _ -> true;
        }
        else {
            this.predicate = matches(includeTables);
        }
    }

    public boolean test(SchemaTableName table)
    {
        return predicate.test(table);
    }

    private static Predicate<SchemaTableName> matches(List<String> tables)
    {
        return tables.stream()
                .map(TableCachingPredicate::parseTableName)
                .map(prefix -> (Predicate<SchemaTableName>) prefix::matches)
                .reduce(Predicate::or)
                .orElse(_ -> false);
    }

    private static SchemaTablePrefix parseTableName(String tableName)
    {
        if (tableName.equals("*")) {
            return new SchemaTablePrefix();
        }
        String[] parts = tableName.split("\\.");
        checkArgument(parts.length == 2, "Invalid schemaTableName: %s", tableName);
        String schema = parts[0];
        String table = parts[1];
        if (table.equals("*")) {
            return new SchemaTablePrefix(schema);
        }
        return new SchemaTablePrefix(schema, table);
    }
}
