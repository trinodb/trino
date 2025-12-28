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
package io.trino.plugin.sqlite;

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;

import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.plugin.sqlite.SqliteClient.SQLITE_DATA_TYPES;

public class SqliteConfig
{
    private boolean includeSystemTables;
    private Map<String, String> customDataTypes = Map.of(
            "TEXT", "VARCHAR",
            "INT", "BIGINT");

    public boolean isIncludeSystemTables()
    {
        return includeSystemTables;
    }

    @Config("sqlite.include-system-tables")
    public SqliteConfig setIncludeSystemTables(boolean includeSystemTables)
    {
        this.includeSystemTables = includeSystemTables;
        return this;
    }

    public Map<String, String> getCustomDataTypes()
    {
        return customDataTypes;
    }

    @Config("sqlite.custom-data-types")
    public SqliteConfig setCustomDataTypes(String customDataTypes)
    {
        this.customDataTypes = Splitter.on(",").withKeyValueSeparator("=").split(customDataTypes).entrySet()
                .stream()
                .filter(entry -> SQLITE_DATA_TYPES.contains(entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return this;
    }
}
