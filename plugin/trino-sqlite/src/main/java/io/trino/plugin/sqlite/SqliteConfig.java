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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import org.sqlite.SQLiteOpenMode;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SqliteConfig
{
    static final String SQLITE_NO_COMMENT = "";
    static final String SQLITE_OPEN_MODE = "open_mode";
    // This set should only contain Java SQL Types names (ie: java.sql.Types)
    static final Set<String> SQLITE_DATA_TYPES = ImmutableSet.of(
            "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",
            "CHAR", "VARCHAR", "CLOB",
            "BLOB", "VARBINARY",
            "DATE");

    // Default custom type alias
    private Map<String, String> customDataTypes = ImmutableMap.of(
            "TEXT", "VARCHAR",
            "INT", "BIGINT",
            "DOUBLE PRECISION", "DOUBLE");
    // If no flag is specified, the default open mode is used
    // and corresponds to sqlite.open-mode=READWRITE,CREATE,FULLMUTEX.
    private int openModeFlags;
    private boolean useTypeAffinity;
    private boolean includeSystemTables;

    public boolean getUseTypeAffinity()
    {
        return useTypeAffinity;
    }

    @Config("sqlite.use-type-affinity")
    public SqliteConfig setUseTypeAffinity(boolean useTypeAffinity)
    {
        this.useTypeAffinity = useTypeAffinity;
        return this;
    }

    public boolean hasOpenModeFlags()
    {
        return openModeFlags != 0;
    }

    public int getOpenModeFlags()
    {
        return openModeFlags;
    }

    @Config("sqlite.open-mode")
    public SqliteConfig setOpenModeFlags(String openMode)
    {
        List<String> openModes = Arrays.asList(openMode.split(",\\s*"));
        this.openModeFlags = EnumSet.allOf(SQLiteOpenMode.class).stream()
                .filter(e -> openModes.contains(e.name()))
                .map(e -> e.flag)
                .reduce(0, (a, b) -> a | b);
        return this;
    }

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
