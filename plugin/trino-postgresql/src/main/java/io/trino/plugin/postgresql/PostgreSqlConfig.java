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
package io.trino.plugin.postgresql;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class PostgreSqlConfig
{
    private ArrayMapping arrayMapping = ArrayMapping.DISABLED;
    private boolean includeSystemTables;
    private boolean enableStringPushdownWithCollate;
    private boolean autoCommit;
    private Optional<String> asOfSystemTime = Optional.empty();
    private long maxRowsPerResultSet;
    private int fetchSize = 1000;

    public enum ArrayMapping
    {
        DISABLED,
        AS_ARRAY,
        AS_JSON,
    }

    @NotNull
    public ArrayMapping getArrayMapping()
    {
        return arrayMapping;
    }

    @Config("postgresql.array-mapping")
    @LegacyConfig("postgresql.experimental.array-mapping")
    public PostgreSqlConfig setArrayMapping(ArrayMapping arrayMapping)
    {
        this.arrayMapping = arrayMapping;
        return this;
    }

    public boolean isIncludeSystemTables()
    {
        return includeSystemTables;
    }

    @Config("postgresql.include-system-tables")
    public PostgreSqlConfig setIncludeSystemTables(boolean includeSystemTables)
    {
        this.includeSystemTables = includeSystemTables;
        return this;
    }

    public boolean isEnableStringPushdownWithCollate()
    {
        return enableStringPushdownWithCollate;
    }

    @Config("postgresql.experimental.enable-string-pushdown-with-collate")
    public PostgreSqlConfig setEnableStringPushdownWithCollate(boolean enableStringPushdownWithCollate)
    {
        this.enableStringPushdownWithCollate = enableStringPushdownWithCollate;
        return this;
    }

    public boolean isAutoCommit()
    {
        return autoCommit;
    }

    @Config("auto-commit")
    public PostgreSqlConfig setAutoCommit(boolean autoCommit)
    {
        this.autoCommit = autoCommit;
        return this;
    }

    public Optional<String> getAsOfSystemTime()
    {
        return asOfSystemTime;
    }

    @Config("as-of-system-time")
    public PostgreSqlConfig setAsOfSystemTime(String asOfSystemTime)
    {
        this.asOfSystemTime = Optional.ofNullable(asOfSystemTime);
        return this;
    }

    public long getMaxRowsPerResultSet()
    {
        return maxRowsPerResultSet;
    }

    @Config("max-rows-per-result-set")
    public PostgreSqlConfig setMaxRowsPerResultSet(long maxRowsPerResultSet)
    {
        this.maxRowsPerResultSet = maxRowsPerResultSet;
        return this;
    }

    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("fetch-size")
    public PostgreSqlConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }
}
