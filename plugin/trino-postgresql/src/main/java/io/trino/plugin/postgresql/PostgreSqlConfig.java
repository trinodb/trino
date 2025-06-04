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
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

@DefunctConfig("postgresql.disable-automatic-fetch-size")
public class PostgreSqlConfig
{
    private ArrayMapping arrayMapping = ArrayMapping.DISABLED;
    private boolean includeSystemTables;
    private boolean enableStringPushdownWithCollate;
    private Integer fetchSize;

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

    public Optional<@Min(0) Integer> getFetchSize()
    {
        return Optional.ofNullable(fetchSize);
    }

    @Config("postgresql.fetch-size")
    @ConfigDescription("Postgresql fetch size, trino specific heuristic is applied if empty")
    public PostgreSqlConfig setFetchSize(Integer fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }
}
