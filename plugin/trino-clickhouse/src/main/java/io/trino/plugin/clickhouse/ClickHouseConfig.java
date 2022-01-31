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
package io.trino.plugin.clickhouse;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class ClickHouseConfig
{
    // TODO (https://github.com/trinodb/trino/issues/7102) reconsider default behavior
    private boolean mapStringAsVarchar;

    // TODO: This config needs to be deprecated when we upgrade clickhouse-jdbc to version 0.4.0 or above
    private boolean legacyDriver;

    public boolean isMapStringAsVarchar()
    {
        return mapStringAsVarchar;
    }

    @Config("clickhouse.map-string-as-varchar")
    @ConfigDescription("Map ClickHouse String and FixedString as varchar instead of varbinary")
    public ClickHouseConfig setMapStringAsVarchar(boolean mapStringAsVarchar)
    {
        this.mapStringAsVarchar = mapStringAsVarchar;
        return this;
    }

    @Deprecated
    public boolean isLegacyDriver()
    {
        return legacyDriver;
    }

    @Deprecated
    @Config("clickhouse.legacy-driver")
    @ConfigDescription("Whether to use a legacy driver")
    public ClickHouseConfig setLegacyDriver(boolean legacyDriver)
    {
        this.legacyDriver = legacyDriver;
        return this;
    }
}
