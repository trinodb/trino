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
import io.airlift.configuration.DefunctConfig;

import java.util.Optional;

@DefunctConfig("clickhouse.legacy-driver")
public class ClickHouseConfig
{
    // TODO (https://github.com/trinodb/trino/issues/7102) reconsider default behavior
    private boolean mapStringAsVarchar;

    private Optional<String> clusterName = Optional.empty();

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

    public Optional<String> getClusterName()
    {
        return clusterName;
    }

    @Config("clickhouse.cluster-name")
    @ConfigDescription("Name of the ClickHouse cluster on which DDL statements are executed")
    public ClickHouseConfig setClusterName(String clusterName)
    {
        // Airlift passes an empty property value through as an empty string, and that would be
        // spliced into the DDL as `ON CLUSTER ""`, which every statement rejects. Treat a blank
        // value the same as leaving the property unset.
        this.clusterName = Optional.ofNullable(clusterName).filter(name -> !name.isEmpty());
        return this;
    }
}
