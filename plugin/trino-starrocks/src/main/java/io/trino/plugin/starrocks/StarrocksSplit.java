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
package io.trino.plugin.starrocks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class StarrocksSplit
        implements ConnectorSplit
{
    private final String schemaName;
    private final String tableName;
    private final List<Long> tabletId;
    private final String beNode;
    private final String opaquedQueryPlan;

    @JsonCreator
    public StarrocksSplit(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tabletId") List<Long> tabletId,
            @JsonProperty("beNode") String beNode,
            @JsonProperty("opaquedQueryPlan") String opaquedQueryPlan)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tabletId = requireNonNull(tabletId, "tabletId is null");
        this.beNode = requireNonNull(beNode, "beNode is null");
        this.opaquedQueryPlan = requireNonNull(opaquedQueryPlan, "opaqued_query_plan is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<Long> getTabletId()
    {
        return tabletId;
    }

    @JsonProperty
    public String getBeNode()
    {
        return beNode;
    }

    @JsonProperty
    public String getOpaquedQueryPlan()
    {
        return opaquedQueryPlan;
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return Map.of(
                "schemaName", schemaName,
                "tableName", tableName,
                "tabletId", tabletId.toString(),
                "beNode", beNode,
                "opaquedQueryPlan", opaquedQueryPlan);
    }
}
