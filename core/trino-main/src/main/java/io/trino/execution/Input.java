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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Input
{
    private final String catalogName;
    private final CatalogVersion catalogVersion;
    private final String schema;
    private final String table;
    private final List<Column> columns;
    private final Optional<Object> connectorInfo;
    private final PlanFragmentId fragmentId;
    private final PlanNodeId planNodeId;

    @JsonCreator
    public Input(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("catalogVersion") CatalogVersion catalogVersion,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("connectorInfo") Optional<Object> connectorInfo,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("fragmentId") PlanFragmentId fragmentId,
            @JsonProperty("planNodeId") PlanNodeId planNodeId)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogVersion = requireNonNull(catalogVersion, "catalogVersion is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.connectorInfo = requireNonNull(connectorInfo, "connectorInfo is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.fragmentId = requireNonNull(fragmentId, "fragmentId is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public CatalogVersion getCatalogVersion()
    {
        return catalogVersion;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<Object> getConnectorInfo()
    {
        return connectorInfo;
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public PlanFragmentId getFragmentId()
    {
        return fragmentId;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Input input = (Input) o;
        return Objects.equals(catalogName, input.catalogName) &&
                Objects.equals(catalogVersion, input.catalogVersion) &&
                Objects.equals(schema, input.schema) &&
                Objects.equals(table, input.table) &&
                Objects.equals(columns, input.columns) &&
                Objects.equals(connectorInfo, input.connectorInfo) &&
                Objects.equals(fragmentId, input.fragmentId) &&
                Objects.equals(planNodeId, input.planNodeId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, catalogVersion, schema, table, columns, connectorInfo, fragmentId, planNodeId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(catalogName)
                .addValue(catalogVersion)
                .addValue(schema)
                .addValue(table)
                .addValue(columns)
                .addValue(fragmentId)
                .addValue(planNodeId)
                .toString();
    }
}
