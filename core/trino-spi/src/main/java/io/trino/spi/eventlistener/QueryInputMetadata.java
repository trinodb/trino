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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.metrics.Metrics;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class QueryInputMetadata
{
    private final String catalogName;
    private final CatalogVersion catalogVersion;
    private final String schema;
    private final String table;
    private final List<String> columns;
    private final Optional<Object> connectorInfo;
    private final Metrics connectorMetrics;
    private final OptionalLong physicalInputBytes;
    private final OptionalLong physicalInputRows;

    @JsonCreator
    @Unstable
    public QueryInputMetadata(
            String catalogName,
            CatalogVersion catalogVersion,
            String schema,
            String table,
            List<String> columns,
            Optional<Object> connectorInfo,
            Metrics connectorMetrics,
            OptionalLong physicalInputBytes,
            OptionalLong physicalInputRows)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogVersion = requireNonNull(catalogVersion, "catalogVersion is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.connectorInfo = requireNonNull(connectorInfo, "connectorInfo is null");
        this.connectorMetrics = requireNonNull(connectorMetrics, "connectorMetrics is null");
        this.physicalInputBytes = requireNonNull(physicalInputBytes, "physicalInputBytes is null");
        this.physicalInputRows = requireNonNull(physicalInputRows, "physicalInputRows is null");
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
    public List<String> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<Object> getConnectorInfo()
    {
        return connectorInfo;
    }

    @JsonProperty
    public Metrics getConnectorMetrics()
    {
        return connectorMetrics;
    }

    @JsonProperty
    public OptionalLong getPhysicalInputBytes()
    {
        return physicalInputBytes;
    }

    @JsonProperty
    public OptionalLong getPhysicalInputRows()
    {
        return physicalInputRows;
    }
}
