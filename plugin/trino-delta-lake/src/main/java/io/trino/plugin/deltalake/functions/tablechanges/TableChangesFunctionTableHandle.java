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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableChangesFunctionTableHandle
        implements ConnectorTableFunctionHandle
{
    private final CatalogHandle catalogHandle;
    private final String schemaName;
    private final String tableName;
    private final long startVersion;
    private final long endVersion;
    private final List<DeltaLakeColumnHandle> columns;

    @JsonCreator
    public TableChangesFunctionTableHandle(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("startVersion") long startVersion,
            @JsonProperty("endVersion") long endVersion,
            @JsonProperty("columns") List<DeltaLakeColumnHandle> columns)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.startVersion = startVersion;
        this.endVersion = endVersion;
        this.columns = requireNonNull(columns, "columns is null");
    }

    @Override
    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
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
    public long getStartVersion()
    {
        return startVersion;
    }

    @JsonProperty
    public long getEndVersion()
    {
        return endVersion;
    }

    @JsonProperty
    public List<DeltaLakeColumnHandle> getColumns()
    {
        return columns;
    }
}
