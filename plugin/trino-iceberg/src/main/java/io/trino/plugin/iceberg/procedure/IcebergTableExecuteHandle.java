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
package io.trino.plugin.iceberg.procedure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public class IcebergTableExecuteHandle
        implements ConnectorTableExecuteHandle
{
    private final SchemaTableName schemaTableName;
    private final IcebergTableProcedureId procedureId;
    private final IcebergProcedureHandle procedureHandle;
    private final String tableLocation;

    @JsonCreator
    public IcebergTableExecuteHandle(
            SchemaTableName schemaTableName,
            IcebergTableProcedureId procedureId,
            IcebergProcedureHandle procedureHandle,
            String tableLocation)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.procedureId = requireNonNull(procedureId, "procedureId is null");
        this.procedureHandle = requireNonNull(procedureHandle, "procedureHandle is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public IcebergTableProcedureId getProcedureId()
    {
        return procedureId;
    }

    @JsonProperty
    public IcebergProcedureHandle getProcedureHandle()
    {
        return procedureHandle;
    }

    @JsonProperty
    public String getTableLocation()
    {
        return tableLocation;
    }

    public IcebergTableExecuteHandle withProcedureHandle(IcebergProcedureHandle procedureHandle)
    {
        return new IcebergTableExecuteHandle(
                schemaTableName,
                procedureId,
                procedureHandle,
                tableLocation);
    }
}
