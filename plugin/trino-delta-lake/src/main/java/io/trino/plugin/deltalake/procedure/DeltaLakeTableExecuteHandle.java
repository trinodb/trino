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
package io.trino.plugin.deltalake.procedure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public class DeltaLakeTableExecuteHandle
        implements ConnectorTableExecuteHandle
{
    private final SchemaTableName schemaTableName;
    private final DeltaLakeTableProcedureId procedureId;
    private final DeltaTableProcedureHandle procedureHandle;
    private final String tableLocation;

    @JsonCreator
    public DeltaLakeTableExecuteHandle(
            SchemaTableName schemaTableName,
            DeltaLakeTableProcedureId procedureId,
            DeltaTableProcedureHandle procedureHandle,
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
    public DeltaLakeTableProcedureId getProcedureId()
    {
        return procedureId;
    }

    @JsonProperty
    public DeltaTableProcedureHandle getProcedureHandle()
    {
        return procedureHandle;
    }

    @JsonProperty
    public String getTableLocation()
    {
        return tableLocation;
    }

    public DeltaLakeTableExecuteHandle withProcedureHandle(DeltaTableProcedureHandle procedureHandle)
    {
        return new DeltaLakeTableExecuteHandle(
                schemaTableName,
                procedureId,
                procedureHandle,
                tableLocation);
    }
}
