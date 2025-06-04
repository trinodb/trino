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

import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record DeltaLakeTableExecuteHandle(
        SchemaTableName schemaTableName,
        DeltaLakeTableProcedureId procedureId,
        DeltaTableProcedureHandle procedureHandle,
        String tableLocation)
        implements ConnectorTableExecuteHandle
{
    public DeltaLakeTableExecuteHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(procedureId, "procedureId is null");
        requireNonNull(procedureHandle, "procedureHandle is null");
        requireNonNull(tableLocation, "tableLocation is null");
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
