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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public record IcebergTableExecuteHandle(
        SchemaTableName schemaTableName,
        IcebergTableProcedureId procedureId,
        IcebergProcedureHandle procedureHandle,
        String tableLocation,
        Map<String, String> fileIoProperties)
        implements ConnectorTableExecuteHandle
{
    public IcebergTableExecuteHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(procedureId, "procedureId is null");
        requireNonNull(procedureHandle, "procedureHandle is null");
        requireNonNull(tableLocation, "tableLocation is null");
        fileIoProperties = ImmutableMap.copyOf(requireNonNull(fileIoProperties, "fileIoProperties is null"));
    }

    @Override
    public String toString()
    {
        return "schemaTableName:%s, procedureId:%s, procedureHandle:{%s}".formatted(
                schemaTableName, procedureId, procedureHandle);
    }
}
