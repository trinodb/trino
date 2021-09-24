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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePageSinkMetadata;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveTableExecuteHandle
        extends HiveWritableTableHandle
        implements ConnectorTableExecuteHandle
{
    private final String procedureName;
    private final HiveTableHandle sourceTableHandle;

    @JsonCreator
    public HiveTableExecuteHandle(
            @JsonProperty("procedureName") String procedureName,
            @JsonProperty("sourceTableHandle") HiveTableHandle sourceTableHandle,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
            @JsonProperty("pageSinkMetadata") HivePageSinkMetadata pageSinkMetadata,
            @JsonProperty("locationHandle") LocationHandle locationHandle,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("tableStorageFormat") HiveStorageFormat tableStorageFormat,
            @JsonProperty("partitionStorageFormat") HiveStorageFormat partitionStorageFormat,
            @JsonProperty("transaction") AcidTransaction transaction)
    {
        super(
                schemaName,
                tableName,
                inputColumns,
                pageSinkMetadata,
                locationHandle,
                bucketProperty,
                tableStorageFormat,
                partitionStorageFormat,
                transaction);

        this.procedureName = requireNonNull(procedureName, "procedureName is null");
        this.sourceTableHandle = requireNonNull(sourceTableHandle, "sourceTableHandle is null");
    }

    @JsonProperty
    public String getProcedureName()
    {
        return procedureName;
    }

    @Override
    @JsonProperty
    public ConnectorTableHandle getSourceTableHandle()
    {
        return sourceTableHandle;
    }

    @Override
    public String toString()
    {
        return procedureName + "(" + getSchemaName() + "." + getTableName() + ")";
    }
}
