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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class HiveTableExecuteHandle
        extends HiveWritableTableHandle
        implements ConnectorTableExecuteHandle
{
    private final String procedureName;
    private final Optional<String> writeDeclarationId;
    private final Optional<Long> maxScannedFileSize;

    @JsonCreator
    public HiveTableExecuteHandle(
            @JsonProperty("procedureName") String procedureName,
            @JsonProperty("writeDeclarationId") Optional<String> writeDeclarationId,
            @JsonProperty("maxScannedFileSize") Optional<Long> maxScannedFileSize,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
            @JsonProperty("pageSinkMetadata") HivePageSinkMetadata pageSinkMetadata,
            @JsonProperty("locationHandle") LocationHandle locationHandle,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("tableStorageFormat") HiveStorageFormat tableStorageFormat,
            @JsonProperty("partitionStorageFormat") HiveStorageFormat partitionStorageFormat,
            @JsonProperty("transaction") AcidTransaction transaction,
            @JsonProperty("retriesEnabled") boolean retriesEnabled)
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
                transaction,
                retriesEnabled);

        // todo to be added soon
        verify(bucketProperty.isEmpty(), "bucketed tables not supported yet");

        this.procedureName = requireNonNull(procedureName, "procedureName is null");
        this.writeDeclarationId = requireNonNull(writeDeclarationId, "writeDeclarationId is null");
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
    }

    @JsonProperty
    public String getProcedureName()
    {
        return procedureName;
    }

    @JsonProperty
    public Optional<String> getWriteDeclarationId()
    {
        return writeDeclarationId;
    }

    @JsonProperty
    public Optional<Long> getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    public HiveTableExecuteHandle withWriteDeclarationId(String writeDeclarationId)
    {
        return new HiveTableExecuteHandle(
                procedureName,
                Optional.of(writeDeclarationId),
                maxScannedFileSize,
                getSchemaName(),
                getTableName(),
                getInputColumns(),
                getPageSinkMetadata(),
                getLocationHandle(),
                getBucketProperty(),
                getTableStorageFormat(),
                getPartitionStorageFormat(),
                getTransaction(),
                isRetriesEnabled());
    }

    @Override
    public String toString()
    {
        return procedureName + "(" + getSchemaName() + "." + getTableName() + ")";
    }
}
