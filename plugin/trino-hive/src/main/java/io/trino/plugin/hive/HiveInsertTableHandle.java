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
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.InsertMode;

import java.util.List;
import java.util.Optional;

public class HiveInsertTableHandle
        extends HiveWritableTableHandle
        implements ConnectorInsertTableHandle
{
    @JsonCreator
    public HiveInsertTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
            @JsonProperty("pageSinkMetadata") HivePageSinkMetadata pageSinkMetadata,
            @JsonProperty("locationHandle") LocationHandle locationHandle,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("tableStorageFormat") HiveStorageFormat tableStorageFormat,
            @JsonProperty("partitionStorageFormat") HiveStorageFormat partitionStorageFormat,
            @JsonProperty("transaction") AcidTransaction transaction,
            @JsonProperty("retriesEnabled") boolean retriesEnabled,
            @JsonProperty("insertMode") Optional<InsertMode> insertMode)
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
                retriesEnabled,
                insertMode);
    }
}
