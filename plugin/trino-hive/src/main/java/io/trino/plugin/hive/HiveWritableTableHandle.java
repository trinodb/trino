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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePageSinkMetadata;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.util.HiveBucketing.getBucketingVersion;
import static java.util.Objects.requireNonNull;

public class HiveWritableTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<HiveColumnHandle> inputColumns;
    private final HivePageSinkMetadata pageSinkMetadata;
    private final LocationHandle locationHandle;
    private final Optional<BucketInfo> bucketInfo;
    private final HiveStorageFormat tableStorageFormat;
    private final HiveStorageFormat partitionStorageFormat;
    private final AcidTransaction transaction;
    private final boolean retriesEnabled;

    public HiveWritableTableHandle(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> inputColumns,
            HivePageSinkMetadata pageSinkMetadata,
            LocationHandle locationHandle,
            Optional<BucketInfo> bucketInfo,
            HiveStorageFormat tableStorageFormat,
            HiveStorageFormat partitionStorageFormat,
            AcidTransaction transaction,
            boolean retriesEnabled)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        this.pageSinkMetadata = requireNonNull(pageSinkMetadata, "pageSinkMetadata is null");
        this.locationHandle = requireNonNull(locationHandle, "locationHandle is null");
        this.bucketInfo = requireNonNull(bucketInfo, "bucketInfo is null");
        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.partitionStorageFormat = requireNonNull(partitionStorageFormat, "partitionStorageFormat is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.retriesEnabled = retriesEnabled;
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

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public List<HiveColumnHandle> getInputColumns()
    {
        return inputColumns;
    }

    @JsonProperty
    public HivePageSinkMetadata getPageSinkMetadata()
    {
        return pageSinkMetadata;
    }

    @JsonProperty
    public LocationHandle getLocationHandle()
    {
        return locationHandle;
    }

    @JsonProperty
    public Optional<BucketInfo> getBucketInfo()
    {
        return bucketInfo;
    }

    @JsonProperty
    public HiveStorageFormat getTableStorageFormat()
    {
        return tableStorageFormat;
    }

    @JsonProperty
    public HiveStorageFormat getPartitionStorageFormat()
    {
        return partitionStorageFormat;
    }

    @JsonProperty
    public AcidTransaction getTransaction()
    {
        return transaction;
    }

    @JsonIgnore
    public boolean isTransactional()
    {
        return transaction.isTransactional();
    }

    @JsonProperty
    public boolean isRetriesEnabled()
    {
        return retriesEnabled;
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName;
    }

    public record BucketInfo(List<String> bucketedBy, BucketingVersion bucketingVersion, int bucketCount, List<SortingColumn> sortedBy)
    {
        public BucketInfo
        {
            bucketedBy = ImmutableList.copyOf(requireNonNull(bucketedBy, "bucketedBy is null"));
            requireNonNull(bucketingVersion, "bucketingVersion is null");
            sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));
        }

        public static Optional<BucketInfo> createBucketInfo(Table table)
        {
            if (table.getStorage().getBucketProperty().isEmpty()) {
                return Optional.empty();
            }
            HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty().get();
            BucketingVersion bucketingVersion = getBucketingVersion(table.getParameters());
            return Optional.of(new BucketInfo(
                    bucketProperty.bucketedBy(),
                    bucketingVersion,
                    bucketProperty.bucketCount(),
                    bucketProperty.sortedBy()));
        }
    }
}
