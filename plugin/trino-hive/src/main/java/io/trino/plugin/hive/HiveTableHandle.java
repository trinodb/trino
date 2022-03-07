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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class HiveTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<Map<String, String>> tableParameters;
    private final List<HiveColumnHandle> partitionColumns;
    private final List<HiveColumnHandle> dataColumns;
    private final Optional<List<String>> partitionNames;
    private final Optional<List<HivePartition>> partitions;
    private final TupleDomain<HiveColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;
    private final Optional<List<List<String>>> analyzePartitionValues;
    private final Optional<Set<String>> analyzeColumnNames;
    private final Set<ColumnHandle> constraintColumns;
    private final Set<ColumnHandle> projectedColumns;
    private final AcidTransaction transaction;
    private final boolean recordScannedFiles;
    private final Optional<Long> maxScannedFileSize;

    @JsonCreator
    public HiveTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("dataColumns") List<HiveColumnHandle> dataColumns,
            @JsonProperty("compactEffectivePredicate") TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            @JsonProperty("enforcedConstraint") TupleDomain<ColumnHandle> enforcedConstraint,
            @JsonProperty("bucketHandle") Optional<HiveBucketHandle> bucketHandle,
            @JsonProperty("bucketFilter") Optional<HiveBucketFilter> bucketFilter,
            @JsonProperty("analyzePartitionValues") Optional<List<List<String>>> analyzePartitionValues,
            @JsonProperty("analyzeColumnNames") Optional<Set<String>> analyzeColumnNames,
            @JsonProperty("transaction") AcidTransaction transaction)
    {
        this(
                schemaName,
                tableName,
                Optional.empty(),
                partitionColumns,
                dataColumns,
                Optional.empty(),
                Optional.empty(),
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                ImmutableSet.of(),
                ImmutableSet.of(),
                transaction,
                false,
                Optional.empty());
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            Map<String, String> tableParameters,
            List<HiveColumnHandle> partitionColumns,
            List<HiveColumnHandle> dataColumns,
            Optional<HiveBucketHandle> bucketHandle)
    {
        this(
                schemaName,
                tableName,
                Optional.of(tableParameters),
                partitionColumns,
                dataColumns,
                Optional.empty(),
                Optional.empty(),
                TupleDomain.all(),
                TupleDomain.all(),
                bucketHandle,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                NO_ACID_TRANSACTION,
                false,
                Optional.empty());
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            Optional<Map<String, String>> tableParameters,
            List<HiveColumnHandle> partitionColumns,
            List<HiveColumnHandle> dataColumns,
            Optional<List<String>> partitionNames,
            Optional<List<HivePartition>> partitions,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            Optional<List<List<String>>> analyzePartitionValues,
            Optional<Set<String>> analyzeColumnNames,
            Set<ColumnHandle> constraintColumns,
            Set<ColumnHandle> projectedColumns,
            AcidTransaction transaction,
            boolean recordScannedFiles,
            Optional<Long> maxSplitFileSize)
    {
        checkState(partitionNames.isEmpty() || partitions.isEmpty(), "partition names and partitions list cannot be present at same time");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters is null").map(ImmutableMap::copyOf);
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.partitionNames = requireNonNull(partitionNames, "partitionNames is null").map(ImmutableList::copyOf);
        this.partitions = requireNonNull(partitions, "partitions is null").map(ImmutableList::copyOf);
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.analyzePartitionValues = requireNonNull(analyzePartitionValues, "analyzePartitionValues is null").map(ImmutableList::copyOf);
        this.analyzeColumnNames = requireNonNull(analyzeColumnNames, "analyzeColumnNames is null").map(ImmutableSet::copyOf);
        this.constraintColumns = ImmutableSet.copyOf(requireNonNull(constraintColumns, "constraintColumns is null"));
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.recordScannedFiles = recordScannedFiles;
        this.maxScannedFileSize = requireNonNull(maxSplitFileSize, "maxSplitFileSize is null");
    }

    public HiveTableHandle withAnalyzePartitionValues(List<List<String>> analyzePartitionValues)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                Optional.of(analyzePartitionValues),
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public HiveTableHandle withAnalyzeColumnNames(Set<String> analyzeColumnNames)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                Optional.of(analyzeColumnNames),
                constraintColumns,
                projectedColumns,
                transaction,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public HiveTableHandle withTransaction(AcidTransaction transaction)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public HiveTableHandle withUpdateProcessor(AcidTransaction transaction, HiveUpdateProcessor updateProcessor)
    {
        requireNonNull(updateProcessor, "updateProcessor is null");
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public HiveTableHandle withProjectedColumns(Set<ColumnHandle> projectedColumns)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public HiveTableHandle withRecordScannedFiles(boolean recordScannedFiles)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public HiveTableHandle withMaxScannedFileSize(Optional<Long> maxScannedFileSize)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction,
                recordScannedFiles,
                maxScannedFileSize);
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

    // do not serialize tableParameters as they are not needed on workers
    @JsonIgnore
    public Optional<Map<String, String>> getTableParameters()
    {
        return tableParameters;
    }

    @JsonProperty
    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public List<HiveColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    /**
     * Represents raw partition information as String.
     * These are partially satisfied by the table filter criteria.
     * This will be set to `Optional#empty` if parsed partition information are loaded.
     * Skip serialization as they are not needed on workers
     */
    @JsonIgnore
    public Optional<List<String>> getPartitionNames()
    {
        return partitionNames;
    }

    /**
     * Represents parsed partition information (which is derived from raw partition string).
     * These are fully satisfied by the table filter criteria.
     * Skip serialization as they are not needed on workers
     */
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return partitions;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        return enforcedConstraint;
    }

    @JsonProperty
    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }

    @JsonProperty
    public Optional<HiveBucketFilter> getBucketFilter()
    {
        return bucketFilter;
    }

    @JsonProperty
    public Optional<List<List<String>>> getAnalyzePartitionValues()
    {
        return analyzePartitionValues;
    }

    @JsonProperty
    public Optional<Set<String>> getAnalyzeColumnNames()
    {
        return analyzeColumnNames;
    }

    @JsonProperty
    public AcidTransaction getTransaction()
    {
        return transaction;
    }

    // do not serialize constraint columns as they are not needed on workers
    @JsonIgnore
    public Set<ColumnHandle> getConstraintColumns()
    {
        return constraintColumns;
    }

    // do not serialize projected columns as they are not needed on workers
    @JsonIgnore
    public Set<ColumnHandle> getProjectedColumns()
    {
        return projectedColumns;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonIgnore
    public boolean isAcidDelete()
    {
        return transaction.isDelete();
    }

    @JsonIgnore
    public boolean isAcidUpdate()
    {
        return transaction.isUpdate();
    }

    @JsonIgnore
    public Optional<HiveUpdateProcessor> getUpdateProcessor()
    {
        return transaction.getUpdateProcessor();
    }

    @JsonIgnore
    public boolean isInAcidTransaction()
    {
        return transaction.isAcidTransactionRunning();
    }

    @JsonIgnore
    public long getAcidTransactionId()
    {
        checkState(transaction.isAcidTransactionRunning(), "The AcidTransaction is not running");
        return transaction.getAcidTransactionId();
    }

    @JsonIgnore
    public long getWriteId()
    {
        checkState(transaction.isAcidTransactionRunning(), "The AcidTransaction is not running");
        return transaction.getWriteId();
    }

    @JsonIgnore
    public boolean isRecordScannedFiles()
    {
        return recordScannedFiles;
    }

    @JsonIgnore
    public Optional<Long> getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HiveTableHandle that = (HiveTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(tableParameters, that.tableParameters) &&
                Objects.equals(partitionColumns, that.partitionColumns) &&
                Objects.equals(partitionNames, that.partitionNames) &&
                Objects.equals(partitions, that.partitions) &&
                Objects.equals(compactEffectivePredicate, that.compactEffectivePredicate) &&
                Objects.equals(enforcedConstraint, that.enforcedConstraint) &&
                Objects.equals(bucketHandle, that.bucketHandle) &&
                Objects.equals(bucketFilter, that.bucketFilter) &&
                Objects.equals(analyzePartitionValues, that.analyzePartitionValues) &&
                Objects.equals(transaction, that.transaction) &&
                Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                partitionNames,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                transaction,
                projectedColumns);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaName).append(":").append(tableName);
        bucketHandle.ifPresent(bucket -> {
            builder.append(" buckets=").append(bucket.getReadBucketCount());
            if (!bucket.getSortedBy().isEmpty()) {
                builder.append(" sorted_by=")
                        .append(bucket.getSortedBy().stream()
                                .map(HiveUtil::sortingColumnToString)
                                .collect(joining(", ", "[", "]")));
            }
        });
        return builder.toString();
    }
}
