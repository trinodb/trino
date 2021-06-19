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
    private final Optional<List<HivePartition>> partitions;
    private final TupleDomain<HiveColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;
    private final Optional<List<List<String>>> analyzePartitionValues;
    private final Optional<Set<String>> analyzeColumnNames;
    private final Optional<Set<ColumnHandle>> constraintColumns;
    private final Optional<Set<ColumnHandle>> projectedColumns;
    private final AcidTransaction transaction;

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
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                Optional.empty(),
                Optional.empty(),
                transaction);
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
                TupleDomain.all(),
                TupleDomain.all(),
                bucketHandle,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION);
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            Optional<Map<String, String>> tableParameters,
            List<HiveColumnHandle> partitionColumns,
            List<HiveColumnHandle> dataColumns,
            Optional<List<HivePartition>> partitions,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            Optional<List<List<String>>> analyzePartitionValues,
            Optional<Set<String>> analyzeColumnNames,
            Optional<Set<ColumnHandle>> constraintColumns,
            Optional<Set<ColumnHandle>> projectedColumns,
            AcidTransaction transaction)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters is null").map(ImmutableMap::copyOf);
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.partitions = requireNonNull(partitions, "partitions is null").map(ImmutableList::copyOf);
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.analyzePartitionValues = requireNonNull(analyzePartitionValues, "analyzePartitionValues is null");
        this.analyzeColumnNames = requireNonNull(analyzeColumnNames, "analyzeColumnNames is null").map(ImmutableSet::copyOf);
        this.constraintColumns = requireNonNull(constraintColumns, "constraintColumns is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
    }

    public HiveTableHandle withAnalyzePartitionValues(List<List<String>> analyzePartitionValues)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                Optional.of(analyzePartitionValues),
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction);
    }

    public HiveTableHandle withAnalyzeColumnNames(Set<String> analyzeColumnNames)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                Optional.of(analyzeColumnNames),
                constraintColumns,
                projectedColumns,
                transaction);
    }

    public HiveTableHandle withTransaction(AcidTransaction transaction)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction);
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
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                projectedColumns,
                transaction);
    }

    public HiveTableHandle withProjectedColumns(Set<ColumnHandle> projectedColumns)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                dataColumns,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                analyzeColumnNames,
                constraintColumns,
                Optional.of(projectedColumns),
                transaction);
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

    // do not serialize partitions as they are not needed on workers
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
    public Optional<Set<ColumnHandle>> getConstraintColumns()
    {
        return constraintColumns;
    }

    // do not serialize projected columns as they are not needed on workers
    @JsonIgnore
    public Optional<Set<ColumnHandle>> getProjectedColumns()
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
