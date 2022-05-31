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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.common.FileUtils;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_EXCEEDED_PARTITION_LIMIT;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toPartitionName;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucketFilter;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class HivePartitionManager
{
    private final int maxPartitions;
    private final int domainCompactionThreshold;

    @Inject
    public HivePartitionManager(HiveConfig hiveConfig)
    {
        this(
                hiveConfig.getMaxPartitionsPerScan(),
                hiveConfig.getDomainCompactionThreshold());
    }

    public HivePartitionManager(
            int maxPartitions,
            int domainCompactionThreshold)
    {
        checkArgument(maxPartitions >= 1, "maxPartitions must be at least 1");
        this.maxPartitions = maxPartitions;
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    public HivePartitionResult getPartitions(SemiTransactionalHiveMetastore metastore, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary()
                .intersect(hiveTableHandle.getEnforcedConstraint());

        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Optional<HiveBucketHandle> hiveBucketHandle = hiveTableHandle.getBucketHandle();
        List<HiveColumnHandle> partitionColumns = hiveTableHandle.getPartitionColumns();

        if (effectivePredicate.isNone()) {
            return new HivePartitionResult(partitionColumns, Optional.empty(), ImmutableList.of(), TupleDomain.none(), TupleDomain.none(), hiveBucketHandle, Optional.empty());
        }

        Optional<HiveBucketFilter> bucketFilter = getHiveBucketFilter(hiveTableHandle, effectivePredicate);
        TupleDomain<HiveColumnHandle> compactEffectivePredicate = effectivePredicate
                .transformKeys(HiveColumnHandle.class::cast)
                .simplify(domainCompactionThreshold);

        if (partitionColumns.isEmpty()) {
            return new HivePartitionResult(
                    partitionColumns,
                    Optional.empty(),
                    ImmutableList.of(new HivePartition(tableName)),
                    effectivePredicate,
                    compactEffectivePredicate,
                    hiveBucketHandle,
                    bucketFilter);
        }

        List<Type> partitionTypes = partitionColumns.stream()
                .map(HiveColumnHandle::getType)
                .collect(toList());

        Optional<List<String>> partitionNames = Optional.empty();
        Iterable<HivePartition> partitionsIterable;
        Predicate<Map<ColumnHandle, NullableValue>> predicate = constraint.predicate().orElse(value -> true);
        if (hiveTableHandle.getPartitions().isPresent()) {
            partitionsIterable = hiveTableHandle.getPartitions().get().stream()
                    .filter(partition -> partitionMatches(partitionColumns, effectivePredicate, predicate, partition))
                    .collect(toImmutableList());
        }
        else {
            List<String> partitionNamesList = hiveTableHandle.getPartitionNames()
                    .orElseGet(() -> getFilteredPartitionNames(metastore, tableName, partitionColumns, compactEffectivePredicate));
            partitionsIterable = () -> partitionNamesList.stream()
                    // Apply extra filters which could not be done by getFilteredPartitionNames
                    .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionTypes, effectivePredicate, predicate))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .iterator();
            partitionNames = Optional.of(partitionNamesList);
        }

        return new HivePartitionResult(partitionColumns, partitionNames, partitionsIterable, effectivePredicate, compactEffectivePredicate, hiveBucketHandle, bucketFilter);
    }

    public HivePartitionResult getPartitions(ConnectorTableHandle tableHandle, List<List<String>> partitionValuesList)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        List<HiveColumnHandle> partitionColumns = hiveTableHandle.getPartitionColumns();
        Optional<HiveBucketHandle> bucketHandle = hiveTableHandle.getBucketHandle();

        List<String> partitionColumnNames = partitionColumns.stream()
                .map(HiveColumnHandle::getName)
                .collect(toImmutableList());

        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(HiveColumnHandle::getType)
                .collect(toImmutableList());

        List<HivePartition> partitionList = partitionValuesList.stream()
                .map(partitionValues -> toPartitionName(partitionColumnNames, partitionValues))
                .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionColumnTypes, TupleDomain.all(), value -> true))
                .map(partition -> partition.orElseThrow(() -> new VerifyException("partition must exist")))
                .collect(toImmutableList());

        return new HivePartitionResult(partitionColumns, Optional.empty(), partitionList, TupleDomain.all(), TupleDomain.all(), bucketHandle, Optional.empty());
    }

    public List<HivePartition> getPartitionsAsList(HivePartitionResult partitionResult)
    {
        ImmutableList.Builder<HivePartition> partitionList = ImmutableList.builder();
        int count = 0;
        Iterator<HivePartition> iterator = partitionResult.getPartitions();
        while (iterator.hasNext()) {
            HivePartition partition = iterator.next();
            if (count == maxPartitions) {
                throw new TrinoException(HIVE_EXCEEDED_PARTITION_LIMIT, format(
                        "Query over table '%s' can potentially read more than %s partitions",
                        partition.getTableName(),
                        maxPartitions));
            }
            partitionList.add(partition);
            count++;
        }
        return partitionList.build();
    }

    public HiveTableHandle applyPartitionResult(HiveTableHandle handle, HivePartitionResult partitions, Constraint constraint)
    {
        Optional<List<String>> partitionNames = partitions.getPartitionNames();
        Optional<List<HivePartition>> partitionList = Optional.empty();
        TupleDomain<ColumnHandle> enforcedConstraint = handle.getEnforcedConstraint();

        // Partitions will be loaded if
        // 1. Number of partitionNames is less than or equal to threshold value. Thereby generating additional filter criteria
        //    that can be applied on other join side (if the join is based on partition column),
        // 2. If additional predicate is passed as a part of Constraint. (specified via loadPartition). This delays the partition checks
        //    until we have additional filtering based on Constraint
        if (canPartitionsBeLoaded(partitions) || constraint.predicate().isPresent()) {
            partitionNames = Optional.empty();
            partitionList = Optional.of(getPartitionsAsList(partitions));
            List<HiveColumnHandle> partitionColumns = partitions.getPartitionColumns();
            enforcedConstraint = partitions.getEffectivePredicate().filter((column, domain) -> partitionColumns.contains(column));
        }
        return new HiveTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableParameters(),
                ImmutableList.copyOf(partitions.getPartitionColumns()),
                handle.getDataColumns(),
                partitionNames,
                partitionList,
                partitions.getCompactEffectivePredicate(),
                enforcedConstraint,
                partitions.getBucketHandle(),
                partitions.getBucketFilter(),
                handle.getAnalyzePartitionValues(),
                handle.getAnalyzeColumnNames(),
                Sets.union(handle.getConstraintColumns(), constraint.getPredicateColumns().orElseGet(ImmutableSet::of)),
                handle.getProjectedColumns(),
                handle.getTransaction(),
                handle.isRecordScannedFiles(),
                handle.getMaxScannedFileSize());
    }

    public List<HivePartition> getOrLoadPartitions(SemiTransactionalHiveMetastore metastore, HiveTableHandle table)
    {
        // In case of partitions not being loaded, their permissible values are specified in `HiveTableHandle#getCompactEffectivePredicate,
        // so we do an intersection of getCompactEffectivePredicate and HiveTable's enforced constraint
        TupleDomain<ColumnHandle> summary = table.getEnforcedConstraint().intersect(
                table.getCompactEffectivePredicate()
                        .transformKeys(ColumnHandle.class::cast));
        return table.getPartitions().orElseGet(() ->
                getPartitionsAsList(getPartitions(metastore, table, new Constraint(summary))));
    }

    public boolean canPartitionsBeLoaded(HivePartitionResult partitionResult)
    {
        if (partitionResult.getPartitionNames().isPresent()) {
            return partitionResult.getPartitionNames().orElseThrow().size() <= maxPartitions;
        }
        return true;
    }

    private Optional<HivePartition> parseValuesAndFilterPartition(
            SchemaTableName tableName,
            String partitionId,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            TupleDomain<ColumnHandle> constraintSummary,
            Predicate<Map<ColumnHandle, NullableValue>> constraint)
    {
        HivePartition partition = parsePartition(tableName, partitionId, partitionColumns, partitionColumnTypes);

        if (partitionMatches(partitionColumns, constraintSummary, constraint, partition)) {
            return Optional.of(partition);
        }
        return Optional.empty();
    }

    private boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<ColumnHandle> constraintSummary, Predicate<Map<ColumnHandle, NullableValue>> constraint, HivePartition partition)
    {
        return partitionMatches(partitionColumns, constraintSummary, partition) && constraint.test(partition.getKeys());
    }

    public static boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<ColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().get();
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }

    private List<String> getFilteredPartitionNames(SemiTransactionalHiveMetastore metastore, SchemaTableName tableName, List<HiveColumnHandle> partitionKeys, TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        List<String> columnNames = partitionKeys.stream()
                .map(HiveColumnHandle::getName)
                .collect(toImmutableList());
        TupleDomain<String> partitionKeysFilter = computePartitionKeyFilter(partitionKeys, effectivePredicate);
        // fetch the partition names
        return metastore.getPartitionNamesByFilter(tableName.getSchemaName(), tableName.getTableName(), columnNames, partitionKeysFilter)
                .orElseThrow(() -> new TableNotFoundException(tableName));
    }

    public static HivePartition parsePartition(
            SchemaTableName tableName,
            String partitionName,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes)
    {
        List<String> partitionValues = extractPartitionValues(partitionName);
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), partitionColumnTypes.get(i));
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.buildOrThrow();
        return new HivePartition(tableName, partitionName, values);
    }

    public static List<String> extractPartitionValues(String partitionName)
    {
        ImmutableList.Builder<String> values = ImmutableList.builder();

        boolean inKey = true;
        int valueStart = -1;
        for (int i = 0; i < partitionName.length(); i++) {
            char current = partitionName.charAt(i);
            if (inKey) {
                checkArgument(current != '/', "Invalid partition spec: %s", partitionName);
                if (current == '=') {
                    inKey = false;
                    valueStart = i + 1;
                }
            }
            else if (current == '/') {
                checkArgument(valueStart != -1, "Invalid partition spec: %s", partitionName);
                values.add(FileUtils.unescapePathName(partitionName.substring(valueStart, i)));
                inKey = true;
                valueStart = -1;
            }
        }
        checkArgument(!inKey, "Invalid partition spec: %s", partitionName);
        values.add(FileUtils.unescapePathName(partitionName.substring(valueStart)));

        return values.build();
    }
}
