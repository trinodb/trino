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
package io.prestosql.plugin.hive;

import com.google.common.base.Predicates;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hive.common.FileUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_EXCEEDED_PARTITION_LIMIT;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.toPartitionName;
import static io.prestosql.plugin.hive.util.HiveBucketing.getHiveBucketFilter;
import static io.prestosql.plugin.hive.util.HiveUtil.parsePartitionValue;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.predicate.TupleDomain.none;
import static io.prestosql.spi.type.Chars.padSpaces;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePartitionManager
{
    private static final String PARTITION_VALUE_WILDCARD = "";

    private final DateTimeZone timeZone;
    private final int maxPartitions;
    private final boolean assumeCanonicalPartitionKeys;
    private final int domainCompactionThreshold;
    private final TypeManager typeManager;

    @Inject
    public HivePartitionManager(
            TypeManager typeManager,
            HiveConfig hiveConfig)
    {
        this(
                typeManager,
                hiveConfig.getDateTimeZone(),
                hiveConfig.getMaxPartitionsPerScan(),
                hiveConfig.isAssumeCanonicalPartitionKeys(),
                hiveConfig.getDomainCompactionThreshold());
    }

    public HivePartitionManager(
            TypeManager typeManager,
            DateTimeZone timeZone,
            int maxPartitions,
            boolean assumeCanonicalPartitionKeys,
            int domainCompactionThreshold)
    {
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        checkArgument(maxPartitions >= 1, "maxPartitions must be at least 1");
        this.maxPartitions = maxPartitions;
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public HivePartitionResult getPartitions(SemiTransactionalHiveMetastore metastore, HiveIdentity identity, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary()
                .intersect(hiveTableHandle.getEnforcedConstraint());

        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Optional<HiveBucketHandle> hiveBucketHandle = hiveTableHandle.getBucketHandle();
        List<HiveColumnHandle> partitionColumns = hiveTableHandle.getPartitionColumns();

        if (effectivePredicate.isNone()) {
            return new HivePartitionResult(partitionColumns, ImmutableList.of(), none(), none(), none(), hiveBucketHandle, Optional.empty());
        }

        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        Optional<HiveBucketFilter> bucketFilter = getHiveBucketFilter(table, effectivePredicate);
        TupleDomain<HiveColumnHandle> compactEffectivePredicate = toCompactTupleDomain(effectivePredicate, domainCompactionThreshold);

        if (partitionColumns.isEmpty()) {
            return new HivePartitionResult(
                    partitionColumns,
                    ImmutableList.of(new HivePartition(tableName)),
                    compactEffectivePredicate,
                    effectivePredicate,
                    TupleDomain.all(),
                    hiveBucketHandle,
                    bucketFilter);
        }

        List<Type> partitionTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getTypeSignature()))
                .collect(toList());

        Iterable<HivePartition> partitionsIterable;
        Predicate<Map<ColumnHandle, NullableValue>> predicate = constraint.predicate().orElse(value -> true);
        if (hiveTableHandle.getPartitions().isPresent()) {
            partitionsIterable = hiveTableHandle.getPartitions().get().stream()
                .filter(partition -> partitionMatches(partitionColumns, effectivePredicate, predicate, partition))
                .collect(toImmutableList());
        }
        else {
            List<String> partitionNames = getFilteredPartitionNames(metastore, identity, tableName, partitionColumns, effectivePredicate);
            partitionsIterable = () -> partitionNames.stream()
                    // Apply extra filters which could not be done by getFilteredPartitionNames
                    .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionTypes, effectivePredicate, predicate))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .iterator();
        }

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains().get(), not(Predicates.in(partitionColumns))));
        TupleDomain<ColumnHandle> enforcedTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains().get(), Predicates.in(partitionColumns)));
        return new HivePartitionResult(partitionColumns, partitionsIterable, compactEffectivePredicate, remainingTupleDomain, enforcedTupleDomain, hiveBucketHandle, bucketFilter);
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
                .map(column -> typeManager.getType(column.getTypeSignature()))
                .collect(toImmutableList());

        List<HivePartition> partitionList = partitionValuesList.stream()
                .map(partitionValues -> toPartitionName(partitionColumnNames, partitionValues))
                .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionColumnTypes, TupleDomain.all(), value -> true))
                .map(partition -> partition.orElseThrow(() -> new VerifyException("partition must exist")))
                .collect(toImmutableList());

        return new HivePartitionResult(partitionColumns, partitionList, TupleDomain.all(), TupleDomain.all(), TupleDomain.all(), bucketHandle, Optional.empty());
    }

    public List<HivePartition> getPartitionsAsList(HivePartitionResult partitionResult)
    {
        ImmutableList.Builder<HivePartition> partitionList = ImmutableList.builder();
        int count = 0;
        Iterator<HivePartition> iterator = partitionResult.getPartitions();
        while (iterator.hasNext()) {
            HivePartition partition = iterator.next();
            if (count == maxPartitions) {
                throw new PrestoException(HIVE_EXCEEDED_PARTITION_LIMIT, format(
                        "Query over table '%s' can potentially read more than %s partitions",
                        partition.getTableName(),
                        maxPartitions));
            }
            partitionList.add(partition);
            count++;
        }
        return partitionList.build();
    }

    public HiveTableHandle applyPartitionResult(HiveTableHandle handle, HivePartitionResult partitions)
    {
        return new HiveTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableParameters(),
                ImmutableList.copyOf(partitions.getPartitionColumns()),
                Optional.of(getPartitionsAsList(partitions)),
                partitions.getCompactEffectivePredicate(),
                partitions.getEnforcedConstraint(),
                partitions.getBucketHandle(),
                partitions.getBucketFilter(),
                handle.getAnalyzePartitionValues());
    }

    public List<HivePartition> getOrLoadPartitions(SemiTransactionalHiveMetastore metastore, HiveIdentity identity, HiveTableHandle table)
    {
        return table.getPartitions().orElseGet(() ->
                getPartitionsAsList(getPartitions(metastore, identity, table, new Constraint(table.getEnforcedConstraint()))));
    }

    private static TupleDomain<HiveColumnHandle> toCompactTupleDomain(TupleDomain<ColumnHandle> effectivePredicate, int threshold)
    {
        ImmutableMap.Builder<HiveColumnHandle, Domain> builder = ImmutableMap.builder();
        effectivePredicate.getDomains().ifPresent(domains -> {
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) entry.getKey();

                ValueSet values = entry.getValue().getValues();
                ValueSet compactValueSet = values.getValuesProcessor().<Optional<ValueSet>>transform(
                        ranges -> ranges.getRangeCount() > threshold ? Optional.of(ValueSet.ofRanges(ranges.getSpan())) : Optional.empty(),
                        discreteValues -> discreteValues.getValues().size() > threshold ? Optional.of(ValueSet.all(values.getType())) : Optional.empty(),
                        allOrNone -> Optional.empty())
                        .orElse(values);
                builder.put(hiveColumnHandle, Domain.create(compactValueSet, entry.getValue().isNullAllowed()));
            }
        });
        return TupleDomain.withColumnDomains(builder.build());
    }

    private Optional<HivePartition> parseValuesAndFilterPartition(
            SchemaTableName tableName,
            String partitionId,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            TupleDomain<ColumnHandle> constraintSummary,
            Predicate<Map<ColumnHandle, NullableValue>> constraint)
    {
        HivePartition partition = parsePartition(tableName, partitionId, partitionColumns, partitionColumnTypes, timeZone);

        if (partitionMatches(partitionColumns, constraintSummary, constraint, partition)) {
            return Optional.of(partition);
        }
        return Optional.empty();
    }

    private boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<ColumnHandle> constraintSummary, Predicate<Map<ColumnHandle, NullableValue>> constraint, HivePartition partition)
    {
        Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().get();
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }

        return constraint.test(partition.getKeys());
    }

    private List<String> getFilteredPartitionNames(SemiTransactionalHiveMetastore metastore, HiveIdentity identity, SchemaTableName tableName, List<HiveColumnHandle> partitionKeys, TupleDomain<ColumnHandle> effectivePredicate)
    {
        checkArgument(effectivePredicate.getDomains().isPresent());

        List<String> filter = new ArrayList<>();
        for (HiveColumnHandle partitionKey : partitionKeys) {
            Domain domain = effectivePredicate.getDomains().get().get(partitionKey);
            if (domain != null && domain.isNullableSingleValue()) {
                Object value = domain.getNullableSingleValue();
                Type type = domain.getType();
                if (value == null) {
                    filter.add(HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION);
                }
                else if (type instanceof CharType) {
                    Slice slice = (Slice) value;
                    filter.add(padSpaces(slice, (CharType) type).toStringUtf8());
                }
                else if (type instanceof VarcharType) {
                    Slice slice = (Slice) value;
                    filter.add(slice.toStringUtf8());
                }
                // Types above this have only a single possible representation for each value.
                // Types below this may have multiple representations for a single value.  For
                // example, a boolean column may represent the false value as "0", "false" or "False".
                // The metastore distinguishes between these representations, so we cannot prune partitions
                // unless we know that all partition values use the canonical Java representation.
                else if (!assumeCanonicalPartitionKeys) {
                    filter.add(PARTITION_VALUE_WILDCARD);
                }
                else if (type instanceof DecimalType && !((DecimalType) type).isShort()) {
                    Slice slice = (Slice) value;
                    filter.add(Decimals.toString(slice, ((DecimalType) type).getScale()));
                }
                else if (type instanceof DecimalType && ((DecimalType) type).isShort()) {
                    filter.add(Decimals.toString((long) value, ((DecimalType) type).getScale()));
                }
                else if (type instanceof DateType) {
                    DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.date().withZoneUTC();
                    filter.add(dateTimeFormatter.print(TimeUnit.DAYS.toMillis((long) value)));
                }
                else if (type instanceof TimestampType) {
                    // we don't have time zone info, so just add a wildcard
                    filter.add(PARTITION_VALUE_WILDCARD);
                }
                else if (type instanceof TinyintType
                        || type instanceof SmallintType
                        || type instanceof IntegerType
                        || type instanceof BigintType
                        || type instanceof DoubleType
                        || type instanceof RealType
                        || type instanceof BooleanType) {
                    filter.add(value.toString());
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported partition key type: %s", type.getDisplayName()));
                }
            }
            else {
                filter.add(PARTITION_VALUE_WILDCARD);
            }
        }

        // fetch the partition names
        return metastore.getPartitionNamesByParts(identity, tableName.getSchemaName(), tableName.getTableName(), filter)
                .orElseThrow(() -> new TableNotFoundException(tableName));
    }

    public static HivePartition parsePartition(
            SchemaTableName tableName,
            String partitionName,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            DateTimeZone timeZone)
    {
        List<String> partitionValues = extractPartitionValues(partitionName);
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), partitionColumnTypes.get(i), timeZone);
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.build();
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
