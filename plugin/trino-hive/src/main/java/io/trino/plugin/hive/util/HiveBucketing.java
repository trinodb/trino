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
package io.trino.plugin.hive.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveType;
import io.trino.metastore.Table;
import io.trino.metastore.type.ListTypeInfo;
import io.trino.metastore.type.MapTypeInfo;
import io.trino.metastore.type.PrimitiveCategory;
import io.trino.metastore.type.PrimitiveTypeInfo;
import io.trino.metastore.type.TypeInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveTablePartitioning;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.cartesianProduct;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.TABLE_BUCKETING_VERSION;
import static io.trino.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isParallelPartitionedBucketedWrites;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static io.trino.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static java.lang.String.format;
import static java.util.Map.Entry;
import static java.util.function.Function.identity;

public final class HiveBucketing
{
    public enum BucketingVersion
    {
        BUCKETING_V1(1) {
            @Override
            int getBucketHashCode(List<TypeInfo> types, Object[] values)
            {
                return HiveBucketingV1.getBucketHashCode(types, values);
            }

            @Override
            int getBucketHashCode(List<TypeInfo> types, Page page, int position)
            {
                return HiveBucketingV1.getBucketHashCode(types, page, position);
            }
        },
        BUCKETING_V2(2) {
            @Override
            int getBucketHashCode(List<TypeInfo> types, Object[] values)
            {
                return HiveBucketingV2.getBucketHashCode(types, values);
            }

            @Override
            int getBucketHashCode(List<TypeInfo> types, Page page, int position)
            {
                return HiveBucketingV2.getBucketHashCode(types, page, position);
            }
        },
        /**/;

        private final int version;

        BucketingVersion(int version)
        {
            this.version = version;
        }

        public int getVersion()
        {
            return version;
        }

        abstract int getBucketHashCode(List<TypeInfo> types, Object[] values);

        abstract int getBucketHashCode(List<TypeInfo> types, Page page, int position);
    }

    private static final long BUCKETS_EXPLORATION_LIMIT_FACTOR = 4;
    private static final long BUCKETS_EXPLORATION_GUARANTEED_LIMIT = 1000;

    private HiveBucketing() {}

    public static int getHiveBucket(BucketingVersion bucketingVersion, int bucketCount, List<TypeInfo> types, Page page, int position)
    {
        return getBucketNumber(bucketingVersion.getBucketHashCode(types, page, position), bucketCount);
    }

    @VisibleForTesting
    static Optional<Set<Integer>> getHiveBuckets(BucketingVersion bucketingVersion, int bucketCount, List<TypeInfo> types, List<List<NullableValue>> values)
    {
        long explorationCount;
        try {
            // explorationCount is the number of combinations of discrete values allowed for bucketing columns.
            // After computing the bucket for every combination, we get a complete set of buckets that need to be read.
            explorationCount = values.stream()
                    .mapToLong(List::size)
                    .reduce(1, Math::multiplyExact);
        }
        catch (ArithmeticException e) {
            return Optional.empty();
        }
        // explorationLimit is the maximum number of combinations for which the bucket numbers will be computed.
        // If the number of combinations highly exceeds the bucket count, then probably all buckets would be hit.
        // In such case, the bucket filter isn't created and all buckets will be read.
        // The threshold is set to bucketCount * BUCKETS_EXPLORATION_LIMIT_FACTOR.
        // The threshold doesn't apply if the number of combinations is low, that is
        // within BUCKETS_EXPLORATION_GUARANTEED_LIMIT.
        long explorationLimit = Math.max(bucketCount * BUCKETS_EXPLORATION_LIMIT_FACTOR, BUCKETS_EXPLORATION_GUARANTEED_LIMIT);
        if (explorationCount > explorationLimit) {
            return Optional.empty();
        }

        Set<Integer> buckets = new HashSet<>();
        for (List<NullableValue> combination : cartesianProduct(values)) {
            buckets.add(getBucketNumber(bucketingVersion.getBucketHashCode(types, combination.stream().map(NullableValue::getValue).toArray()), bucketCount));
            if (buckets.size() >= bucketCount) {
                return Optional.empty();
            }
        }

        return Optional.of(ImmutableSet.copyOf(buckets));
    }

    @VisibleForTesting
    static int getBucketNumber(int hashCode, int bucketCount)
    {
        return (hashCode & Integer.MAX_VALUE) % bucketCount;
    }

    public static Optional<HiveTablePartitioning> getHiveTablePartitioningForRead(ConnectorSession session, Table table, TypeManager typeManager)
    {
        return getHiveTablePartitioning(false, session, table, typeManager);
    }

    public static Optional<HiveTablePartitioning> getHiveTablePartitioningForWrite(ConnectorSession session, Table table, TypeManager typeManager)
    {
        return getHiveTablePartitioning(true, session, table, typeManager);
    }

    private static Optional<HiveTablePartitioning> getHiveTablePartitioning(boolean forWrite, ConnectorSession session, Table table, TypeManager typeManager)
    {
        if (table.getParameters().containsKey(SPARK_TABLE_PROVIDER_KEY)) {
            return Optional.empty();
        }

        Optional<HiveBucketProperty> hiveBucketProperty = table.getStorage().getBucketProperty();
        if (hiveBucketProperty.isEmpty()) {
            return Optional.empty();
        }

        if (!isSupportedBucketing(table)) {
            return Optional.empty();
        }

        HiveTimestampPrecision timestampPrecision = getTimestampPrecision(session);
        Map<String, HiveColumnHandle> map = getRegularColumnHandles(table, typeManager, timestampPrecision).stream()
                .collect(Collectors.toMap(HiveColumnHandle::getName, identity()));

        ImmutableList.Builder<HiveColumnHandle> bucketColumns = ImmutableList.builder();
        for (String bucketColumnName : hiveBucketProperty.get().bucketedBy()) {
            HiveColumnHandle bucketColumnHandle = map.get(bucketColumnName);
            if (bucketColumnHandle == null) {
                throw new TrinoException(
                        HIVE_INVALID_METADATA,
                        format("Table '%s.%s' is bucketed on non-existent column '%s'", table.getDatabaseName(), table.getTableName(), bucketColumnName));
            }
            bucketColumns.add(bucketColumnHandle);
        }

        return Optional.of(new HiveTablePartitioning(
                forWrite,
                getBucketingVersion(table.getParameters()),
                hiveBucketProperty.get().bucketCount(),
                bucketColumns.build(),
                forWrite && !table.getPartitionColumns().isEmpty() && isParallelPartitionedBucketedWrites(session),
                hiveBucketProperty.get().sortedBy(),
                forWrite));
    }

    public static Optional<HiveBucketFilter> getHiveBucketFilter(HiveTableHandle hiveTable, TupleDomain<ColumnHandle> effectivePredicate)
    {
        if (hiveTable.getTablePartitioning().isEmpty()) {
            return Optional.empty();
        }

        HiveBucketProperty hiveBucketProperty = hiveTable.getTablePartitioning().get().toTableBucketProperty();
        List<HiveColumnHandle> dataColumns = hiveTable.getDataColumns().stream()
                .collect(toImmutableList());

        Optional<Map<ColumnHandle, List<NullableValue>>> bindings = TupleDomain.extractDiscreteValues(effectivePredicate);
        if (bindings.isEmpty()) {
            return Optional.empty();
        }
        BucketingVersion bucketingVersion = hiveTable.getTablePartitioning().get().partitioningHandle().getBucketingVersion();
        Optional<Set<Integer>> buckets = getHiveBuckets(bucketingVersion, hiveBucketProperty, dataColumns, bindings.get());
        if (buckets.isPresent()) {
            return Optional.of(new HiveBucketFilter(buckets.get()));
        }

        Optional<Domain> domain = effectivePredicate.getDomains()
                .flatMap(domains -> domains.entrySet().stream()
                        .filter(entry -> ((HiveColumnHandle) entry.getKey()).getName().equals(BUCKET_COLUMN_NAME))
                        .findFirst()
                        .map(Entry::getValue));
        if (domain.isEmpty()) {
            return Optional.empty();
        }
        ValueSet values = domain.get().getValues();
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        int bucketCount = hiveBucketProperty.bucketCount();
        for (int i = 0; i < bucketCount; i++) {
            if (values.containsValue((long) i)) {
                builder.add(i);
            }
        }
        return Optional.of(new HiveBucketFilter(builder.build()));
    }

    private static Optional<Set<Integer>> getHiveBuckets(BucketingVersion bucketingVersion, HiveBucketProperty hiveBucketProperty, List<HiveColumnHandle> dataColumns, Map<ColumnHandle, List<NullableValue>> bindings)
    {
        if (bindings.isEmpty()) {
            return Optional.empty();
        }

        // Get bucket columns names
        List<String> bucketColumns = hiveBucketProperty.bucketedBy();

        // Verify the bucket column types are supported
        Map<String, HiveType> hiveTypes = new HashMap<>();
        for (HiveColumnHandle column : dataColumns) {
            hiveTypes.put(column.getName(), column.getHiveType());
        }
        for (String column : bucketColumns) {
            if (!isTypeSupportedForBucketing(hiveTypes.get(column).getTypeInfo())) {
                return Optional.empty();
            }
        }

        // Get bindings for bucket columns
        Map<String, List<NullableValue>> bucketBindings = new HashMap<>();
        for (Entry<ColumnHandle, List<NullableValue>> entry : bindings.entrySet()) {
            HiveColumnHandle columnHandle = (HiveColumnHandle) entry.getKey();
            if (bucketColumns.contains(columnHandle.getName())) {
                bucketBindings.put(columnHandle.getName(), entry.getValue());
            }
        }

        // Check that we have bindings for all bucket columns
        if (bucketBindings.size() != bucketColumns.size()) {
            return Optional.empty();
        }

        // Order bucket column bindings accordingly to bucket columns order
        List<List<NullableValue>> orderedBindings = bucketColumns.stream()
                .map(bucketBindings::get)
                .collect(toImmutableList());

        // Get TypeInfos for bucket columns
        List<TypeInfo> typeInfos = bucketColumns.stream()
                .map(name -> hiveTypes.get(name).getTypeInfo())
                .collect(toImmutableList());

        return getHiveBuckets(
                bucketingVersion,
                hiveBucketProperty.bucketCount(),
                typeInfos,
                orderedBindings);
    }

    public static BucketingVersion getBucketingVersion(Map<String, String> tableProperties)
    {
        String bucketingVersion = tableProperties.getOrDefault(TABLE_BUCKETING_VERSION, "1");
        return switch (bucketingVersion) {
            case "1" -> BUCKETING_V1;
            case "2" -> BUCKETING_V2;
            // org.apache.hadoop.hive.ql.exec.Utilities.getBucketingVersion is more permissive and treats any non-number as "1"
            default -> throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, format("Unsupported bucketing version: '%s'", bucketingVersion));
        };
    }

    public static boolean isSupportedBucketing(Table table)
    {
        return isSupportedBucketing(table.getStorage().getBucketProperty().orElseThrow().bucketedBy(), table.getDataColumns(), table.getTableName());
    }

    public static boolean isSupportedBucketing(List<String> bucketedBy, List<Column> dataColumns, String tableName)
    {
        return bucketedBy.stream()
                .map(columnName -> dataColumns.stream().filter(column -> column.getName().equals(columnName)).findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(format("Cannot find column '%s' in %s", columnName, tableName))))
                .map(Column::getType)
                .map(HiveType::getTypeInfo)
                .allMatch(HiveBucketing::isTypeSupportedForBucketing);
    }

    private static boolean isTypeSupportedForBucketing(TypeInfo type)
    {
        switch (type.getCategory()) {
            case PRIMITIVE:
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                return switch (primitiveCategory) {
                    case BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, VARCHAR, DATE -> true;
                    case BINARY, TIMESTAMP, DECIMAL, CHAR -> false;
                    default -> throw new UnsupportedOperationException("Unknown type " + type);
                };
            case LIST:
                return isTypeSupportedForBucketing(((ListTypeInfo) type).getListElementTypeInfo());
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
                return isTypeSupportedForBucketing(mapTypeInfo.getMapKeyTypeInfo()) && isTypeSupportedForBucketing(mapTypeInfo.getMapValueTypeInfo());
            case STRUCT:
            case UNION:
                return false;
        }
        throw new UnsupportedOperationException("Unknown type " + type);
    }

    public static class HiveBucketFilter
    {
        private final Set<Integer> bucketsToKeep;

        @JsonCreator
        public HiveBucketFilter(@JsonProperty("bucketsToKeep") Set<Integer> bucketsToKeep)
        {
            this.bucketsToKeep = bucketsToKeep;
        }

        @JsonProperty
        public Set<Integer> getBucketsToKeep()
        {
            return bucketsToKeep;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            HiveBucketFilter other = (HiveBucketFilter) obj;
            return Objects.equals(this.bucketsToKeep, other.bucketsToKeep);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(bucketsToKeep);
        }
    }
}
