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
package io.prestosql.plugin.hive.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.HiveBucketHandle;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

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
import static io.prestosql.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.prestosql.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.prestosql.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.prestosql.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.prestosql.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static java.lang.String.format;
import static java.util.Map.Entry;
import static java.util.function.Function.identity;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_BUCKETING_VERSION;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP;

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

    private static final Set<HiveType> SUPPORTED_TYPES_FOR_BUCKET_FILTER = ImmutableSet.of(
            HiveType.HIVE_BYTE,
            HiveType.HIVE_SHORT,
            HiveType.HIVE_INT,
            HiveType.HIVE_LONG,
            HiveType.HIVE_BOOLEAN,
            HiveType.HIVE_STRING);

    private HiveBucketing() {}

    public static int getHiveBucket(BucketingVersion bucketingVersion, int bucketCount, List<TypeInfo> types, Page page, int position)
    {
        return getBucketNumber(bucketingVersion.getBucketHashCode(types, page, position), bucketCount);
    }

    public static int getHiveBucket(BucketingVersion bucketingVersion, int bucketCount, List<TypeInfo> types, Object[] values)
    {
        return getBucketNumber(bucketingVersion.getBucketHashCode(types, values), bucketCount);
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

    public static Optional<HiveBucketHandle> getHiveBucketHandle(ConnectorSession session, Table table, TypeManager typeManager)
    {
        Optional<HiveBucketProperty> hiveBucketProperty = table.getStorage().getBucketProperty();
        if (hiveBucketProperty.isEmpty()) {
            return Optional.empty();
        }

        int timestampPrecision = getTimestampPrecision(session).getPrecision();
        Map<String, HiveColumnHandle> map = getRegularColumnHandles(table, typeManager, timestampPrecision).stream()
                .collect(Collectors.toMap(HiveColumnHandle::getName, identity()));

        ImmutableList.Builder<HiveColumnHandle> bucketColumns = ImmutableList.builder();
        for (String bucketColumnName : hiveBucketProperty.get().getBucketedBy()) {
            HiveColumnHandle bucketColumnHandle = map.get(bucketColumnName);
            if (bucketColumnHandle == null) {
                throw new PrestoException(
                        HIVE_INVALID_METADATA,
                        format("Table '%s.%s' is bucketed on non-existent column '%s'", table.getDatabaseName(), table.getTableName(), bucketColumnName));
            }
            bucketColumns.add(bucketColumnHandle);
        }

        BucketingVersion bucketingVersion = hiveBucketProperty.get().getBucketingVersion();
        int bucketCount = hiveBucketProperty.get().getBucketCount();
        return Optional.of(new HiveBucketHandle(bucketColumns.build(), bucketingVersion, bucketCount, bucketCount));
    }

    public static Optional<HiveBucketFilter> getHiveBucketFilter(Table table, TupleDomain<ColumnHandle> effectivePredicate)
    {
        if (table.getStorage().getBucketProperty().isEmpty()) {
            return Optional.empty();
        }

        if (bucketedOnTimestamp(table.getStorage().getBucketProperty().get(), table)) {
            return Optional.empty();
        }

        Optional<Map<ColumnHandle, List<NullableValue>>> bindings = TupleDomain.extractDiscreteValues(effectivePredicate);
        if (bindings.isEmpty()) {
            return Optional.empty();
        }
        Optional<Set<Integer>> buckets = getHiveBuckets(table, bindings.get());
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
        int bucketCount = table.getStorage().getBucketProperty().get().getBucketCount();
        for (int i = 0; i < bucketCount; i++) {
            if (values.containsValue((long) i)) {
                builder.add(i);
            }
        }
        return Optional.of(new HiveBucketFilter(builder.build()));
    }

    private static Optional<Set<Integer>> getHiveBuckets(Table table, Map<ColumnHandle, List<NullableValue>> bindings)
    {
        if (bindings.isEmpty()) {
            return Optional.empty();
        }

        // Get bucket columns names
        List<String> bucketColumns = table.getStorage().getBucketProperty().get().getBucketedBy();

        // Verify the bucket column types are supported
        Map<String, HiveType> hiveTypes = new HashMap<>();
        for (Column column : table.getDataColumns()) {
            hiveTypes.put(column.getName(), column.getType());
        }
        for (String column : bucketColumns) {
            if (!SUPPORTED_TYPES_FOR_BUCKET_FILTER.contains(hiveTypes.get(column))) {
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
                getBucketingVersion(table.getParameters()),
                table.getStorage().getBucketProperty().get().getBucketCount(),
                typeInfos,
                orderedBindings);
    }

    public static BucketingVersion getBucketingVersion(Map<String, String> tableProperties)
    {
        String bucketingVersion = tableProperties.getOrDefault(TABLE_BUCKETING_VERSION, "1");
        switch (bucketingVersion) {
            case "1":
                return BUCKETING_V1;
            case "2":
                return BUCKETING_V2;
            default:
                // org.apache.hadoop.hive.ql.exec.Utilities.getBucketingVersion is more permissive and treats any non-number as "1"
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, format("Unsupported bucketing version: '%s'", bucketingVersion));
        }
    }

    public static boolean bucketedOnTimestamp(HiveBucketProperty bucketProperty, Table table)
    {
        return bucketProperty.getBucketedBy().stream()
                .map(columnName -> table.getColumn(columnName)
                        .orElseThrow(() -> new IllegalArgumentException(format("Cannot find column '%s' in %s", columnName, table))))
                .map(Column::getType)
                .map(HiveType::getTypeInfo)
                .anyMatch(HiveBucketing::bucketedOnTimestamp);
    }

    private static boolean bucketedOnTimestamp(TypeInfo type)
    {
        switch (type.getCategory()) {
            case PRIMITIVE:
                return ((PrimitiveTypeInfo) type).getPrimitiveCategory() == TIMESTAMP;
            case LIST:
                return bucketedOnTimestamp(((ListTypeInfo) type).getListElementTypeInfo());
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
                return bucketedOnTimestamp(mapTypeInfo.getMapKeyTypeInfo()) ||
                        bucketedOnTimestamp(mapTypeInfo.getMapValueTypeInfo());
            default:
                // TODO: support more types, e.g. ROW
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory());
        }
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
