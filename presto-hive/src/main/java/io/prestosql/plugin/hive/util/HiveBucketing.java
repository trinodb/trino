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
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
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
        BUCKETING_V1(1),
        BUCKETING_V2(2),
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
    }

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
        return getBucketNumber(getBucketHashCode(bucketingVersion, types, page, position), bucketCount);
    }

    public static int getHiveBucket(BucketingVersion bucketingVersion, int bucketCount, List<TypeInfo> types, Object[] values)
    {
        return getBucketNumber(getBucketHashCode(bucketingVersion, types, values), bucketCount);
    }

    @VisibleForTesting
    static int getBucketNumber(int hashCode, int bucketCount)
    {
        return (hashCode & Integer.MAX_VALUE) % bucketCount;
    }

    @VisibleForTesting
    static int getBucketHashCode(BucketingVersion bucketingVersion, List<TypeInfo> types, Page page, int position)
    {
        switch (bucketingVersion) {
            case BUCKETING_V1:
                return HiveBucketingV1.getBucketHashCode(types, page, position);
            case BUCKETING_V2:
                return HiveBucketingV2.getBucketHashCode(types, page, position);
            default:
                throw new IllegalArgumentException("Unsupported bucketing version: " + bucketingVersion);
        }
    }

    @VisibleForTesting
    static int getBucketHashCode(BucketingVersion bucketingVersion, List<TypeInfo> types, Object[] values)
    {
        switch (bucketingVersion) {
            case BUCKETING_V1:
                return HiveBucketingV1.getBucketHashCode(types, values);
            case BUCKETING_V2:
                return HiveBucketingV2.getBucketHashCode(types, values);
            default:
                throw new IllegalArgumentException("Unsupported bucketing version: " + bucketingVersion);
        }
    }

    public static Optional<HiveBucketHandle> getHiveBucketHandle(Table table, TypeManager typeManager)
    {
        Optional<HiveBucketProperty> hiveBucketProperty = table.getStorage().getBucketProperty();
        if (!hiveBucketProperty.isPresent()) {
            return Optional.empty();
        }

        Map<String, HiveColumnHandle> map = getRegularColumnHandles(table, typeManager).stream()
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
        if (!table.getStorage().getBucketProperty().isPresent()
                || effectivePredicate.isAll()
                || effectivePredicate.isNone()) {
            return Optional.empty();
        }

        // TODO (https://github.com/prestosql/presto/issues/1706): support bucketing v2 for timestamp
        if (containsTimestampBucketedV2(table.getStorage().getBucketProperty().get(), table)) {
            return Optional.empty();
        }

        HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty().get();

        Optional<Domain> hiddenBucketColumnDomain = effectivePredicate.getDomains().get().entrySet().stream()
                .filter(entry -> ((HiveColumnHandle) entry.getKey()).getName().equals(BUCKET_COLUMN_NAME))
                .findFirst()
                .map(Entry::getValue);
        if (hiddenBucketColumnDomain.isPresent()) {
            return getFilterByHiddenBucketColumnPredicate(bucketProperty.getBucketCount(), hiddenBucketColumnDomain.get());
        }

        BucketingVersion bucketingVersion = getBucketingVersion(table);
        Map<String, Column> dataColumns = table.getDataColumns().stream()
                .collect(toImmutableMap(Column::getName, identity()));
        Map<String, Domain> domains = effectivePredicate.getDomains().get().entrySet().stream()
                .collect(toImmutableMap(entry -> ((HiveColumnHandle) entry.getKey()).getName(), entry -> entry.getValue()));
        return getFilterByBucketColumnsPredicate(bucketProperty, bucketingVersion, dataColumns, domains);
    }

    private static Optional<HiveBucketFilter> getFilterByHiddenBucketColumnPredicate(int bucketCount, Domain hiddenBucketColumnDomain)
    {
        if (hiddenBucketColumnDomain.isSingleValue()) {
            return Optional.of(new HiveBucketFilter(ImmutableSet.of(((Long) hiddenBucketColumnDomain.getSingleValue()).intValue())));
        }
        ValueSet values = hiddenBucketColumnDomain.getValues();
        Set<Integer> buckets = IntStream.range(0, bucketCount)
                .filter(i -> values.containsValue((long) i))
                .mapToObj(Integer::new)
                .collect(toImmutableSet());
        return Optional.of(new HiveBucketFilter(buckets));
    }

    private static Optional<HiveBucketFilter> getFilterByBucketColumnsPredicate(
            HiveBucketProperty bucketProperty,
            BucketingVersion bucketingVersion,
            Map<String, Column> dataColumns,
            Map<String, Domain> domains)
    {
        List<String> bucketColumns = bucketProperty.getBucketedBy();

        ImmutableList.Builder<TypeInfo> typeInfos = ImmutableList.builder();
        Object[] values = new Object[bucketColumns.size()];
        int i = 0;

        for (String bucketColumn : bucketColumns) {
            Domain domain = domains.get(bucketColumn);
            Column column = dataColumns.get(bucketColumn);
            if (domain == null
                    || !domain.isNullableSingleValue()
                    || column == null
                    || !SUPPORTED_TYPES_FOR_BUCKET_FILTER.contains(column.getType())) {
                return Optional.empty();
            }
            typeInfos.add(column.getType().getTypeInfo());
            values[i] = domain.getNullableSingleValue();
            i++;
        }

        int bucket = getHiveBucket(bucketingVersion, bucketProperty.getBucketCount(), typeInfos.build(), values);
        return Optional.of(new HiveBucketFilter(ImmutableSet.of(bucket)));
    }

    public static BucketingVersion getBucketingVersion(Table table)
    {
        return getBucketingVersion(table.getParameters());
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

    // TODO (https://github.com/prestosql/presto/issues/1706): support bucketing v2 for timestamp and remove this method
    public static boolean containsTimestampBucketedV2(HiveBucketProperty bucketProperty, Table table)
    {
        switch (bucketProperty.getBucketingVersion()) {
            case BUCKETING_V1:
                return false;
            case BUCKETING_V2:
                break;
            default:
                throw new IllegalArgumentException("Unsupported bucketing version: " + bucketProperty.getBucketingVersion());
        }
        return bucketProperty.getBucketedBy().stream()
                .map(columnName -> table.getColumn(columnName)
                        .orElseThrow(() -> new IllegalArgumentException(format("Cannot find column '%s' in %s", columnName, table))))
                .map(Column::getType)
                .map(HiveType::getTypeInfo)
                .anyMatch(HiveBucketing::containsTimestampBucketedV2);
    }

    private static boolean containsTimestampBucketedV2(TypeInfo type)
    {
        switch (type.getCategory()) {
            case PRIMITIVE:
                return ((PrimitiveTypeInfo) type).getPrimitiveCategory() == TIMESTAMP;
            case LIST:
                return containsTimestampBucketedV2(((ListTypeInfo) type).getListElementTypeInfo());
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
                // Note: we do not check map value type because HiveBucketingV2#hashOfMap hashes map values with v1
                return containsTimestampBucketedV2(mapTypeInfo.getMapKeyTypeInfo());
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
    }
}
