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
package io.trino.plugin.kudu.properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static io.trino.plugin.kudu.properties.RangePartitionDefinition.EMPTY_RANGE_PARTITION;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class KuduTableProperties
{
    public static final String PARTITION_BY_HASH_COLUMNS = "partition_by_hash_columns";
    public static final String PARTITION_BY_HASH_BUCKETS = "partition_by_hash_buckets";
    public static final String PARTITION_BY_HASH_COLUMNS_2 = "partition_by_second_hash_columns";
    public static final String PARTITION_BY_HASH_BUCKETS_2 = "partition_by_second_hash_buckets";
    public static final String PARTITION_BY_RANGE_COLUMNS = "partition_by_range_columns";
    public static final String RANGE_PARTITIONS = "range_partitions";
    public static final String NUM_REPLICAS = "number_of_replicas";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final long DEFAULT_TIMEOUT = 20_000; // timeout for retrieving range partitions in milliseconds

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public KuduTableProperties()
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PARTITION_BY_HASH_COLUMNS,
                        "Columns for optional first hash partition level",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                integerProperty(
                        PARTITION_BY_HASH_BUCKETS,
                        "Number of buckets for optional first hash partition level.",
                        null,
                        false),
                new PropertyMetadata<>(
                        PARTITION_BY_HASH_COLUMNS_2,
                        "Columns for optional second hash partition level",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                integerProperty(
                        PARTITION_BY_HASH_BUCKETS_2,
                        "Number of buckets for optional second hash partition level.",
                        null,
                        false),
                new PropertyMetadata<>(
                        PARTITION_BY_RANGE_COLUMNS,
                        "Columns for optional range partition level",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value),
                integerProperty(
                        NUM_REPLICAS,
                        "Number of tablet replicas. Uses default value from Kudu master if not specified.",
                        null,
                        false),
                stringProperty(
                        RANGE_PARTITIONS,
                        "Initial range partitions as JSON",
                        null,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static PartitionDesign getPartitionDesign(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        List<String> hashColumns = (List<String>) tableProperties.getOrDefault(PARTITION_BY_HASH_COLUMNS, ImmutableList.of());

        @SuppressWarnings("unchecked")
        List<String> hashColumns2 = (List<String>) tableProperties.getOrDefault(PARTITION_BY_HASH_COLUMNS_2, ImmutableList.of());

        PartitionDesign design = new PartitionDesign();
        if (!hashColumns.isEmpty()) {
            List<HashPartitionDefinition> hashPartitions = new ArrayList<>();
            HashPartitionDefinition hash1 = getHashPartitionDefinition(tableProperties, hashColumns, PARTITION_BY_HASH_BUCKETS);
            hashPartitions.add(hash1);
            if (!hashColumns2.isEmpty()) {
                HashPartitionDefinition hash2 = getHashPartitionDefinition(tableProperties, hashColumns2, PARTITION_BY_HASH_BUCKETS_2);
                hashPartitions.add(hash2);
            }
            design.setHash(hashPartitions);
        }
        else if (!hashColumns2.isEmpty()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Table property " + PARTITION_BY_HASH_COLUMNS_2 + " is only allowed if there is also " + PARTITION_BY_HASH_COLUMNS);
        }

        @SuppressWarnings("unchecked")
        List<String> rangeColumns = (List<String>) tableProperties.getOrDefault(PARTITION_BY_RANGE_COLUMNS, ImmutableList.of());
        if (!rangeColumns.isEmpty()) {
            design.setRange(new RangePartitionDefinition(rangeColumns));
        }

        if (!design.hasPartitions()) {
            design.setRange(EMPTY_RANGE_PARTITION);
        }

        return design;
    }

    private static HashPartitionDefinition getHashPartitionDefinition(Map<String, Object> tableProperties, List<String> columns, String bucketPropertyName)
    {
        Integer hashBuckets = (Integer) tableProperties.get(bucketPropertyName);
        if (hashBuckets == null) {
            throw new TrinoException(GENERIC_USER_ERROR, "Missing table property " + bucketPropertyName);
        }
        return new HashPartitionDefinition(columns, hashBuckets);
    }

    public static List<RangePartition> getRangePartitions(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String json = (String) tableProperties.get(RANGE_PARTITIONS);
        if (json == null) {
            return ImmutableList.of();
        }
        RangePartition[] partitions = parseJson(mapper, json, RangePartition[].class);
        if (partitions == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(partitions);
    }

    public static RangePartition parseRangePartition(String json)
    {
        if (json == null) {
            return null;
        }
        return parseJson(mapper, json, RangePartition.class);
    }

    public static Optional<Integer> getNumReplicas(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        Integer numReplicas = (Integer) tableProperties.get(NUM_REPLICAS);
        return Optional.ofNullable(numReplicas);
    }

    public static Map<String, Object> toMap(KuduTable table)
    {
        Map<String, Object> properties = new HashMap<>();

        PartitionDesign partitionDesign = getPartitionDesign(table);

        List<RangePartition> rangePartitionList = getRangePartitionList(table, DEFAULT_TIMEOUT);

        try {
            if (partitionDesign.getHash() != null) {
                List<HashPartitionDefinition> list = partitionDesign.getHash();
                if (!list.isEmpty()) {
                    properties.put(PARTITION_BY_HASH_COLUMNS, list.get(0).columns());
                    properties.put(PARTITION_BY_HASH_BUCKETS, list.get(0).buckets());
                }
                if (list.size() >= 2) {
                    properties.put(PARTITION_BY_HASH_COLUMNS_2, list.get(1).columns());
                    properties.put(PARTITION_BY_HASH_BUCKETS_2, list.get(1).buckets());
                }
            }

            if (partitionDesign.getRange() != null) {
                properties.put(PARTITION_BY_RANGE_COLUMNS, partitionDesign.getRange().columns());
            }

            String partitionRangesValue = mapper.writeValueAsString(rangePartitionList);
            properties.put(RANGE_PARTITIONS, partitionRangesValue);

            properties.put(NUM_REPLICAS, table.getNumReplicas());

            return properties;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<RangePartition> getRangePartitionList(KuduTable table, long timeout)
    {
        List<RangePartition> rangePartitions = new ArrayList<>();
        if (!table.getPartitionSchema().getRangeSchema().getColumnIds().isEmpty()) {
            try {
                for (Partition partition : table.getRangePartitions(timeout)) {
                    RangePartition rangePartition = buildRangePartition(table, partition);
                    rangePartitions.add(rangePartition);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return rangePartitions;
    }

    private static RangePartition buildRangePartition(KuduTable table, Partition partition)
    {
        RangeBoundValue lower = buildRangePartitionBound(table, partition.getRangeKeyStart());
        RangeBoundValue upper = buildRangePartitionBound(table, partition.getRangeKeyEnd());

        return new RangePartition(lower, upper);
    }

    private static RangeBoundValue buildRangePartitionBound(KuduTable table, byte[] rangeKey)
    {
        if (rangeKey.length == 0) {
            return null;
        }
        Schema schema = table.getSchema();
        PartitionSchema partitionSchema = table.getPartitionSchema();
        PartitionSchema.RangeSchema rangeSchema = partitionSchema.getRangeSchema();
        List<Integer> rangeColumns = rangeSchema.getColumnIds();

        int numColumns = rangeColumns.size();

        PartialRow bound = KeyEncoderAccessor.decodeRangePartitionKey(schema, partitionSchema, rangeKey);

        ArrayList<Object> list = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
            Object obj = toValue(schema, bound, rangeColumns.get(i));
            list.add(obj);
        }
        return new RangeBoundValue(list);
    }

    private static Object toValue(Schema schema, PartialRow bound, Integer idx)
    {
        Type type = schema.getColumnByIndex(idx).getType();
        switch (type) {
            case UNIXTIME_MICROS:
                long millis = bound.getLong(idx) / 1000;
                return ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).print(millis);
            case STRING:
                return bound.getString(idx);
            case INT64:
                return bound.getLong(idx);
            case INT32:
                return bound.getInt(idx);
            case INT16:
                return bound.getShort(idx);
            case INT8:
                return (short) bound.getByte(idx);
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                // TODO unsupported
                break;
            case BOOL:
                return bound.getBoolean(idx);
            case BINARY:
                return bound.getBinaryCopy(idx);
            // TODO: add support for varchar and date types: https://github.com/trinodb/trino/issues/11009
            case VARCHAR:
            case DATE:
                break;
        }
        throw new IllegalStateException("Unhandled type " + type + " for range partition");
    }

    public static PartitionDesign getPartitionDesign(KuduTable table)
    {
        Schema schema = table.getSchema();
        PartitionDesign partitionDesign = new PartitionDesign();
        PartitionSchema partitionSchema = table.getPartitionSchema();

        List<HashPartitionDefinition> hashPartitions = partitionSchema.getHashBucketSchemas().stream()
                .map(hashBucketSchema -> {
                    List<String> cols = hashBucketSchema.getColumnIds().stream()
                            .map(idx -> schema.getColumnByIndex(idx).getName()).collect(toImmutableList());
                    return new HashPartitionDefinition(cols, hashBucketSchema.getNumBuckets());
                }).collect(toImmutableList());
        partitionDesign.setHash(hashPartitions);

        List<Integer> rangeColumns = partitionSchema.getRangeSchema().getColumnIds();
        if (!rangeColumns.isEmpty()) {
            partitionDesign.setRange(new RangePartitionDefinition(rangeColumns.stream()
                    .map(i -> schema.getColumns().get(i).getName())
                    .collect(toImmutableList())));
        }

        return partitionDesign;
    }

    public static PartialRow toRangeBoundToPartialRow(Schema schema, RangePartitionDefinition definition,
            RangeBoundValue boundValue)
    {
        PartialRow partialRow = new PartialRow(schema);
        if (boundValue != null) {
            List<Integer> rangeColumns = definition.columns().stream()
                    .map(schema::getColumnIndex).collect(toImmutableList());

            if (rangeColumns.size() != boundValue.getValues().size()) {
                throw new IllegalStateException("Expected " + rangeColumns.size()
                        + " range columns, but got " + boundValue.getValues().size());
            }
            for (int i = 0; i < rangeColumns.size(); i++) {
                Object obj = boundValue.getValues().get(i);
                int idx = rangeColumns.get(i);
                ColumnSchema columnSchema = schema.getColumnByIndex(idx);
                setColumnValue(partialRow, idx, obj, columnSchema.getType(), columnSchema.getName());
            }
        }
        return partialRow;
    }

    private static void setColumnValue(PartialRow partialRow, int idx, Object obj, Type type, String name)
    {
        Number n;

        switch (type) {
            case STRING:
                if (obj instanceof String) {
                    partialRow.addString(idx, (String) obj);
                }
                else {
                    handleInvalidValue(name, type, obj);
                }
                break;
            case INT64:
                n = toNumber(obj, type, name);
                partialRow.addLong(idx, n.longValue());
                break;
            case INT32:
                n = toNumber(obj, type, name);
                partialRow.addInt(idx, n.intValue());
                break;
            case INT16:
                n = toNumber(obj, type, name);
                partialRow.addShort(idx, n.shortValue());
                break;
            case INT8:
                n = toNumber(obj, type, name);
                partialRow.addByte(idx, n.byteValue());
                break;
            case DOUBLE:
                n = toNumber(obj, type, name);
                partialRow.addDouble(idx, n.doubleValue());
                break;
            case FLOAT:
                n = toNumber(obj, type, name);
                partialRow.addFloat(idx, n.floatValue());
                break;
            case UNIXTIME_MICROS:
                long l = toUnixTimeMicros(obj, type, name);
                partialRow.addLong(idx, l);
                break;
            case BOOL:
                boolean b = toBoolean(obj, type, name);
                partialRow.addBoolean(idx, b);
                break;
            case BINARY:
                byte[] bytes = toByteArray(obj, type, name);
                partialRow.addBinary(idx, bytes);
                break;
            default:
                handleInvalidValue(name, type, obj);
                break;
        }
    }

    private static byte[] toByteArray(Object obj, Type type, String name)
    {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        if (obj instanceof String) {
            return Base64.getDecoder().decode((String) obj);
        }
        handleInvalidValue(name, type, obj);
        return null;
    }

    private static boolean toBoolean(Object obj, Type type, String name)
    {
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }
        if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        }
        handleInvalidValue(name, type, obj);
        return false;
    }

    private static long toUnixTimeMicros(Object obj, Type type, String name)
    {
        if (Number.class.isAssignableFrom(obj.getClass())) {
            return ((Number) obj).longValue();
        }
        if (obj instanceof String s) {
            s = s.trim().replace(' ', 'T');
            long millis = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC).parseMillis(s);
            return millis * 1000;
        }
        handleInvalidValue(name, type, obj);
        return 0;
    }

    private static Number toNumber(Object obj, Type type, String name)
    {
        if (Number.class.isAssignableFrom(obj.getClass())) {
            return (Number) obj;
        }
        if (obj instanceof String) {
            return new BigDecimal((String) obj);
        }
        handleInvalidValue(name, type, obj);
        return 0;
    }

    private static void handleInvalidValue(String name, Type type, Object obj)
    {
        throw new IllegalStateException("Invalid value " + obj + " for column " + name + " of type " + type);
    }
}
