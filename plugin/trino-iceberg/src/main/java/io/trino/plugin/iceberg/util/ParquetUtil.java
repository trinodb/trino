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

package io.trino.plugin.iceberg.util;

import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.MetricsUtil.createNanValueCounts;
import static org.apache.iceberg.parquet.ParquetUtil.extractTimestampInt96;

public final class ParquetUtil
{
    // based on org.apache.iceberg.parquet.ParquetUtil and on org.apache.iceberg.parquet.ParquetConversions
    private ParquetUtil() {}

    public static Metrics footerMetrics(ParquetMetadata metadata, Stream<FieldMetrics<?>> fieldMetrics, MetricsConfig metricsConfig)
    {
        return footerMetrics(metadata, fieldMetrics, metricsConfig, null);
    }

    public static Metrics footerMetrics(
            ParquetMetadata metadata,
            Stream<FieldMetrics<?>> fieldMetrics,
            MetricsConfig metricsConfig,
            NameMapping nameMapping)
    {
        requireNonNull(fieldMetrics, "fieldMetrics should not be null");

        long rowCount = 0;
        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, Literal<?>> lowerBounds = new HashMap<>();
        Map<Integer, Literal<?>> upperBounds = new HashMap<>();
        Set<Integer> missingStats = new HashSet<>();

        // ignore metrics for fields we failed to determine reliable IDs
        MessageType parquetTypeWithIds = getParquetTypeWithIds(metadata, nameMapping);
        Schema fileSchema = ParquetSchemaUtil.convertAndPrune(parquetTypeWithIds);

        Map<Integer, FieldMetrics<?>> fieldMetricsMap = fieldMetrics.collect(toMap(FieldMetrics::id, identity()));

        List<BlockMetadata> blocks = metadata.getBlocks();
        for (BlockMetadata block : blocks) {
            rowCount += block.getRowCount();
            for (ColumnChunkMetadata column : block.getColumns()) {
                Integer fieldId = fileSchema.aliasToId(column.getPath().toDotString());
                if (fieldId == null) {
                    // fileSchema may contain a subset of columns present in the file
                    // as we prune columns we could not assign ids
                    continue;
                }

                increment(columnSizes, fieldId, column.getTotalSize());

                MetricsMode metricsMode = MetricsUtil.metricsMode(fileSchema, metricsConfig, fieldId);
                if (metricsMode == MetricsModes.None.get()) {
                    continue;
                }
                increment(valueCounts, fieldId, column.getValueCount());

                Statistics stats = column.getStatistics();
                if (stats != null && !stats.isEmpty()) {
                    increment(nullValueCounts, fieldId, stats.getNumNulls());

                    // when there are metrics gathered by Iceberg for a column, we should use those instead
                    // of the ones from Parquet
                    if (metricsMode != MetricsModes.Counts.get() && !fieldMetricsMap.containsKey(fieldId)) {
                        Types.NestedField field = fileSchema.findField(fieldId);
                        if (field != null && stats.hasNonNullValue() && shouldStoreBounds(column, fileSchema)) {
                            Literal<?> min = fromParquetPrimitive(field.type(), column.getPrimitiveType(), stats.genericGetMin());
                            updateMin(lowerBounds, fieldId, field.type(), min, metricsMode);
                            Literal<?> max = fromParquetPrimitive(field.type(), column.getPrimitiveType(), stats.genericGetMax());
                            updateMax(upperBounds, fieldId, field.type(), max, metricsMode);
                        }
                    }
                }
                else {
                    missingStats.add(fieldId);
                }
            }
        }

        // discard accumulated values if any stats were missing
        for (Integer fieldId : missingStats) {
            nullValueCounts.remove(fieldId);
            lowerBounds.remove(fieldId);
            upperBounds.remove(fieldId);
        }

        updateFromFieldMetrics(fieldMetricsMap, metricsConfig, fileSchema, lowerBounds, upperBounds);

        return new Metrics(
                rowCount,
                columnSizes,
                valueCounts,
                nullValueCounts,
                createNanValueCounts(fieldMetricsMap.values().stream(), metricsConfig, fileSchema),
                toBufferMap(fileSchema, lowerBounds),
                toBufferMap(fileSchema, upperBounds));
    }

    private static void updateFromFieldMetrics(
            Map<Integer, FieldMetrics<?>> idToFieldMetricsMap,
            MetricsConfig metricsConfig,
            Schema schema,
            Map<Integer, Literal<?>> lowerBounds,
            Map<Integer, Literal<?>> upperBounds)
    {
        idToFieldMetricsMap
                .entrySet()
                .forEach(
                        entry -> {
                            int fieldId = entry.getKey();
                            FieldMetrics<?> metrics = entry.getValue();
                            MetricsMode metricsMode = MetricsUtil.metricsMode(schema, metricsConfig, fieldId);

                            // only check for MetricsModes.None, since we don't truncate float/double values.
                            if (metricsMode != MetricsModes.None.get()) {
                                if (!metrics.hasBounds()) {
                                    lowerBounds.remove(fieldId);
                                    upperBounds.remove(fieldId);
                                }
                                else if (metrics.upperBound() instanceof Float) {
                                    lowerBounds.put(fieldId, Literal.of((Float) metrics.lowerBound()));
                                    upperBounds.put(fieldId, Literal.of((Float) metrics.upperBound()));
                                }
                                else if (metrics.upperBound() instanceof Double) {
                                    lowerBounds.put(fieldId, Literal.of((Double) metrics.lowerBound()));
                                    upperBounds.put(fieldId, Literal.of((Double) metrics.upperBound()));
                                }
                                else {
                                    throw new UnsupportedOperationException("Expected only float or double column metrics");
                                }
                            }
                        });
    }

    private static MessageType getParquetTypeWithIds(ParquetMetadata metadata, NameMapping nameMapping)
    {
        MessageType type = metadata.getFileMetaData().getSchema();

        if (ParquetSchemaUtil.hasIds(type)) {
            return type;
        }

        if (nameMapping != null) {
            return ParquetSchemaUtil.applyNameMapping(type, nameMapping);
        }

        return ParquetSchemaUtil.addFallbackIds(type);
    }

    // we allow struct nesting, but not maps or arrays
    private static boolean shouldStoreBounds(ColumnChunkMetadata column, Schema schema)
    {
        if (column.getPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
            // stats for INT96 are not reliable
            return false;
        }

        ColumnPath columnPath = column.getPath();
        Iterator<String> pathIterator = columnPath.iterator();
        Type currentType = schema.asStruct();

        while (pathIterator.hasNext()) {
            if (currentType == null || !currentType.isStructType()) {
                return false;
            }
            String fieldName = pathIterator.next();
            currentType = currentType.asStructType().fieldType(fieldName);
        }

        return currentType != null && currentType.isPrimitiveType();
    }

    private static void increment(Map<Integer, Long> columns, int fieldId, long amount)
    {
        if (columns != null) {
            if (columns.containsKey(fieldId)) {
                columns.put(fieldId, columns.get(fieldId) + amount);
            }
            else {
                columns.put(fieldId, amount);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void updateMin(
            Map<Integer, Literal<?>> lowerBounds,
            int id,
            Type type,
            Literal<T> min,
            MetricsMode metricsMode)
    {
        Literal<T> currentMin = (Literal<T>) lowerBounds.get(id);
        if (currentMin == null || min.comparator().compare(min.value(), currentMin.value()) < 0) {
            if (metricsMode == MetricsModes.Full.get()) {
                lowerBounds.put(id, min);
            }
            else {
                MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
                int truncateLength = truncateMode.length();
                switch (type.typeId()) {
                    case STRING:
                        lowerBounds.put(id, UnicodeUtil.truncateStringMin((Literal<CharSequence>) min, truncateLength));
                        break;
                    case FIXED:
                    case BINARY:
                        lowerBounds.put(id, BinaryUtil.truncateBinaryMin((Literal<ByteBuffer>) min, truncateLength));
                        break;
                    default:
                        lowerBounds.put(id, min);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void updateMax(
            Map<Integer, Literal<?>> upperBounds,
            int id,
            Type type,
            Literal<T> max,
            MetricsMode metricsMode)
    {
        Literal<T> currentMax = (Literal<T>) upperBounds.get(id);
        if (currentMax == null || max.comparator().compare(max.value(), currentMax.value()) > 0) {
            if (metricsMode == MetricsModes.Full.get()) {
                upperBounds.put(id, max);
            }
            else {
                MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
                int truncateLength = truncateMode.length();
                switch (type.typeId()) {
                    case STRING:
                        Literal<CharSequence> truncatedMaxString = UnicodeUtil.truncateStringMax((Literal<CharSequence>) max, truncateLength);
                        if (truncatedMaxString != null) {
                            upperBounds.put(id, truncatedMaxString);
                        }
                        break;
                    case FIXED:
                    case BINARY:
                        Literal<ByteBuffer> truncatedMaxBinary = BinaryUtil.truncateBinaryMax((Literal<ByteBuffer>) max, truncateLength);
                        if (truncatedMaxBinary != null) {
                            upperBounds.put(id, truncatedMaxBinary);
                        }
                        break;
                    default:
                        upperBounds.put(id, max);
                }
            }
        }
    }

    private static Map<Integer, ByteBuffer> toBufferMap(Schema schema, Map<Integer, Literal<?>> map)
    {
        Map<Integer, ByteBuffer> bufferMap = new HashMap<>();
        for (Map.Entry<Integer, Literal<?>> entry : map.entrySet()) {
            bufferMap.put(
                    entry.getKey(),
                    Conversions.toByteBuffer(schema.findType(entry.getKey()), entry.getValue().value()));
        }
        return bufferMap;
    }

    @SuppressWarnings("unchecked")
    public static <T> Literal<T> fromParquetPrimitive(Type type, PrimitiveType parquetType, Object value)
    {
        return switch (type.typeId()) {
            case BOOLEAN -> (Literal<T>) Literal.of((Boolean) value);
            case INTEGER, DATE -> (Literal<T>) Literal.of((Integer) value);
            case LONG, TIME, TIMESTAMP -> (Literal<T>) Literal.of((Long) value);
            case FLOAT -> (Literal<T>) Literal.of((Float) value);
            case DOUBLE -> (Literal<T>) Literal.of((Double) value);
            case STRING -> {
                Function<Object, Object> stringConversion = converterFromParquet(parquetType);
                yield (Literal<T>) Literal.of((CharSequence) stringConversion.apply(value));
            }
            case UUID -> {
                Function<Object, Object> uuidConversion = converterFromParquet(parquetType);
                yield (Literal<T>) Literal.of((UUID) uuidConversion.apply(value));
            }
            case FIXED, BINARY -> {
                Function<Object, Object> binaryConversion = converterFromParquet(parquetType);
                yield (Literal<T>) Literal.of((ByteBuffer) binaryConversion.apply(value));
            }
            case DECIMAL -> {
                Function<Object, Object> decimalConversion = converterFromParquet(parquetType);
                yield (Literal<T>) Literal.of((BigDecimal) decimalConversion.apply(value));
            }
            default -> throw new IllegalArgumentException("Unsupported primitive type: " + type);
        };
    }

    static Function<Object, Object> converterFromParquet(PrimitiveType type)
    {
        if (type.getOriginalType() != null) {
            switch (type.getOriginalType()) {
                case UTF8:
                    // decode to CharSequence to avoid copying into a new String
                    return binary -> StandardCharsets.UTF_8.decode(((Binary) binary).toByteBuffer());
                case DECIMAL:
                    DecimalLogicalTypeAnnotation decimal = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
                    int scale = decimal.getScale();
                    return switch (type.getPrimitiveTypeName()) {
                        case INT32, INT64 -> number -> BigDecimal.valueOf(((Number) number).longValue(), scale);
                        case FIXED_LEN_BYTE_ARRAY, BINARY -> binary -> new BigDecimal(new BigInteger(((Binary) binary).getBytes()), scale);
                        default -> throw new IllegalArgumentException("Unsupported primitive type for decimal: " + type.getPrimitiveTypeName());
                    };
                default:
            }
        }

        return switch (type.getPrimitiveTypeName()) {
            case FIXED_LEN_BYTE_ARRAY, BINARY -> binary -> ByteBuffer.wrap(((Binary) binary).getBytes());
            case INT96 -> binary -> extractTimestampInt96(ByteBuffer.wrap(((Binary) binary).getBytes()).order(ByteOrder.LITTLE_ENDIAN));
            default -> obj -> obj;
        };
    }
}
