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
package io.trino.parquet;

import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.BsonType;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.JsonType;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.MilliSeconds;
import org.apache.parquet.format.NanoSeconds;
import org.apache.parquet.format.NullType;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.UUIDType;
import org.apache.parquet.internal.column.columnindex.BinaryTruncator;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics;
import static org.apache.parquet.schema.LogicalTypeAnnotation.BsonLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

// based on org.apache.parquet.format.converter.ParquetMetadataConverter
public final class ParquetMetadataConverter
{
    public static final long MAX_STATS_SIZE = 4096;

    private ParquetMetadataConverter() {}

    public static LogicalTypeAnnotation getLogicalTypeAnnotation(ConvertedType type, SchemaElement element)
    {
        return switch (type) {
            case UTF8 -> stringType();
            case MAP -> mapType();
            case MAP_KEY_VALUE -> MapKeyValueTypeAnnotation.getInstance();
            case LIST -> listType();
            case ENUM -> enumType();
            case DECIMAL -> {
                int scale = (element == null) ? 0 : element.scale;
                int precision = (element == null) ? 0 : element.precision;
                yield decimalType(scale, precision);
            }
            case DATE -> dateType();
            case TIME_MILLIS -> timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIME_MICROS -> timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
            case TIMESTAMP_MILLIS -> timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIMESTAMP_MICROS -> timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
            case INTERVAL -> IntervalLogicalTypeAnnotation.getInstance();
            case INT_8 -> intType(8, true);
            case INT_16 -> intType(16, true);
            case INT_32 -> intType(32, true);
            case INT_64 -> intType(64, true);
            case UINT_8 -> intType(8, false);
            case UINT_16 -> intType(16, false);
            case UINT_32 -> intType(32, false);
            case UINT_64 -> intType(64, false);
            case JSON -> jsonType();
            case BSON -> bsonType();
        };
    }

    public static LogicalTypeAnnotation getLogicalTypeAnnotation(LogicalType type)
    {
        return switch (type.getSetField()) {
            case MAP -> mapType();
            case BSON -> bsonType();
            case DATE -> dateType();
            case ENUM -> enumType();
            case JSON -> jsonType();
            case LIST -> listType();
            case TIME -> {
                TimeType time = type.getTIME();
                yield timeType(time.isAdjustedToUTC, convertTimeUnit(time.unit));
            }
            case STRING -> stringType();
            case DECIMAL -> {
                DecimalType decimal = type.getDECIMAL();
                yield decimalType(decimal.scale, decimal.precision);
            }
            case INTEGER -> {
                IntType integer = type.getINTEGER();
                yield intType(integer.bitWidth, integer.isSigned);
            }
            case UNKNOWN -> null;
            case TIMESTAMP -> {
                TimestampType timestamp = type.getTIMESTAMP();
                yield timestampType(timestamp.isAdjustedToUTC, convertTimeUnit(timestamp.unit));
            }
            case UUID -> uuidType();
        };
    }

    public static LogicalType convertToLogicalType(LogicalTypeAnnotation annotation)
    {
        return annotation.accept(new LogicalTypeConverterVisitor()).orElse(null);
    }

    public static PrimitiveTypeName getPrimitive(Type type)
    {
        return switch (type) {
            case BYTE_ARRAY -> PrimitiveTypeName.BINARY;
            case INT64 -> PrimitiveTypeName.INT64;
            case INT32 -> PrimitiveTypeName.INT32;
            case BOOLEAN -> PrimitiveTypeName.BOOLEAN;
            case FLOAT -> PrimitiveTypeName.FLOAT;
            case DOUBLE -> PrimitiveTypeName.DOUBLE;
            case INT96 -> PrimitiveTypeName.INT96;
            case FIXED_LEN_BYTE_ARRAY -> PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        };
    }

    public static Encoding getEncoding(org.apache.parquet.column.Encoding encoding)
    {
        return Encoding.valueOf(encoding.name());
    }

    public static org.apache.parquet.column.Encoding getEncoding(Encoding encoding)
    {
        return org.apache.parquet.column.Encoding.valueOf(encoding.name());
    }

    public static EncodingStats convertEncodingStats(List<PageEncodingStats> stats)
    {
        if (stats == null) {
            return null;
        }

        EncodingStats.Builder builder = new EncodingStats.Builder();
        for (PageEncodingStats stat : stats) {
            switch (stat.getPage_type()) {
                case DATA_PAGE_V2:
                    builder.withV2Pages();
                    // fall through
                case DATA_PAGE:
                    builder.addDataEncoding(getEncoding(stat.getEncoding()), stat.getCount());
                    break;
                case DICTIONARY_PAGE:
                    builder.addDictEncoding(getEncoding(stat.getEncoding()), stat.getCount());
                    break;
                default:
                    // ignore
            }
        }
        return builder.build();
    }

    public static org.apache.parquet.internal.column.columnindex.ColumnIndex fromParquetColumnIndex(PrimitiveType type,
            ColumnIndex parquetColumnIndex)
    {
        if (!isMinMaxStatsSupported(type)) {
            return null;
        }
        return ColumnIndexBuilder.build(type,
                fromParquetBoundaryOrder(parquetColumnIndex.getBoundary_order()),
                parquetColumnIndex.getNull_pages(),
                parquetColumnIndex.getNull_counts(),
                parquetColumnIndex.getMin_values(),
                parquetColumnIndex.getMax_values());
    }

    public static org.apache.parquet.internal.column.columnindex.OffsetIndex fromParquetOffsetIndex(OffsetIndex parquetOffsetIndex)
    {
        OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
        for (PageLocation pageLocation : parquetOffsetIndex.getPage_locations()) {
            builder.add(pageLocation.getOffset(), pageLocation.getCompressed_page_size(), pageLocation.getFirst_row_index());
        }
        return builder.build();
    }

    public static boolean isMinMaxStatsSupported(PrimitiveType type)
    {
        return type.columnOrder().getColumnOrderName() == ColumnOrderName.TYPE_DEFINED_ORDER;
    }

    public static Statistics toParquetStatistics(org.apache.parquet.column.statistics.Statistics<?> stats)
    {
        return toParquetStatistics(stats, ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH);
    }

    public static Statistics toParquetStatistics(org.apache.parquet.column.statistics.Statistics<?> stats, int truncateLength)
    {
        Statistics formatStats = new Statistics();
        if (!stats.isEmpty() && withinLimit(stats, truncateLength)) {
            formatStats.setNull_count(stats.getNumNulls());
            if (stats.hasNonNullValue()) {
                byte[] min;
                byte[] max;

                if (stats instanceof BinaryStatistics && truncateLength != Integer.MAX_VALUE) {
                    BinaryTruncator truncator = BinaryTruncator.getTruncator(stats.type());
                    min = tuncateMin(truncator, truncateLength, stats.getMinBytes());
                    max = tuncateMax(truncator, truncateLength, stats.getMaxBytes());
                }
                else {
                    min = stats.getMinBytes();
                    max = stats.getMaxBytes();
                }
                // Fill the former min-max statistics only if the comparison logic is
                // signed so the logic of V1 and V2 stats are the same (which is
                // trivially true for equal min-max values)
                if (sortOrder(stats.type()) == SortOrder.SIGNED || Arrays.equals(min, max)) {
                    formatStats.setMin(min);
                    formatStats.setMax(max);
                }

                if (isMinMaxStatsSupported(stats.type()) || Arrays.equals(min, max)) {
                    formatStats.setMin_value(min);
                    formatStats.setMax_value(max);
                }
            }
        }
        return formatStats;
    }

    public static org.apache.parquet.column.statistics.Statistics<?> fromParquetStatistics(String createdBy, Statistics statistics, PrimitiveType type)
    {
        org.apache.parquet.column.statistics.Statistics.Builder statsBuilder =
                org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);
        if (statistics != null) {
            if (statistics.isSetMin_value() && statistics.isSetMax_value()) {
                byte[] min = statistics.min_value.array();
                byte[] max = statistics.max_value.array();
                if (isMinMaxStatsSupported(type) || Arrays.equals(min, max)) {
                    statsBuilder.withMin(min);
                    statsBuilder.withMax(max);
                }
            }
            else {
                boolean isSet = statistics.isSetMax() && statistics.isSetMin();
                boolean maxEqualsMin = isSet && Arrays.equals(statistics.getMin(), statistics.getMax());
                boolean sortOrdersMatch = SortOrder.SIGNED == sortOrder(type);
                if (isSet && !shouldIgnoreStatistics(createdBy, type.getPrimitiveTypeName()) && (sortOrdersMatch || maxEqualsMin)) {
                    statsBuilder.withMin(statistics.min.array());
                    statsBuilder.withMax(statistics.max.array());
                }
            }

            if (statistics.isSetNull_count()) {
                statsBuilder.withNumNulls(statistics.null_count);
            }
        }
        return statsBuilder.build();
    }

    public static IndexReference toColumnIndexReference(ColumnChunk columnChunk)
    {
        if (columnChunk.isSetColumn_index_offset() && columnChunk.isSetColumn_index_length()) {
            return new IndexReference(columnChunk.getColumn_index_offset(), columnChunk.getColumn_index_length());
        }
        return null;
    }

    public static IndexReference toOffsetIndexReference(ColumnChunk columnChunk)
    {
        if (columnChunk.isSetOffset_index_offset() && columnChunk.isSetOffset_index_length()) {
            return new IndexReference(columnChunk.getOffset_index_offset(), columnChunk.getOffset_index_length());
        }
        return null;
    }

    public enum SortOrder
    {
        SIGNED,
        UNSIGNED,
        UNKNOWN
    }

    private static SortOrder sortOrder(PrimitiveType primitive)
    {
        LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
        if (annotation == null) {
            return defaultSortOrder(primitive.getPrimitiveTypeName());
        }
        return annotation.accept(new SortOrderVisitor())
                .orElse(defaultSortOrder(primitive.getPrimitiveTypeName()));
    }

    private static SortOrder defaultSortOrder(PrimitiveTypeName primitive)
    {
        return switch (primitive) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> SortOrder.SIGNED;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> SortOrder.UNSIGNED;
            default -> SortOrder.UNKNOWN;
        };
    }

    private static LogicalTypeAnnotation.TimeUnit convertTimeUnit(TimeUnit unit)
    {
        return switch (unit.getSetField()) {
            case MICROS -> LogicalTypeAnnotation.TimeUnit.MICROS;
            case MILLIS -> LogicalTypeAnnotation.TimeUnit.MILLIS;
            case NANOS -> LogicalTypeAnnotation.TimeUnit.NANOS;
        };
    }

    private static org.apache.parquet.internal.column.columnindex.BoundaryOrder fromParquetBoundaryOrder(BoundaryOrder boundaryOrder)
    {
        return switch (boundaryOrder) {
            case ASCENDING -> org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING;
            case DESCENDING -> org.apache.parquet.internal.column.columnindex.BoundaryOrder.DESCENDING;
            case UNORDERED -> org.apache.parquet.internal.column.columnindex.BoundaryOrder.UNORDERED;
        };
    }

    private static boolean withinLimit(org.apache.parquet.column.statistics.Statistics<?> stats, int truncateLength)
    {
        if (stats.isSmallerThan(MAX_STATS_SIZE)) {
            return true;
        }

        return (stats instanceof BinaryStatistics binaryStats) &&
                binaryStats.isSmallerThanWithTruncation(MAX_STATS_SIZE, truncateLength);
    }

    private static byte[] tuncateMin(BinaryTruncator truncator, int truncateLength, byte[] input)
    {
        return truncator.truncateMin(Binary.fromConstantByteArray(input), truncateLength).getBytes();
    }

    private static byte[] tuncateMax(BinaryTruncator truncator, int truncateLength, byte[] input)
    {
        return truncator.truncateMax(Binary.fromConstantByteArray(input), truncateLength).getBytes();
    }

    private static class LogicalTypeConverterVisitor
            implements LogicalTypeAnnotationVisitor<LogicalType>
    {
        @Override
        public Optional<LogicalType> visit(StringLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.STRING(new StringType()));
        }

        @Override
        public Optional<LogicalType> visit(MapLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.MAP(new MapType()));
        }

        @Override
        public Optional<LogicalType> visit(ListLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.LIST(new ListType()));
        }

        @Override
        public Optional<LogicalType> visit(EnumLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.ENUM(new EnumType()));
        }

        @Override
        public Optional<LogicalType> visit(DecimalLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.DECIMAL(new DecimalType(type.getScale(), type.getPrecision())));
        }

        @Override
        public Optional<LogicalType> visit(DateLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.DATE(new DateType()));
        }

        @Override
        public Optional<LogicalType> visit(TimeLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.TIME(new TimeType(type.isAdjustedToUTC(), convertUnit(type.getUnit()))));
        }

        @Override
        public Optional<LogicalType> visit(TimestampLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.TIMESTAMP(new TimestampType(type.isAdjustedToUTC(), convertUnit(type.getUnit()))));
        }

        @Override
        public Optional<LogicalType> visit(IntLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.INTEGER(new IntType((byte) type.getBitWidth(), type.isSigned())));
        }

        @Override
        public Optional<LogicalType> visit(JsonLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.JSON(new JsonType()));
        }

        @Override
        public Optional<LogicalType> visit(BsonLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.BSON(new BsonType()));
        }

        @Override
        public Optional<LogicalType> visit(UUIDLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.UUID(new UUIDType()));
        }

        @Override
        public Optional<LogicalType> visit(IntervalLogicalTypeAnnotation type)
        {
            return Optional.of(LogicalType.UNKNOWN(new NullType()));
        }

        static TimeUnit convertUnit(LogicalTypeAnnotation.TimeUnit unit)
        {
            return switch (unit) {
                case MICROS -> TimeUnit.MICROS(new MicroSeconds());
                case MILLIS -> TimeUnit.MILLIS(new MilliSeconds());
                case NANOS -> TimeUnit.NANOS(new NanoSeconds());
            };
        }
    }

    private static class SortOrderVisitor
            implements LogicalTypeAnnotationVisitor<SortOrder>
    {
        @Override
        public Optional<SortOrder> visit(IntLogicalTypeAnnotation intLogicalType)
        {
            return Optional.of(intLogicalType.isSigned() ? SortOrder.SIGNED : SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(IntervalLogicalTypeAnnotation intervalLogicalType)
        {
            return Optional.of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(DateLogicalTypeAnnotation dateLogicalType)
        {
            return Optional.of(SortOrder.SIGNED);
        }

        @Override
        public Optional<SortOrder> visit(EnumLogicalTypeAnnotation enumLogicalType)
        {
            return Optional.of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(BsonLogicalTypeAnnotation bsonLogicalType)
        {
            return Optional.of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(UUIDLogicalTypeAnnotation uuidLogicalType)
        {
            return Optional.of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(JsonLogicalTypeAnnotation jsonLogicalType)
        {
            return Optional.of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(StringLogicalTypeAnnotation stringLogicalType)
        {
            return Optional.of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(DecimalLogicalTypeAnnotation decimalLogicalType)
        {
            return Optional.of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(MapKeyValueTypeAnnotation mapKeyValueLogicalType)
        {
            return Optional.of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(MapLogicalTypeAnnotation mapLogicalType)
        {
            return Optional.of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(ListLogicalTypeAnnotation listLogicalType)
        {
            return Optional.of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(TimeLogicalTypeAnnotation timeLogicalType)
        {
            return Optional.of(SortOrder.SIGNED);
        }

        @Override
        public Optional<SortOrder> visit(TimestampLogicalTypeAnnotation timestampLogicalType)
        {
            return Optional.of(SortOrder.SIGNED);
        }
    }
}
