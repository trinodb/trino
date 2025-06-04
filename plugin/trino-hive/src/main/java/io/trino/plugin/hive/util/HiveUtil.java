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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.trino.filesystem.Location;
import io.trino.metastore.Column;
import io.trino.metastore.HiveType;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Table;
import io.trino.metastore.type.StructTypeInfo;
import io.trino.orc.OrcWriterOptions;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hive.formats.HiveClassNames.HUDI_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_REALTIME_INPUT_FORMAT;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.metastore.HiveType.toHiveTypes;
import static io.trino.metastore.Partitions.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.metastore.Partitions.escapePathName;
import static io.trino.metastore.SortingColumn.Order.ASCENDING;
import static io.trino.metastore.SortingColumn.Order.DESCENDING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.bucketColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.fileSizeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isBucketColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isFileModifiedTimeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isFileSizeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isPartitionColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isPathColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.partitionColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_COLUMNS_KEY;
import static io.trino.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_FPP_KEY;
import static io.trino.plugin.hive.HiveMetadata.PARQUET_BLOOM_FILTER_COLUMNS_KEY;
import static io.trino.plugin.hive.HiveMetadata.SKIP_FOOTER_COUNT_KEY;
import static io.trino.plugin.hive.HiveMetadata.SKIP_HEADER_COUNT_KEY;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_FPP;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.getPartitionProjectionTrinoColumnProperties;
import static io.trino.plugin.hive.util.HiveBucketing.isSupportedBucketing;
import static io.trino.plugin.hive.util.HiveTypeUtil.getType;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.util.HiveTypeUtil.typeSupported;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Byte.parseByte;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Math.floorDiv;
import static java.lang.Short.parseShort;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

public final class HiveUtil
{
    public static final String SPARK_TABLE_PROVIDER_KEY = "spark.sql.sources.provider";
    public static final String DELTA_LAKE_PROVIDER = "delta";
    private static final String SPARK_TABLE_BUCKET_NUMBER_KEY = "spark.sql.sources.schema.numBuckets";

    public static final String ICEBERG_TABLE_TYPE_NAME = "table_type";
    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";

    private static final LocalDateTime EPOCH_DAY = new LocalDateTime(1970, 1, 1, 0, 0);
    private static final DateTimeFormatter HIVE_DATE_PARSER;
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER;

    private static final String BIG_DECIMAL_POSTFIX = "BD";

    private static final Splitter COLUMN_NAMES_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private static final CharMatcher DOT_MATCHER = CharMatcher.is('.');

    public static String splitError(Throwable t, Location location, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", location, start, length, t.getMessage());
    }

    static {
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSSSS").getParser(),
        };
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").getPrinter();
        HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder().append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser).toFormatter().withZoneUTC();
        HIVE_DATE_PARSER = new DateTimeFormatterBuilder().append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser).toFormatter().withZoneUTC();
    }

    private HiveUtil() {}

    public static Optional<String> getInputFormatName(Map<String, String> schema)
    {
        return Optional.ofNullable(schema.get(FILE_INPUT_FORMAT));
    }

    private static long parseHiveDate(String value)
    {
        LocalDateTime date = HIVE_DATE_PARSER.parseLocalDateTime(value);
        if (!date.toLocalTime().equals(LocalTime.MIDNIGHT)) {
            throw new IllegalArgumentException(format("The value should be a whole round date: '%s'", value));
        }
        return Days.daysBetween(EPOCH_DAY, date).getDays();
    }

    public static long parseHiveTimestamp(String value)
    {
        return HIVE_TIMESTAMP_PARSER.parseMillis(value) * MICROSECONDS_PER_MILLISECOND;
    }

    public static String getSerializationLibraryName(Map<String, String> schema)
    {
        String name = schema.get(SERIALIZATION_LIB);
        checkCondition(name != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive deserializer property: %s", SERIALIZATION_LIB);
        return name;
    }

    private static boolean isHiveNull(byte[] bytes)
    {
        return bytes.length == 2 && bytes[0] == '\\' && bytes[1] == 'N';
    }

    public static void verifyPartitionTypeSupported(String partitionName, Type type)
    {
        if (!isValidPartitionType(type)) {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported type [%s] for partition: %s", type, partitionName));
        }
    }

    private static boolean isValidPartitionType(Type type)
    {
        return type instanceof DecimalType ||
                BOOLEAN.equals(type) ||
                TINYINT.equals(type) ||
                SMALLINT.equals(type) ||
                INTEGER.equals(type) ||
                BIGINT.equals(type) ||
                REAL.equals(type) ||
                DOUBLE.equals(type) ||
                DATE.equals(type) ||
                TIMESTAMP_MILLIS.equals(type) ||
                type instanceof VarcharType ||
                type instanceof CharType;
    }

    public static NullableValue parsePartitionValue(String partitionName, String value, Type type)
    {
        verifyPartitionTypeSupported(partitionName, type);

        boolean isNull = HIVE_DEFAULT_DYNAMIC_PARTITION.equals(value);

        if (type instanceof DecimalType decimalType) {
            if (isNull) {
                return NullableValue.asNull(decimalType);
            }
            if (decimalType.isShort()) {
                if (value.isEmpty()) {
                    return NullableValue.of(decimalType, 0L);
                }
                return NullableValue.of(decimalType, shortDecimalPartitionKey(value, decimalType, partitionName));
            }
            if (value.isEmpty()) {
                return NullableValue.of(decimalType, Int128.ZERO);
            }
            return NullableValue.of(decimalType, longDecimalPartitionKey(value, decimalType, partitionName));
        }

        if (BOOLEAN.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(BOOLEAN);
            }
            if (value.isEmpty()) {
                return NullableValue.of(BOOLEAN, false);
            }
            return NullableValue.of(BOOLEAN, booleanPartitionKey(value, partitionName));
        }

        if (TINYINT.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(TINYINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(TINYINT, 0L);
            }
            return NullableValue.of(TINYINT, tinyintPartitionKey(value, partitionName));
        }

        if (SMALLINT.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(SMALLINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(SMALLINT, 0L);
            }
            return NullableValue.of(SMALLINT, smallintPartitionKey(value, partitionName));
        }

        if (INTEGER.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(INTEGER);
            }
            if (value.isEmpty()) {
                return NullableValue.of(INTEGER, 0L);
            }
            return NullableValue.of(INTEGER, integerPartitionKey(value, partitionName));
        }

        if (BIGINT.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(BIGINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(BIGINT, 0L);
            }
            return NullableValue.of(BIGINT, bigintPartitionKey(value, partitionName));
        }

        if (DATE.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(DATE);
            }
            return NullableValue.of(DATE, datePartitionKey(value, partitionName));
        }

        if (TIMESTAMP_MILLIS.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(TIMESTAMP_MILLIS);
            }
            return NullableValue.of(TIMESTAMP_MILLIS, timestampPartitionKey(value, partitionName));
        }

        if (REAL.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(REAL);
            }
            if (value.isEmpty()) {
                return NullableValue.of(REAL, (long) floatToRawIntBits(0.0f));
            }
            return NullableValue.of(REAL, floatPartitionKey(value, partitionName));
        }

        if (DOUBLE.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(DOUBLE);
            }
            if (value.isEmpty()) {
                return NullableValue.of(DOUBLE, 0.0);
            }
            return NullableValue.of(DOUBLE, doublePartitionKey(value, partitionName));
        }

        if (type instanceof VarcharType) {
            if (isNull) {
                return NullableValue.asNull(type);
            }
            return NullableValue.of(type, varcharPartitionKey(value, partitionName, type));
        }

        if (type instanceof CharType) {
            if (isNull) {
                return NullableValue.asNull(type);
            }
            return NullableValue.of(type, charPartitionKey(value, partitionName, type));
        }

        if (type instanceof VarbinaryType) {
            if (isNull) {
                return NullableValue.asNull(type);
            }
            return NullableValue.of(type, utf8Slice(value));
        }

        throw new VerifyException(format("Unhandled type [%s] for partition: %s", type, partitionName));
    }

    public static boolean isStructuralType(Type type)
    {
        return (type instanceof ArrayType) || (type instanceof MapType) || (type instanceof RowType);
    }

    private static boolean booleanPartitionKey(String value, String name)
    {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for BOOLEAN partition key: %s", value, name));
    }

    private static long bigintPartitionKey(String value, String name)
    {
        try {
            return parseLong(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for BIGINT partition key: %s", value, name));
        }
    }

    private static long integerPartitionKey(String value, String name)
    {
        try {
            return parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for INTEGER partition key: %s", value, name));
        }
    }

    private static long smallintPartitionKey(String value, String name)
    {
        try {
            return parseShort(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for SMALLINT partition key: %s", value, name));
        }
    }

    private static long tinyintPartitionKey(String value, String name)
    {
        try {
            return parseByte(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for TINYINT partition key: %s", value, name));
        }
    }

    private static long floatPartitionKey(String value, String name)
    {
        try {
            return floatToRawIntBits(parseFloat(value));
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for FLOAT partition key: %s", value, name));
        }
    }

    private static double doublePartitionKey(String value, String name)
    {
        try {
            return parseDouble(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for DOUBLE partition key: %s", value, name));
        }
    }

    private static long datePartitionKey(String value, String name)
    {
        try {
            return parseHiveDate(value);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for DATE partition key: %s", value, name));
        }
    }

    private static long timestampPartitionKey(String value, String name)
    {
        try {
            return parseHiveTimestamp(value);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for TIMESTAMP partition key: %s", value, name));
        }
    }

    private static long shortDecimalPartitionKey(String value, DecimalType type, String name)
    {
        return decimalPartitionKey(value, type, name).unscaledValue().longValue();
    }

    private static Int128 longDecimalPartitionKey(String value, DecimalType type, String name)
    {
        return Int128.valueOf(decimalPartitionKey(value, type, name).unscaledValue());
    }

    private static BigDecimal decimalPartitionKey(String value, DecimalType type, String name)
    {
        try {
            if (value.endsWith(BIG_DECIMAL_POSTFIX)) {
                value = value.substring(0, value.length() - BIG_DECIMAL_POSTFIX.length());
            }

            BigDecimal decimal = new BigDecimal(value);
            decimal = decimal.setScale(type.getScale(), UNNECESSARY);
            if (decimal.precision() > type.getPrecision()) {
                throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, type, name));
            }
            return decimal;
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, type, name));
        }
    }

    private static Slice varcharPartitionKey(String value, String name, Type columnType)
    {
        Slice partitionKey = utf8Slice(value);
        VarcharType varcharType = (VarcharType) columnType;
        if (!varcharType.isUnbounded() && SliceUtf8.countCodePoints(partitionKey) > varcharType.getBoundedLength()) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, columnType, name));
        }
        return partitionKey;
    }

    private static Slice charPartitionKey(String value, String name, Type columnType)
    {
        Slice partitionKey = trimTrailingSpaces(utf8Slice(value));
        CharType charType = (CharType) columnType;
        if (SliceUtf8.countCodePoints(partitionKey) > charType.getLength()) {
            throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, columnType, name));
        }
        return partitionKey;
    }

    public static List<ColumnMetadata> getTableColumnMetadata(ConnectorSession session, Table table, TypeManager typeManager)
    {
        return hiveColumnHandles(table, typeManager, getTimestampPrecision(session)).stream()
                .map(columnMetadataGetter(table))
                .collect(toImmutableList());
    }

    public static List<HiveColumnHandle> hiveColumnHandles(Table table, TypeManager typeManager, HiveTimestampPrecision timestampPrecision)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        // add the data fields first
        columns.addAll(getRegularColumnHandles(table, typeManager, timestampPrecision));

        // add the partition keys last (like Hive does)
        columns.addAll(getPartitionKeyColumnHandles(table, typeManager));

        // add hidden columns
        columns.add(pathColumnHandle());
        if (table.getStorage().getBucketProperty().isPresent()) {
            if (isSupportedBucketing(table)) {
                columns.add(bucketColumnHandle());
            }
        }
        columns.add(fileSizeColumnHandle());
        columns.add(fileModifiedTimeColumnHandle());
        if (!table.getPartitionColumns().isEmpty()) {
            columns.add(partitionColumnHandle());
        }

        return columns.build();
    }

    public static List<HiveColumnHandle> getRegularColumnHandles(Table table, TypeManager typeManager, HiveTimestampPrecision timestampPrecision)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        int hiveColumnIndex = 0;
        for (Column field : table.getDataColumns()) {
            // ignore unsupported types rather than failing
            HiveType hiveType = field.getType();
            if (typeSupported(hiveType.getTypeInfo(), table.getStorage().getStorageFormat())) {
                columns.add(createBaseColumn(field.getName(), hiveColumnIndex, hiveType, getType(hiveType, typeManager, timestampPrecision), REGULAR, field.getComment()));
            }
            hiveColumnIndex++;
        }

        return columns.build();
    }

    public static List<HiveColumnHandle> getPartitionKeyColumnHandles(Table table, TypeManager typeManager)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        List<Column> partitionKeys = table.getPartitionColumns();
        for (Column field : partitionKeys) {
            HiveType hiveType = field.getType();
            if (!typeSupported(hiveType.getTypeInfo(), table.getStorage().getStorageFormat())) {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
            columns.add(createBaseColumn(field.getName(), -1, hiveType, typeManager.getType(getTypeSignature(hiveType)), PARTITION_KEY, field.getComment()));
        }

        return columns.build();
    }

    @FormatMethod
    public static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new TrinoException(errorCode, format(formatString, args));
        }
    }

    @Nullable
    public static String columnExtraInfo(boolean partitionKey)
    {
        return partitionKey ? "partition key" : null;
    }

    public static NullableValue getPrefilledColumnValue(
            HiveColumnHandle columnHandle,
            HivePartitionKey partitionKey,
            String path,
            OptionalInt bucketNumber,
            long fileSize,
            long fileModifiedTime,
            String partitionName)
    {
        String columnValue;
        if (partitionKey != null) {
            columnValue = partitionKey.value();
        }
        else if (isPathColumnHandle(columnHandle)) {
            columnValue = path;
        }
        else if (isBucketColumnHandle(columnHandle)) {
            columnValue = String.valueOf(bucketNumber.getAsInt());
        }
        else if (isFileSizeColumnHandle(columnHandle)) {
            columnValue = String.valueOf(fileSize);
        }
        else if (isFileModifiedTimeColumnHandle(columnHandle)) {
            columnValue = HIVE_TIMESTAMP_PARSER.print(fileModifiedTime);
        }
        else if (isPartitionColumnHandle(columnHandle)) {
            columnValue = partitionName;
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "unsupported hidden column: " + columnHandle);
        }

        byte[] bytes = columnValue.getBytes(UTF_8);
        String name = columnHandle.getName();
        Type type = columnHandle.getType();
        if (isHiveNull(bytes)) {
            return NullableValue.asNull(type);
        }
        if (type.equals(BOOLEAN)) {
            return NullableValue.of(type, booleanPartitionKey(columnValue, name));
        }
        if (type.equals(BIGINT)) {
            return NullableValue.of(type, bigintPartitionKey(columnValue, name));
        }
        if (type.equals(INTEGER)) {
            return NullableValue.of(type, integerPartitionKey(columnValue, name));
        }
        if (type.equals(SMALLINT)) {
            return NullableValue.of(type, smallintPartitionKey(columnValue, name));
        }
        if (type.equals(TINYINT)) {
            return NullableValue.of(type, tinyintPartitionKey(columnValue, name));
        }
        if (type.equals(REAL)) {
            return NullableValue.of(type, floatPartitionKey(columnValue, name));
        }
        if (type.equals(DOUBLE)) {
            return NullableValue.of(type, doublePartitionKey(columnValue, name));
        }
        if (type instanceof VarcharType) {
            return NullableValue.of(type, varcharPartitionKey(columnValue, name, type));
        }
        if (type instanceof CharType) {
            return NullableValue.of(type, charPartitionKey(columnValue, name, type));
        }
        if (type.equals(DATE)) {
            return NullableValue.of(type, datePartitionKey(columnValue, name));
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return NullableValue.of(type, timestampPartitionKey(columnValue, name));
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            // used for $file_modified_time
            return NullableValue.of(type, packDateTimeWithZone(floorDiv(timestampPartitionKey(columnValue, name), MICROSECONDS_PER_MILLISECOND), DateTimeZone.getDefault().getID()));
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return NullableValue.of(type, shortDecimalPartitionKey(columnValue, decimalType, name));
            }
            return NullableValue.of(type, longDecimalPartitionKey(columnValue, decimalType, name));
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return NullableValue.of(type, utf8Slice(columnValue));
        }

        throw new TrinoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
    }

    public static List<HiveType> extractStructFieldTypes(HiveType hiveType)
    {
        return ((StructTypeInfo) hiveType.getTypeInfo()).getAllStructFieldTypeInfos().stream()
                .map(typeInfo -> HiveType.valueOf(typeInfo.getTypeName()))
                .collect(toImmutableList());
    }

    public static int getHeaderCount(Map<String, String> schema)
    {
        return getPositiveIntegerValue(schema, SKIP_HEADER_COUNT_KEY, "0");
    }

    public static int getFooterCount(Map<String, String> schema)
    {
        return getPositiveIntegerValue(schema, SKIP_FOOTER_COUNT_KEY, "0");
    }

    private static int getPositiveIntegerValue(Map<String, String> schema, String key, String defaultValue)
    {
        String value = schema.getOrDefault(key, defaultValue);
        try {
            int intValue = parseInt(value);
            if (intValue < 0) {
                throw new TrinoException(HIVE_INVALID_METADATA, format("Invalid value for %s property: %s", key, value));
            }
            return intValue;
        }
        catch (NumberFormatException e) {
            throw new TrinoException(HIVE_INVALID_METADATA, format("Invalid value for %s property: %s", key, value));
        }
    }

    public static List<String> getColumnNames(Map<String, String> schema)
    {
        return COLUMN_NAMES_SPLITTER.splitToList(schema.getOrDefault(LIST_COLUMNS, ""));
    }

    public static List<HiveType> getColumnTypes(Map<String, String> schema)
    {
        return toHiveTypes(schema.getOrDefault(LIST_COLUMN_TYPES, ""));
    }

    public static Set<String> getParquetBloomFilterColumns(Map<String, String> schema)
    {
        return ImmutableSet.copyOf(
                Optional.ofNullable(schema.get(PARQUET_BLOOM_FILTER_COLUMNS_KEY))
                        .map(COLUMN_NAMES_SPLITTER::splitToList)
                        .orElse(ImmutableList.of()));
    }

    public static OrcWriterOptions getOrcWriterOptions(Map<String, String> schema, OrcWriterOptions orcWriterOptions)
    {
        if (schema.containsKey(ORC_BLOOM_FILTER_COLUMNS_KEY)) {
            try {
                // use default fpp DEFAULT_BLOOM_FILTER_FPP if fpp key does not exist in table metadata
                double fpp = schema.containsKey(ORC_BLOOM_FILTER_FPP_KEY)
                        ? parseDouble(schema.get(ORC_BLOOM_FILTER_FPP_KEY))
                        : orcWriterOptions.getBloomFilterFpp();
                return orcWriterOptions
                        .withBloomFilterColumns(ImmutableSet.copyOf(COLUMN_NAMES_SPLITTER.splitToList(schema.get(ORC_BLOOM_FILTER_COLUMNS_KEY))))
                        .withBloomFilterFpp(fpp);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, format("Invalid value for %s property: %s", ORC_BLOOM_FILTER_FPP, schema.get(ORC_BLOOM_FILTER_FPP_KEY)));
            }
        }
        return orcWriterOptions;
    }

    public static SortingColumn sortingColumnFromString(String name)
    {
        SortingColumn.Order order = ASCENDING;
        String lower = name.toUpperCase(ENGLISH);
        if (lower.endsWith(" ASC")) {
            name = name.substring(0, name.length() - 4).trim();
        }
        else if (lower.endsWith(" DESC")) {
            name = name.substring(0, name.length() - 5).trim();
            order = DESCENDING;
        }
        return new SortingColumn(name, order);
    }

    public static String sortingColumnToString(SortingColumn column)
    {
        return column.columnName() + ((column.order() == DESCENDING) ? " DESC" : "");
    }

    public static boolean isHiveSystemSchema(String schemaName)
    {
        if ("information_schema".equals(schemaName)) {
            // `information_schema` is filtered within engine. This condition exists for internal handling in Hive connector.
            return true;
        }
        if ("sys".equals(schemaName)) {
            // Hive 3's `sys` schema contains no objects we can handle, so there is no point in exposing it.
            // Also, exposing it may require proper handling in access control.
            return true;
        }
        return false;
    }

    public static boolean isDeltaLakeTable(Table table)
    {
        return isDeltaLakeTable(table.getParameters());
    }

    public static boolean isDeltaLakeTable(Map<String, String> tableParameters)
    {
        return DELTA_LAKE_PROVIDER.equalsIgnoreCase(tableParameters.get(SPARK_TABLE_PROVIDER_KEY));
    }

    public static boolean isIcebergTable(Table table)
    {
        return isIcebergTable(table.getParameters());
    }

    public static boolean isIcebergTable(Map<String, String> tableParameters)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableParameters.get(ICEBERG_TABLE_TYPE_NAME));
    }

    public static boolean isHudiTable(Table table)
    {
        return isHudiTable(table.getStorage().getStorageFormat().getInputFormatNullable());
    }

    public static boolean isHudiTable(String inputFormat)
    {
        return HUDI_PARQUET_INPUT_FORMAT.equals(inputFormat) ||
                HUDI_PARQUET_REALTIME_INPUT_FORMAT.equals(inputFormat) ||
                HUDI_INPUT_FORMAT.equals(inputFormat) ||
                HUDI_REALTIME_INPUT_FORMAT.equals(inputFormat);
    }

    public static boolean isSparkBucketedTable(Table table)
    {
        return table.getParameters().containsKey(SPARK_TABLE_PROVIDER_KEY)
                && table.getParameters().containsKey(SPARK_TABLE_BUCKET_NUMBER_KEY);
    }

    public static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        table.getPartitionColumns().stream().map(Column::getName).forEach(columnNames::add);
        table.getDataColumns().stream().map(Column::getName).forEach(columnNames::add);
        List<String> allColumnNames = columnNames.build();
        if (allColumnNames.size() > Sets.newHashSet(allColumnNames).size()) {
            throw new TrinoException(HIVE_INVALID_METADATA,
                    format("Hive metadata for table %s is invalid: Table descriptor contains duplicate columns", table.getTableName()));
        }

        List<Column> tableColumns = table.getDataColumns();
        ImmutableMap.Builder<String, Optional<String>> builder = ImmutableMap.builder();
        for (Column field : concat(tableColumns, table.getPartitionColumns())) {
            if (field.getComment().isPresent() && !field.getComment().get().equals("from deserializer")) {
                builder.put(field.getName(), field.getComment());
            }
            else {
                builder.put(field.getName(), Optional.empty());
            }
        }

        Map<String, Optional<String>> columnComment = builder.buildOrThrow();

        return handle -> ColumnMetadata.builder()
                .setName(handle.getName())
                .setType(handle.getType())
                .setComment(handle.isHidden() ? Optional.empty() : columnComment.get(handle.getName()))
                .setExtraInfo(Optional.ofNullable(columnExtraInfo(handle.isPartitionKey())))
                .setHidden(handle.isHidden())
                .setProperties(getPartitionProjectionTrinoColumnProperties(table, handle.getName()))
                .build();
    }

    public static String escapeSchemaName(String schemaName)
    {
        if (isNullOrEmpty(schemaName)) {
            throw new IllegalArgumentException("The provided schemaName cannot be null or empty");
        }
        if (DOT_MATCHER.matchesAllOf(schemaName)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Invalid schema name");
        }
        return escapePathName(schemaName);
    }

    public static String escapeTableName(String tableName)
    {
        if (isNullOrEmpty(tableName)) {
            throw new IllegalArgumentException("The provided tableName cannot be null or empty");
        }
        if (DOT_MATCHER.matchesAllOf(tableName)) {
            throw new TrinoException(GENERIC_USER_ERROR, "Invalid table name");
        }
        return escapePathName(tableName);
    }
}
