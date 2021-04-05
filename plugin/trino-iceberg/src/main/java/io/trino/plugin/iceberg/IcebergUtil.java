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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.reverse;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_PARTITION_VALUE;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.FIXED;

final class IcebergUtil
{
    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z][a-z0-9]*");

    private IcebergUtil() {}

    public static boolean isIcebergTable(io.trino.plugin.hive.metastore.Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    public static Table loadIcebergTable(HiveTableOperationsProvider tableOperationsProvider, ConnectorSession session, SchemaTableName table)
    {
        TableOperations operations = tableOperationsProvider.createTableOperations(
                new HdfsContext(session),
                session.getQueryId(),
                new HiveIdentity(session),
                table.getSchemaName(),
                table.getTableName(),
                Optional.empty(),
                Optional.empty());
        return new BaseTable(operations, quotedTableName(table));
    }

    public static Table getIcebergTableWithMetadata(
            HiveTableOperationsProvider tableOperationsProvider,
            ConnectorSession session,
            SchemaTableName table,
            TableMetadata tableMetadata)
    {
        HiveTableOperations operations = (HiveTableOperations) tableOperationsProvider.createTableOperations(
                new HdfsContext(session),
                session.getQueryId(),
                new HiveIdentity(session),
                table.getSchemaName(),
                table.getTableName(),
                Optional.empty(),
                Optional.empty());
        operations.initializeFromMetadata(tableMetadata);
        return new BaseTable(operations, quotedTableName(table));
    }

    public static long resolveSnapshotId(Table table, long snapshotId)
    {
        if (table.snapshot(snapshotId) != null) {
            return snapshotId;
        }

        return reverse(table.history()).stream()
                .filter(entry -> entry.timestampMillis() <= snapshotId)
                .map(HistoryEntry::snapshotId)
                .findFirst()
                .orElseThrow(() -> new TrinoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", snapshotId, table)));
    }

    public static List<IcebergColumnHandle> getColumns(Schema schema, TypeManager typeManager)
    {
        return schema.columns().stream()
                .map(column -> IcebergColumnHandle.create(column, typeManager))
                .collect(toImmutableList());
    }

    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().toString().equals("identity")) {
                columns.put(field, i);
            }
        }
        return columns.build();
    }

    public static String getDataPath(String location)
    {
        if (!location.endsWith("/")) {
            location += "/";
        }
        return location + "data";
    }

    public static FileFormat getFileFormat(Table table)
    {
        return FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public static Optional<String> getTableComment(Table table)
    {
        return Optional.ofNullable(table.properties().get(TABLE_COMMENT));
    }

    private static String quotedTableName(SchemaTableName name)
    {
        return quotedName(name.getSchemaName()) + "." + quotedName(name.getTableName());
    }

    private static String quotedName(String name)
    {
        if (SIMPLE_NAME.matcher(name).matches()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    public static Object deserializePartitionValue(Type type, String valueString, String name, TimeZoneKey timeZoneKey)
    {
        if (valueString == null) {
            return null;
        }

        try {
            if (type.equals(BOOLEAN)) {
                if (valueString.equalsIgnoreCase("true")) {
                    return true;
                }
                if (valueString.equalsIgnoreCase("false")) {
                    return false;
                }
                throw new IllegalArgumentException();
            }
            if (type.equals(INTEGER)) {
                return parseLong(valueString);
            }
            if (type.equals(BIGINT)) {
                return parseLong(valueString);
            }
            if (type.equals(REAL)) {
                return (long) floatToRawIntBits(parseFloat(valueString));
            }
            if (type.equals(DOUBLE)) {
                return parseDouble(valueString);
            }
            if (type.equals(DATE)) {
                return parseLong(valueString);
            }
            if (type.equals(TIME_MICROS)) {
                return parseLong(valueString) * PICOSECONDS_PER_MICROSECOND;
            }
            if (type.equals(TIMESTAMP_MICROS)) {
                return parseLong(valueString);
            }
            if (type.equals(TIMESTAMP_TZ_MICROS)) {
                return timestampTzFromMicros(parseLong(valueString), timeZoneKey);
            }
            if (type instanceof VarcharType) {
                Slice value = utf8Slice(valueString);
                VarcharType varcharType = (VarcharType) type;
                if (!varcharType.isUnbounded() && SliceUtf8.countCodePoints(value) > varcharType.getBoundedLength()) {
                    throw new IllegalArgumentException();
                }
                return value;
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                return utf8Slice(valueString);
            }
            if (isShortDecimal(type) || isLongDecimal(type)) {
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = new BigDecimal(valueString);
                decimal = decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
                if (decimal.precision() > decimalType.getPrecision()) {
                    throw new IllegalArgumentException();
                }
                BigInteger unscaledValue = decimal.unscaledValue();
                return isShortDecimal(type) ? unscaledValue.longValue() : Decimals.encodeUnscaledValue(unscaledValue);
            }
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(ICEBERG_INVALID_PARTITION_VALUE, format(
                    "Invalid partition value '%s' for %s partition key: %s",
                    valueString,
                    type.getDisplayName(),
                    name));
        }
        // Iceberg tables don't partition by non-primitive-type columns.
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid partition type " + type.toString());
    }

    public static Map<Integer, String> getPartitionKeys(FileScanTask scanTask)
    {
        StructLike partition = scanTask.file().partition();
        PartitionSpec spec = scanTask.spec();
        Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
        Map<Integer, String> partitionKeys = new HashMap<>();

        fieldToIndex.forEach((field, index) -> {
            int id = field.sourceId();
            org.apache.iceberg.types.Type type = spec.schema().findType(id);
            Class<?> javaClass = type.typeId().javaClass();
            Object value = partition.get(index, javaClass);

            if (value == null) {
                partitionKeys.put(id, null);
            }
            else {
                String partitionValue;
                if (type.typeId() == FIXED || type.typeId() == BINARY) {
                    // this is safe because Iceberg PartitionData directly wraps the byte array
                    partitionValue = new String(((ByteBuffer) value).array(), UTF_8);
                }
                else {
                    partitionValue = value.toString();
                }
                partitionKeys.put(id, partitionValue);
            }
        });

        return Collections.unmodifiableMap(partitionKeys);
    }
}
