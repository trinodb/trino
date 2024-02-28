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
import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.HiveReadOnlyException;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.ProtectMode;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.PrimitiveCategory;
import io.trino.plugin.hive.type.PrimitiveTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TableType.MATERIALIZED_VIEW;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getProtectMode;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyOnline;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HiveWriteUtils
{
    private static final DateTimeFormatter HIVE_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HIVE_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .toFormatter();

    private HiveWriteUtils()
    {
    }

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            String value = toPartitionValue(partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
            if (!CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(value)) {
                String encoded = base16().withSeparator(" ", 2).encode(value.getBytes(UTF_8));
                throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, "Hive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: " + encoded);
            }
            partitionValues.add(value);
        }
        return partitionValues.build();
    }

    private static String toPartitionValue(Type type, Block block, int position)
    {
        // see HiveUtil#isValidPartitionType
        if (block.isNull(position)) {
            return HIVE_DEFAULT_DYNAMIC_PARTITION;
        }
        if (BOOLEAN.equals(type)) {
            return String.valueOf(BOOLEAN.getBoolean(block, position));
        }
        if (BIGINT.equals(type)) {
            return String.valueOf(BIGINT.getLong(block, position));
        }
        if (INTEGER.equals(type)) {
            return String.valueOf(INTEGER.getInt(block, position));
        }
        if (SMALLINT.equals(type)) {
            return String.valueOf(SMALLINT.getShort(block, position));
        }
        if (TINYINT.equals(type)) {
            return String.valueOf(TINYINT.getByte(block, position));
        }
        if (REAL.equals(type)) {
            return String.valueOf(REAL.getFloat(block, position));
        }
        if (DOUBLE.equals(type)) {
            return String.valueOf(DOUBLE.getDouble(block, position));
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof CharType charType) {
            return padSpaces(charType.getSlice(block, position), charType).toStringUtf8();
        }
        if (DATE.equals(type)) {
            return LocalDate.ofEpochDay(DATE.getInt(block, position)).format(HIVE_DATE_FORMATTER);
        }
        if (TIMESTAMP_MILLIS.equals(type)) {
            long epochMicros = type.getLong(block, position);
            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanosOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            return LocalDateTime.ofEpochSecond(epochSeconds, nanosOfSecond, ZoneOffset.UTC).format(HIVE_TIMESTAMP_FORMATTER);
        }
        if (type instanceof DecimalType decimalType) {
            return readBigDecimal(decimalType, block, position).stripTrailingZeros().toPlainString();
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for partition: " + type);
    }

    public static void checkTableIsWritable(Table table, boolean writesToNonManagedTablesEnabled)
    {
        if (table.getTableType().equals(MATERIALIZED_VIEW.name())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot write to Hive materialized view");
        }

        if (!writesToNonManagedTablesEnabled && !table.getTableType().equals(MANAGED_TABLE.name())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot write to non-managed Hive table");
        }

        checkWritable(
                table.getSchemaTableName(),
                Optional.empty(),
                getProtectMode(table),
                table.getParameters(),
                table.getStorage());
    }

    public static void checkPartitionIsWritable(String partitionName, Partition partition)
    {
        checkWritable(
                partition.getSchemaTableName(),
                Optional.of(partitionName),
                getProtectMode(partition),
                partition.getParameters(),
                partition.getStorage());
    }

    private static void checkWritable(
            SchemaTableName tableName,
            Optional<String> partitionName,
            ProtectMode protectMode,
            Map<String, String> parameters,
            Storage storage)
    {
        String tablePartitionDescription = "Table '" + tableName + "'";
        if (partitionName.isPresent()) {
            tablePartitionDescription += " partition '" + partitionName.get() + "'";
        }

        // verify online
        verifyOnline(tableName, partitionName, protectMode, parameters);

        // verify not read only
        if (protectMode.readOnly()) {
            throw new HiveReadOnlyException(tableName, partitionName);
        }

        // verify skew info
        if (storage.isSkewed()) {
            throw new TrinoException(NOT_SUPPORTED, format("Inserting into bucketed tables with skew is not supported. %s", tablePartitionDescription));
        }
    }

    public static Location getTableDefaultLocation(SemiTransactionalHiveMetastore metastore, TrinoFileSystem fileSystem, String schemaName, String tableName)
    {
        Database database = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        return getTableDefaultLocation(database, fileSystem, schemaName, tableName);
    }

    public static Location getTableDefaultLocation(Database database, TrinoFileSystem fileSystem, String schemaName, String tableName)
    {
        Location location = database.getLocation().map(Location::of)
                .orElseThrow(() -> new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not set", schemaName)));

        if (!directoryExists(fileSystem, location).orElse(true)) {
            throw new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location does not exist: %s", schemaName, location));
        }

        return location.appendPath(escapeTableName(tableName));
    }

    public static Optional<Boolean> directoryExists(TrinoFileSystem fileSystem, Location path)
    {
        try {
            return fileSystem.directoryExists(path);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    public static boolean isFileCreatedByQuery(String fileName, String queryId)
    {
        return fileName.startsWith(queryId) || fileName.endsWith(queryId);
    }

    public static Optional<Location> createTemporaryPath(TrinoFileSystem fileSystem, ConnectorIdentity identity, Location targetPath, String temporaryStagingDirectoryPath)
    {
        // interpolate the username into the temporary directory path to avoid permission problems
        String temporaryPrefix = temporaryStagingDirectoryPath.replace("${USER}", identity.getUser());

        try {
            return fileSystem.createTemporaryDirectory(targetPath, temporaryPrefix, ".hive-staging");
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, e);
        }
    }

    public static boolean isWritableType(HiveType hiveType)
    {
        return isWritableType(hiveType.getTypeInfo());
    }

    private static boolean isWritableType(TypeInfo typeInfo)
    {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
                return isWritablePrimitiveType(primitiveCategory);
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return isWritableType(mapTypeInfo.getMapKeyTypeInfo()) && isWritableType(mapTypeInfo.getMapValueTypeInfo());
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                return isWritableType(listTypeInfo.getListElementTypeInfo());
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                return structTypeInfo.getAllStructFieldTypeInfos().stream().allMatch(HiveWriteUtils::isWritableType);
            case UNION:
                // unsupported for writing
        }
        return false;
    }

    private static boolean isWritablePrimitiveType(PrimitiveCategory primitiveCategory)
    {
        switch (primitiveCategory) {
            case BOOLEAN:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case DATE:
            case TIMESTAMP:
            case BINARY:
            case DECIMAL:
            case VARCHAR:
            case CHAR:
                return true;
            case VOID:
            case TIMESTAMPLOCALTZ:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case UNKNOWN:
                // unsupported for writing
                break;
        }
        return false;
    }
}
