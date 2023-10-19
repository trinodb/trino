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
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.rubix.CachingTrinoS3FileSystem;
import io.trino.hdfs.s3.TrinoS3FileSystem;
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
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

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
import static io.trino.hdfs.FileSystemUtils.getRawFileSystem;
import static io.trino.hdfs.s3.HiveS3Module.EMR_FS_CLASS_NAME;
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
import static java.util.UUID.randomUUID;

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

    public static Location getTableDefaultLocation(HdfsContext context, SemiTransactionalHiveMetastore metastore, HdfsEnvironment hdfsEnvironment, String schemaName, String tableName)
    {
        Database database = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        return getTableDefaultLocation(database, context, hdfsEnvironment, schemaName, tableName);
    }

    public static Location getTableDefaultLocation(Database database, HdfsContext context, HdfsEnvironment hdfsEnvironment, String schemaName, String tableName)
    {
        String location = database.getLocation()
                .orElseThrow(() -> new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not set", schemaName)));

        Path databasePath = new Path(location);
        if (!isS3FileSystem(context, hdfsEnvironment, databasePath)) {
            if (!pathExists(context, hdfsEnvironment, databasePath)) {
                throw new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location does not exist: %s", schemaName, databasePath));
            }
            if (!isDirectory(context, hdfsEnvironment, databasePath)) {
                throw new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not a directory: %s", schemaName, databasePath));
            }
        }

        // Note: this results in `databaseLocation` being a "normalized location", e.g. not containing double slashes.
        // TODO (https://github.com/trinodb/trino/issues/17803): We need to use normalized location until all relevant Hive connector components are migrated off Hadoop Path.
        Location databaseLocation = Location.of(databasePath.toString());
        return databaseLocation.appendPath(escapeTableName(tableName));
    }

    public static boolean pathExists(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(context, path).exists(path);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    public static boolean isS3FileSystem(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            FileSystem fileSystem = getRawFileSystem(hdfsEnvironment.getFileSystem(context, path));
            return fileSystem instanceof TrinoS3FileSystem || fileSystem.getClass().getName().equals(EMR_FS_CLASS_NAME) || fileSystem instanceof CachingTrinoS3FileSystem;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    public static boolean isViewFileSystem(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return getRawFileSystem(hdfsEnvironment.getFileSystem(context, path)) instanceof ViewFileSystem;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    private static boolean isDirectory(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(context, path).isDirectory(path);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    public static boolean isHdfsEncrypted(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            FileSystem fileSystem = getRawFileSystem(hdfsEnvironment.getFileSystem(context, path));
            if (fileSystem instanceof DistributedFileSystem) {
                return ((DistributedFileSystem) fileSystem).getEZForPath(path) != null;
            }
            return false;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking encryption status for path: " + path, e);
        }
    }

    public static boolean isFileCreatedByQuery(String fileName, String queryId)
    {
        return fileName.startsWith(queryId) || fileName.endsWith(queryId);
    }

    public static Location createTemporaryPath(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path targetPath, String temporaryStagingDirectoryPath)
    {
        // use a per-user temporary directory to avoid permission problems
        String temporaryPrefix = temporaryStagingDirectoryPath.replace("${USER}", context.getIdentity().getUser());

        // use relative temporary directory on ViewFS
        if (isViewFileSystem(context, hdfsEnvironment, targetPath)) {
            temporaryPrefix = ".hive-staging";
        }

        // create a temporary directory on the same filesystem
        Path temporaryRoot = new Path(targetPath, temporaryPrefix);
        Path temporaryPath = new Path(temporaryRoot, randomUUID().toString());

        createDirectory(context, hdfsEnvironment, temporaryPath);

        if (hdfsEnvironment.isNewFileInheritOwnership()) {
            setDirectoryOwner(context, hdfsEnvironment, temporaryPath, targetPath);
        }

        return Location.of(temporaryPath.toString());
    }

    private static void setDirectoryOwner(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path, Path targetPath)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, path);
            FileStatus fileStatus;
            if (!fileSystem.exists(targetPath)) {
                // For new table
                Path parent = targetPath.getParent();
                if (!fileSystem.exists(parent)) {
                    return;
                }
                fileStatus = fileSystem.getFileStatus(parent);
            }
            else {
                // For existing table
                fileStatus = fileSystem.getFileStatus(targetPath);
            }
            String owner = fileStatus.getOwner();
            String group = fileStatus.getGroup();
            fileSystem.setOwner(path, owner, group);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Failed to set owner on %s based on %s", path, targetPath), e);
        }
    }

    public static void createDirectory(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            if (!hdfsEnvironment.getFileSystem(context, path).mkdirs(path, hdfsEnvironment.getNewDirectoryPermissions().orElse(null))) {
                throw new IOException("mkdirs returned false");
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to create directory: " + path, e);
        }

        if (hdfsEnvironment.getNewDirectoryPermissions().isPresent()) {
            // explicitly set permission since the default umask overrides it on creation
            try {
                hdfsEnvironment.getFileSystem(context, path).setPermission(path, hdfsEnvironment.getNewDirectoryPermissions().get());
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to set permission on directory: " + path, e);
            }
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
