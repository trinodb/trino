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
import io.trino.plugin.hive.HiveTimestampPrecision;
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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
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
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.UUID.randomUUID;

public final class HiveWriteUtils
{
    private HiveWriteUtils()
    {
    }

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            Object value = getField(partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
            if (value == null) {
                partitionValues.add(HIVE_DEFAULT_DYNAMIC_PARTITION);
            }
            else {
                String valueString = value.toString();
                if (!CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(valueString)) {
                    throw new TrinoException(HIVE_INVALID_PARTITION_VALUE,
                            "Hive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: " +
                                    base16().withSeparator(" ", 2).encode(valueString.getBytes(UTF_8)));
                }
                partitionValues.add(valueString);
            }
        }
        return partitionValues.build();
    }

    private static Object getField(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (BOOLEAN.equals(type)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (BIGINT.equals(type)) {
            return BIGINT.getLong(block, position);
        }
        if (INTEGER.equals(type)) {
            return INTEGER.getInt(block, position);
        }
        if (SMALLINT.equals(type)) {
            return SMALLINT.getShort(block, position);
        }
        if (TINYINT.equals(type)) {
            return TINYINT.getByte(block, position);
        }
        if (REAL.equals(type)) {
            return REAL.getFloat(block, position);
        }
        if (DOUBLE.equals(type)) {
            return DOUBLE.getDouble(block, position);
        }
        if (type instanceof VarcharType varcharType) {
            return new Text(varcharType.getSlice(block, position).getBytes());
        }
        if (type instanceof CharType charType) {
            return new Text(padSpaces(charType.getSlice(block, position), charType).toStringUtf8());
        }
        if (VARBINARY.equals(type)) {
            return VARBINARY.getSlice(block, position).getBytes();
        }
        if (DATE.equals(type)) {
            return Date.ofEpochDay(DATE.getInt(block, position));
        }
        if (type instanceof TimestampType timestampType) {
            return getHiveTimestamp(timestampType, block, position);
        }
        if (type instanceof TimestampWithTimeZoneType) {
            checkArgument(type.equals(TIMESTAMP_TZ_MILLIS));
            return getHiveTimestampTz(block, position);
        }
        if (type instanceof DecimalType decimalType) {
            return getHiveDecimal(decimalType, block, position);
        }
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            Block arrayBlock = block.getObject(position, Block.class);
            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(getField(elementType, arrayBlock, i));
            }
            return unmodifiableList(list);
        }
        if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            Block mapBlock = block.getObject(position, Block.class);
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(getField(keyType, mapBlock, i),
                        getField(valueType, mapBlock, i + 1));
            }
            return unmodifiableMap(map);
        }
        if (type instanceof RowType rowType) {
            List<Type> fieldTypes = rowType.getTypeParameters();
            Block rowBlock = block.getObject(position, Block.class);
            verify(fieldTypes.size() == rowBlock.getPositionCount(), "expected row value field count does not match type field count");
            List<Object> row = new ArrayList<>(rowBlock.getPositionCount());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                row.add(getField(fieldTypes.get(i), rowBlock, i));
            }
            return unmodifiableList(row);
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
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

    public static void checkedDelete(FileSystem fileSystem, Path file, boolean recursive)
            throws IOException
    {
        try {
            if (!fileSystem.delete(file, recursive)) {
                if (fileSystem.exists(file)) {
                    // only throw exception if file still exists
                    throw new IOException("Failed to delete " + file);
                }
            }
        }
        catch (FileNotFoundException ignored) {
            // ok
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

    private static HiveDecimal getHiveDecimal(DecimalType decimalType, Block block, int position)
    {
        BigInteger unscaledValue;
        if (decimalType.isShort()) {
            unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
        }
        else {
            unscaledValue = ((Int128) decimalType.getObject(block, position)).toBigInteger();
        }
        return HiveDecimal.create(unscaledValue, decimalType.getScale());
    }

    private static Timestamp getHiveTimestamp(TimestampType type, Block block, int position)
    {
        verify(type.getPrecision() <= HiveTimestampPrecision.MAX.getPrecision(), "Timestamp precision too high for Hive");

        long epochMicros;
        int nanosOfMicro;
        if (type.isShort()) {
            epochMicros = type.getLong(block, position);
            nanosOfMicro = 0;
        }
        else {
            LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
            epochMicros = timestamp.getEpochMicros();
            nanosOfMicro = timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
        }

        long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int microsOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND);
        int nanosOfSecond = microsOfSecond * NANOSECONDS_PER_MICROSECOND + nanosOfMicro;
        return Timestamp.ofEpochSecond(epochSeconds, nanosOfSecond);
    }

    private static Timestamp getHiveTimestampTz(Block block, int position)
    {
        long epochMillis = unpackMillisUtc(block.getLong(position, 0));
        return Timestamp.ofEpochMilli(epochMillis);
    }
}
