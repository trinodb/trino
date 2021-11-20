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
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveReadOnlyException;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.avro.AvroRecordWriter;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.parquet.ParquetRecordWriter;
import io.trino.plugin.hive.rubix.CachingTrinoS3FileSystem;
import io.trino.plugin.hive.s3.TrinoS3FileSystem;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.BaseEncoding.base16;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_SERDE_NOT_FOUND;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.trino.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.plugin.hive.HiveSessionProperties.getTemporaryStagingDirectoryPath;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getProtectMode;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyOnline;
import static io.trino.plugin.hive.s3.HiveS3Module.EMR_FS_CLASS_NAME;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.isArrayType;
import static io.trino.plugin.hive.util.HiveUtil.isMapType;
import static io.trino.plugin.hive.util.HiveUtil.isRowType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MATERIALIZED_VIEW;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;

public final class HiveWriteUtils
{
    private HiveWriteUtils()
    {
    }

    public static RecordWriter createRecordWriter(Path target, JobConf conf, Properties properties, String outputFormatName, ConnectorSession session)
    {
        return createRecordWriter(target, conf, properties, outputFormatName, session, Optional.empty());
    }

    public static RecordWriter createRecordWriter(Path target, JobConf conf, Properties properties, String outputFormatName, ConnectorSession session, Optional<TextHeaderWriter> textHeaderWriter)
    {
        try {
            boolean compress = HiveConf.getBoolVar(conf, COMPRESSRESULT);
            if (outputFormatName.equals(MapredParquetOutputFormat.class.getName())) {
                return ParquetRecordWriter.create(target, conf, properties, session);
            }
            if (outputFormatName.equals(HiveIgnoreKeyTextOutputFormat.class.getName())) {
                return new TextRecordWriter(target, conf, properties, compress, textHeaderWriter);
            }
            if (outputFormatName.equals(HiveSequenceFileOutputFormat.class.getName())) {
                return new SequenceFileRecordWriter(target, conf, Text.class, compress);
            }
            if (outputFormatName.equals(AvroContainerOutputFormat.class.getName())) {
                return new AvroRecordWriter(target, conf, compress, properties);
            }
            Object writer = Class.forName(outputFormatName).getConstructor().newInstance();
            return ((HiveOutputFormat<?, ?>) writer).getHiveRecordWriter(conf, target, Text.class, compress, properties, Reporter.NULL);
        }
        catch (IOException | ReflectiveOperationException e) {
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    public static Serializer initializeSerializer(Configuration conf, Properties properties, String serializerName)
    {
        try {
            Serializer result = (Serializer) Class.forName(serializerName).getConstructor().newInstance();
            result.initialize(conf, properties);
            return result;
        }
        catch (ClassNotFoundException e) {
            throw new TrinoException(HIVE_SERDE_NOT_FOUND, "Serializer does not exist: " + serializerName);
        }
        catch (SerDeException | ReflectiveOperationException e) {
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    public static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        if (type.equals(BIGINT)) {
            return javaLongObjectInspector;
        }
        if (type.equals(INTEGER)) {
            return javaIntObjectInspector;
        }
        if (type.equals(SMALLINT)) {
            return javaShortObjectInspector;
        }
        if (type.equals(TINYINT)) {
            return javaByteObjectInspector;
        }
        if (type.equals(REAL)) {
            return javaFloatObjectInspector;
        }
        if (type.equals(DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        if (type instanceof VarcharType) {
            return writableStringObjectInspector;
        }
        if (type instanceof CharType) {
            return writableHiveCharObjectInspector;
        }
        if (type.equals(VARBINARY)) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(DATE)) {
            return javaDateObjectInspector;
        }
        if (type instanceof TimestampType) {
            return javaTimestampObjectInspector;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        if (isArrayType(type)) {
            return ObjectInspectorFactory.getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        if (isMapType(type)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        if (isRowType(type)) {
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    type.getTypeSignature().getParameters().stream()
                            .map(parameter -> parameter.getNamedTypeSignature().getName().get())
                            .collect(toImmutableList()),
                    type.getTypeParameters().stream()
                            .map(HiveWriteUtils::getJavaObjectInspector)
                            .collect(toImmutableList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            Object value = getField(DateTimeZone.UTC, partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
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

    public static Object getField(DateTimeZone localZone, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        if (BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        if (INTEGER.equals(type)) {
            return toIntExact(type.getLong(block, position));
        }
        if (SMALLINT.equals(type)) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        if (TINYINT.equals(type)) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        if (REAL.equals(type)) {
            return intBitsToFloat((int) type.getLong(block, position));
        }
        if (DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return new Text(type.getSlice(block, position).getBytes());
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return new Text(padSpaces(type.getSlice(block, position), charType).toStringUtf8());
        }
        if (VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        if (DATE.equals(type)) {
            return Date.ofEpochDay(toIntExact(type.getLong(block, position)));
        }
        if (type instanceof TimestampType) {
            return getHiveTimestamp(localZone, (TimestampType) type, block, position);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getHiveDecimal(decimalType, block, position);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(getField(localZone, elementType, arrayBlock, i));
            }
            return unmodifiableList(list);
        }
        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            Block mapBlock = block.getObject(position, Block.class);

            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(
                        getField(localZone, keyType, mapBlock, i),
                        getField(localZone, valueType, mapBlock, i + 1));
            }
            return unmodifiableMap(map);
        }
        if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            Block rowBlock = block.getObject(position, Block.class);
            checkCondition(
                    fieldTypes.size() == rowBlock.getPositionCount(),
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Expected row value field count does not match type field count");
            List<Object> row = new ArrayList<>(rowBlock.getPositionCount());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                row.add(getField(localZone, fieldTypes.get(i), rowBlock, i));
            }
            return unmodifiableList(row);
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    public static void checkTableIsWritable(Table table, boolean writesToNonManagedTablesEnabled)
    {
        if (table.getTableType().equals(MATERIALIZED_VIEW.toString())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot write to Hive materialized view");
        }

        if (!writesToNonManagedTablesEnabled && !table.getTableType().equals(MANAGED_TABLE.toString())) {
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
        if (protectMode.readOnly) {
            throw new HiveReadOnlyException(tableName, partitionName);
        }

        // verify skew info
        if (storage.isSkewed()) {
            throw new TrinoException(NOT_SUPPORTED, format("Inserting into bucketed tables with skew is not supported. %s", tablePartitionDescription));
        }
    }

    public static Path getTableDefaultLocation(HdfsContext context, SemiTransactionalHiveMetastore metastore, HdfsEnvironment hdfsEnvironment, String schemaName, String tableName)
    {
        Database database = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        return getTableDefaultLocation(database, context, hdfsEnvironment, schemaName, tableName);
    }

    public static Path getTableDefaultLocation(Database database, HdfsContext context, HdfsEnvironment hdfsEnvironment, String schemaName, String tableName)
    {
        Optional<String> location = database.getLocation();
        if (location.isEmpty()) {
            throw new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not set", schemaName));
        }

        Path databasePath = new Path(location.get());
        if (!isS3FileSystem(context, hdfsEnvironment, databasePath)) {
            if (!pathExists(context, hdfsEnvironment, databasePath)) {
                throw new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location does not exist: %s", schemaName, databasePath));
            }
            if (!isDirectory(context, hdfsEnvironment, databasePath)) {
                throw new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not a directory: %s", schemaName, databasePath));
            }
        }

        return new Path(databasePath, tableName);
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

    public static FileSystem getRawFileSystem(FileSystem fileSystem)
    {
        if (fileSystem instanceof FilterFileSystem) {
            return getRawFileSystem(((FilterFileSystem) fileSystem).getRawFileSystem());
        }
        return fileSystem;
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

    public static Path createTemporaryPath(ConnectorSession session, HdfsContext context, HdfsEnvironment hdfsEnvironment, Path targetPath)
    {
        // use a per-user temporary directory to avoid permission problems
        String temporaryPrefix = getTemporaryStagingDirectoryPath(session)
                .replace("${USER}", context.getIdentity().getUser());

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

        return temporaryPath;
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

    public static List<ObjectInspector> getRowColumnInspectors(List<Type> types)
    {
        return types.stream()
                .map(HiveWriteUtils::getRowColumnInspector)
                .collect(toList());
    }

    public static ObjectInspector getRowColumnInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return writableBooleanObjectInspector;
        }

        if (type.equals(BIGINT)) {
            return writableLongObjectInspector;
        }

        if (type.equals(INTEGER)) {
            return writableIntObjectInspector;
        }

        if (type.equals(SMALLINT)) {
            return writableShortObjectInspector;
        }

        if (type.equals(TINYINT)) {
            return writableByteObjectInspector;
        }

        if (type.equals(REAL)) {
            return writableFloatObjectInspector;
        }

        if (type.equals(DOUBLE)) {
            return writableDoubleObjectInspector;
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                // Unbounded VARCHAR is not supported by Hive.
                // Values for such columns must be stored as STRING in Hive
                return writableStringObjectInspector;
            }
            if (varcharType.getBoundedLength() <= HiveVarchar.MAX_VARCHAR_LENGTH) {
                // VARCHAR columns with the length less than or equal to 65535 are supported natively by Hive
                return getPrimitiveWritableObjectInspector(getVarcharTypeInfo(varcharType.getBoundedLength()));
            }
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            int charLength = charType.getLength();
            return getPrimitiveWritableObjectInspector(getCharTypeInfo(charLength));
        }

        if (type.equals(VARBINARY)) {
            return writableBinaryObjectInspector;
        }

        if (type.equals(DATE)) {
            return writableDateObjectInspector;
        }

        if (type instanceof TimestampType) {
            return writableTimestampObjectInspector;
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveWritableObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }

        if (isArrayType(type) || isMapType(type) || isRowType(type)) {
            return getJavaObjectInspector(type);
        }

        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static HiveDecimal getHiveDecimal(DecimalType decimalType, Block block, int position)
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

    private static Timestamp getHiveTimestamp(DateTimeZone localZone, TimestampType type, Block block, int position)
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

        long epochSeconds;
        if (DateTimeZone.UTC.equals(localZone)) {
            epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        }
        else {
            long localEpochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
            long utcEpochMillis = localZone.convertLocalToUTC(localEpochMillis, false);
            epochSeconds = floorDiv(utcEpochMillis, MILLISECONDS_PER_SECOND);
        }

        int microsOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND);
        int nanosOfSecond = microsOfSecond * NANOSECONDS_PER_MICROSECOND + nanosOfMicro;
        return Timestamp.ofEpochSecond(epochSeconds, nanosOfSecond);
    }
}
