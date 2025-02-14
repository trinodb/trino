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
package io.trino.plugin.hive.parquet;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.write.MapKeyValuesSchemaConverter;
import io.trino.plugin.hive.parquet.write.SingleLevelArrayMapKeyValuesSchemaConverter;
import io.trino.plugin.hive.parquet.write.SingleLevelArraySchemaConverter;
import io.trino.plugin.hive.parquet.write.TestingMapredParquetOutputFormat;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Functions.constant;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetWriteValidation.ParquetWriteValidationBuilder;
import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING;
import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.trino.plugin.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.parquet.ParquetUtil.createPageSource;
import static io.trino.plugin.hive.util.HiveUtil.isStructuralType;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfosFromTypeString;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.CompressionCodec.GZIP;
import static org.apache.parquet.format.CompressionCodec.LZ4;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.CompressionCodec.ZSTD;
import static org.apache.parquet.hadoop.ParquetOutputFormat.COMPRESSION;
import static org.apache.parquet.hadoop.ParquetOutputFormat.ENABLE_DICTIONARY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

class ParquetTester
{
    private static final int MAX_PRECISION_INT64 = toIntExact(maxPrecision(8));

    private static final ConnectorSession SESSION = getHiveSession(createHiveConfig(false));

    private static final ConnectorSession SESSION_USE_NAME = getHiveSession(createHiveConfig(true));

    public static final List<String> TEST_COLUMN = singletonList("test");

    private final Set<CompressionCodec> compressions;

    private final Set<CompressionCodec> writerCompressions;

    private final Set<WriterVersion> versions;

    private final Set<ConnectorSession> sessions;

    public static ParquetTester quickParquetTester()
    {
        return new ParquetTester(
                ImmutableSet.of(GZIP),
                ImmutableSet.of(GZIP),
                ImmutableSet.of(PARQUET_1_0),
                ImmutableSet.of(SESSION));
    }

    public static ParquetTester fullParquetTester()
    {
        return new ParquetTester(
                ImmutableSet.of(GZIP, UNCOMPRESSED, SNAPPY, LZ4, ZSTD),
                ImmutableSet.of(GZIP, UNCOMPRESSED, SNAPPY, ZSTD),
                ImmutableSet.copyOf(WriterVersion.values()),
                ImmutableSet.of(SESSION, SESSION_USE_NAME));
    }

    public ParquetTester(
            Set<CompressionCodec> compressions,
            Set<CompressionCodec> writerCompressions,
            Set<WriterVersion> versions,
            Set<ConnectorSession> sessions)
    {
        this.compressions = requireNonNull(compressions, "compressions is null");
        this.writerCompressions = requireNonNull(writerCompressions, "writerCompressions is null");
        this.versions = requireNonNull(versions, "writerCompressions is null");
        this.sessions = requireNonNull(sessions, "sessions is null");
    }

    public void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<?> writeValues, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, writeValues, parameterType);
    }

    public <W, R> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<W> writeValues, Function<W, R> readTransform, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, readTransform::apply), parameterType);
    }

    public void testSingleLevelArraySchemaRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        List<TypeInfo> typeInfos = getTypeInfosFromTypeString(objectInspector.getTypeName());
        MessageType schema = SingleLevelArraySchemaConverter.convert(TEST_COLUMN, typeInfos);
        testRoundTrip(objectInspector, writeValues, readValues, getOnlyElement(TEST_COLUMN), type, Optional.of(schema), ParquetSchemaOptions.withSingleLevelArray());
        if (objectInspector.getTypeName().contains("map<")) {
            schema = SingleLevelArrayMapKeyValuesSchemaConverter.convert(TEST_COLUMN, typeInfos);
            testRoundTrip(objectInspector, writeValues, readValues, getOnlyElement(TEST_COLUMN), type, Optional.of(schema), ParquetSchemaOptions.withSingleLevelArray());
        }
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        testRoundTrip(objectInspector, writeValues, readValues, type, Optional.empty());

        if (objectInspector.getTypeName().contains("map<")) {
            List<TypeInfo> typeInfos = getTypeInfosFromTypeString(objectInspector.getTypeName());
            MessageType schema = MapKeyValuesSchemaConverter.convert(TEST_COLUMN, typeInfos);
            testRoundTrip(objectInspector, writeValues, readValues, type, Optional.of(schema));
        }
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type, Optional<MessageType> parquetSchema)
            throws Exception
    {
        testRoundTrip(
                objectInspector,
                writeValues,
                readValues,
                getOnlyElement(TEST_COLUMN),
                type,
                parquetSchema,
                ParquetSchemaOptions.defaultOptions());
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, String columnName, Type type, Optional<MessageType> parquetSchema)
            throws Exception
    {
        testRoundTrip(
                objectInspector,
                writeValues,
                readValues,
                columnName,
                type,
                parquetSchema,
                ParquetSchemaOptions.defaultOptions());
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, String columnName, Type type, Optional<MessageType> parquetSchema, ParquetSchemaOptions schemaOptions)
            throws Exception
    {
        testRoundTrip(
                singletonList(objectInspector),
                new Iterable<?>[] {writeValues},
                new Iterable<?>[] {readValues},
                singletonList(columnName),
                singletonList(type),
                parquetSchema,
                schemaOptions);
    }

    public void testRoundTrip(List<ObjectInspector> objectInspectors, Iterable<?>[] writeValues, Iterable<?>[] readValues, List<String> columnNames, List<Type> columnTypes, Optional<MessageType> parquetSchema, ParquetSchemaOptions schemaOptions)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspectors, writeValues, readValues, columnNames, columnTypes, parquetSchema, schemaOptions);

        // all nulls
        assertRoundTrip(objectInspectors, transformToNulls(writeValues), transformToNulls(readValues), columnNames, columnTypes, parquetSchema, schemaOptions);
    }

    private void testRoundTripType(
            List<ObjectInspector> objectInspectors,
            Iterable<?>[] writeValues,
            Iterable<?>[] readValues,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<MessageType> parquetSchema,
            ParquetSchemaOptions schemaOptions)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspectors, writeValues, readValues, columnNames, columnTypes, parquetSchema, schemaOptions);

        // reverse order
        assertRoundTrip(objectInspectors, reverse(writeValues), reverse(readValues), columnNames, columnTypes, parquetSchema, schemaOptions);

        // forward order with nulls
        assertRoundTrip(objectInspectors, insertNullEvery(5, writeValues), insertNullEvery(5, readValues), columnNames, columnTypes, parquetSchema, schemaOptions);

        // reverse order with nulls
        assertRoundTrip(objectInspectors, insertNullEvery(5, reverse(writeValues)), insertNullEvery(5, reverse(readValues)), columnNames, columnTypes, parquetSchema, schemaOptions);
    }

    void assertRoundTrip(
            ObjectInspector objectInspectors,
            Iterable<?> writeValues,
            Iterable<?> readValues,
            String columnName,
            Type columnType,
            Optional<MessageType> parquetSchema)
            throws Exception
    {
        assertRoundTrip(
                singletonList(objectInspectors),
                new Iterable[] {writeValues},
                new Iterable[] {readValues},
                singletonList(columnName),
                singletonList(columnType),
                parquetSchema,
                ParquetSchemaOptions.defaultOptions());
    }

    void assertRoundTrip(
            List<ObjectInspector> objectInspectors,
            Iterable<?>[] writeValues,
            Iterable<?>[] readValues,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<MessageType> parquetSchema,
            ParquetSchemaOptions schemaOptions)
            throws Exception
    {
        assertRoundTripWithHiveWriter(objectInspectors, writeValues, readValues, columnNames, columnTypes, parquetSchema, schemaOptions);

        // write Trino parquet
        for (CompressionCodec compressionCodec : writerCompressions) {
            for (ConnectorSession session : sessions) {
                try (TempFile tempFile = new TempFile("test", "parquet")) {
                    OptionalInt min = stream(writeValues).mapToInt(Iterables::size).min();
                    checkState(min.isPresent());
                    writeParquetColumnTrino(tempFile.getFile(), columnTypes, columnNames, getIterators(readValues), min.getAsInt(), compressionCodec, schemaOptions);
                    assertFileContents(
                            session,
                            tempFile.getFile(),
                            getIterators(readValues),
                            columnNames,
                            columnTypes);
                }
            }
        }
    }

    // Certain tests need the ability to specify a parquet schema which the writer wouldn't choose by itself based on the engine type.
    // Explicitly provided parquetSchema is supported only by the hive writer.
    // This method should be used when we need to assert that an exception should be thrown when reading from a file written with the specified
    // parquetSchema to avoid getting misled due to an exception thrown when from reading the file produced by trino parquet writer which may not
    // be following the specified parquetSchema.
    void assertRoundTripWithHiveWriter(
            List<ObjectInspector> objectInspectors,
            Iterable<?>[] writeValues,
            Iterable<?>[] readValues,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<MessageType> parquetSchema,
            ParquetSchemaOptions schemaOptions)
            throws Exception
    {
        for (WriterVersion version : versions) {
            for (CompressionCodec compressionCodec : compressions) {
                for (ConnectorSession session : sessions) {
                    try (TempFile tempFile = new TempFile("test", "parquet")) {
                        JobConf jobConf = new JobConf(false);
                        jobConf.setEnum(COMPRESSION, compressionCodec);
                        jobConf.setBoolean(ENABLE_DICTIONARY, true);
                        jobConf.setEnum(WRITER_VERSION, version);
                        writeParquetColumn(
                                jobConf,
                                tempFile.getFile(),
                                compressionCodec,
                                createTableProperties(columnNames, objectInspectors),
                                getStandardStructObjectInspector(columnNames, objectInspectors),
                                getIterators(writeValues),
                                parquetSchema,
                                schemaOptions.isSingleLevelArray(),
                                DateTimeZone.getDefault());
                        assertFileContents(
                                session,
                                tempFile.getFile(),
                                getIterators(readValues),
                                columnNames,
                                columnTypes);
                    }
                }
            }
        }
    }

    void testMaxReadBytes(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type, DataSize maxReadBlockSize)
            throws Exception
    {
        assertMaxReadBytes(
                singletonList(objectInspector),
                new Iterable<?>[] {writeValues},
                new Iterable<?>[] {readValues},
                TEST_COLUMN,
                singletonList(type),
                Optional.empty(),
                maxReadBlockSize);
    }

    void assertMaxReadBytes(
            List<ObjectInspector> objectInspectors,
            Iterable<?>[] writeValues,
            Iterable<?>[] readValues,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<MessageType> parquetSchema,
            DataSize maxReadBlockSize)
            throws Exception
    {
        CompressionCodec compressionCodec = UNCOMPRESSED;
        HiveSessionProperties hiveSessionProperties = new HiveSessionProperties(
                new HiveConfig()
                        .setHiveStorageFormat(HiveStorageFormat.PARQUET)
                        .setUseParquetColumnNames(false),
                new OrcReaderConfig(),
                new OrcWriterConfig(),
                new ParquetReaderConfig()
                        .setMaxReadBlockSize(maxReadBlockSize),
                new ParquetWriterConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(hiveSessionProperties.getSessionProperties())
                .build();

        try (TempFile tempFile = new TempFile("test", "parquet")) {
            JobConf jobConf = new JobConf(false);
            jobConf.setEnum(COMPRESSION, compressionCodec);
            jobConf.setBoolean(ENABLE_DICTIONARY, true);
            jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);
            writeParquetColumn(
                    jobConf,
                    tempFile.getFile(),
                    compressionCodec,
                    createTableProperties(columnNames, objectInspectors),
                    getStandardStructObjectInspector(columnNames, objectInspectors),
                    getIterators(writeValues),
                    parquetSchema,
                    false,
                    DateTimeZone.getDefault());

            Iterator<?>[] expectedValues = getIterators(readValues);
            try (ConnectorPageSource pageSource = createPageSource(
                    session,
                    tempFile.getFile(),
                    columnNames,
                    columnTypes)) {
                assertPageSource(
                        columnTypes,
                        expectedValues,
                        pageSource,
                        Optional.of(getParquetMaxReadBlockSize(session).toBytes()));
                assertThat(stream(expectedValues).allMatch(Iterator::hasNext)).isFalse();
            }
        }
    }

    private void assertFileContents(
            ConnectorSession session,
            File dataFile,
            Iterator<?>[] expectedValues,
            List<String> columnNames,
            List<Type> columnTypes)
            throws IOException
    {
        try (ConnectorPageSource pageSource = createPageSource(
                session,
                dataFile,
                columnNames,
                columnTypes)) {
            if (pageSource instanceof RecordPageSource) {
                assertRecordCursor(columnTypes, expectedValues, ((RecordPageSource) pageSource).getCursor());
            }
            else {
                assertPageSource(columnTypes, expectedValues, pageSource);
            }
            assertThat(stream(expectedValues).allMatch(Iterator::hasNext)).isFalse();
        }
    }

    private static void assertPageSource(List<Type> types, Iterator<?>[] valuesByField, ConnectorPageSource pageSource)
    {
        assertPageSource(types, valuesByField, pageSource, Optional.empty());
    }

    private static void assertPageSource(List<Type> types, Iterator<?>[] valuesByField, ConnectorPageSource pageSource, Optional<Long> maxReadBlockSize)
    {
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            maxReadBlockSize.ifPresent(max ->
                    assertThat(page.getPositionCount() == 1 || page.getSizeInBytes() <= max).isTrue());

            for (int field = 0; field < page.getChannelCount(); field++) {
                Block block = page.getBlock(field);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    assertThat(valuesByField[field].hasNext()).isTrue();
                    Object expected = valuesByField[field].next();
                    Object actual = decodeObject(types.get(field), block, i);
                    assertThat(actual).isEqualTo(expected);
                }
            }
        }
    }

    private static void assertRecordCursor(List<Type> types, Iterator<?>[] valuesByField, RecordCursor cursor)
    {
        while (cursor.advanceNextPosition()) {
            for (int field = 0; field < types.size(); field++) {
                assertThat(valuesByField[field].hasNext()).isTrue();
                Object expected = valuesByField[field].next();
                Object actual = getActualCursorValue(cursor, types.get(field), field);
                assertThat(actual).isEqualTo(expected);
            }
        }
    }

    private static Object getActualCursorValue(RecordCursor cursor, Type type, int field)
    {
        Object fieldFromCursor = getFieldFromCursor(cursor, type, field);
        if (fieldFromCursor == null) {
            return null;
        }
        if (isStructuralType(type)) {
            if (type instanceof ArrayType arrayType) {
                return toArrayValue((Block) fieldFromCursor, arrayType.getElementType());
            }
            if (type instanceof MapType mapType) {
                return toMapValue((SqlMap) fieldFromCursor, mapType.getKeyType(), mapType.getValueType());
            }
            if (type instanceof RowType) {
                return toRowValue((Block) fieldFromCursor, type.getTypeParameters());
            }
        }
        if (type instanceof DecimalType decimalType) {
            return new SqlDecimal((BigInteger) fieldFromCursor, decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof VarcharType) {
            return new String(((Slice) fieldFromCursor).getBytes(), UTF_8);
        }
        if (VARBINARY.equals(type)) {
            return new SqlVarbinary(((Slice) fieldFromCursor).getBytes());
        }
        if (DATE.equals(type)) {
            return new SqlDate(((Long) fieldFromCursor).intValue());
        }
        if (TIMESTAMP_MILLIS.equals(type)) {
            return SqlTimestamp.fromMillis(3, (long) fieldFromCursor);
        }
        return fieldFromCursor;
    }

    private static Object getFieldFromCursor(RecordCursor cursor, Type type, int field)
    {
        if (cursor.isNull(field)) {
            return null;
        }
        if (BOOLEAN.equals(type)) {
            return cursor.getBoolean(field);
        }
        if (TINYINT.equals(type)) {
            return cursor.getLong(field);
        }
        if (SMALLINT.equals(type)) {
            return cursor.getLong(field);
        }
        if (INTEGER.equals(type)) {
            return (int) cursor.getLong(field);
        }
        if (BIGINT.equals(type)) {
            return cursor.getLong(field);
        }
        if (REAL.equals(type)) {
            return intBitsToFloat((int) cursor.getLong(field));
        }
        if (DOUBLE.equals(type)) {
            return cursor.getDouble(field);
        }
        if (type instanceof VarcharType || type instanceof CharType || VARBINARY.equals(type)) {
            return cursor.getSlice(field);
        }
        if (DateType.DATE.equals(type)) {
            return cursor.getLong(field);
        }
        if (TimestampType.TIMESTAMP_MILLIS.equals(type)) {
            return cursor.getLong(field);
        }
        if (isStructuralType(type)) {
            return cursor.getObject(field);
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return BigInteger.valueOf(cursor.getLong(field));
            }
            return ((Int128) cursor.getObject(field)).toBigInteger();
        }
        throw new RuntimeException("unknown type");
    }

    private static Map<?, ?> toMapValue(SqlMap sqlMap, Type keyType, Type valueType)
    {
        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        Map<Object, Object> map = new HashMap<>(sqlMap.getSize());
        for (int i = 0; i < sqlMap.getSize(); i++) {
            map.put(keyType.getObjectValue(SESSION, rawKeyBlock, rawOffset + i), valueType.getObjectValue(SESSION, rawValueBlock, rawOffset + i));
        }
        return Collections.unmodifiableMap(map);
    }

    private static List<?> toArrayValue(Block arrayBlock, Type elementType)
    {
        List<Object> values = new ArrayList<>();
        for (int position = 0; position < arrayBlock.getPositionCount(); position++) {
            values.add(elementType.getObjectValue(SESSION, arrayBlock, position));
        }
        return Collections.unmodifiableList(values);
    }

    private static List<?> toRowValue(Block rowBlock, List<Type> fieldTypes)
    {
        List<Object> values = new ArrayList<>(rowBlock.getPositionCount());
        for (int i = 0; i < rowBlock.getPositionCount(); i++) {
            values.add(fieldTypes.get(i).getObjectValue(SESSION, rowBlock, i));
        }
        return Collections.unmodifiableList(values);
    }

    private static HiveConfig createHiveConfig(boolean useParquetColumnNames)
    {
        return new HiveConfig()
                .setHiveStorageFormat(HiveStorageFormat.PARQUET)
                .setUseParquetColumnNames(useParquetColumnNames);
    }

    public static void writeParquetColumn(
            JobConf jobConf,
            File outputFile,
            CompressionCodec compressionCodec,
            Properties tableProperties,
            SettableStructObjectInspector objectInspector,
            Iterator<?>[] valuesByField,
            Optional<MessageType> parquetSchema,
            boolean singleLevelArray,
            DateTimeZone dateTimeZone)
            throws Exception
    {
        RecordWriter recordWriter = new TestingMapredParquetOutputFormat(parquetSchema, singleLevelArray, dateTimeZone)
                .getHiveRecordWriter(
                        jobConf,
                        new Path(outputFile.toURI()),
                        Text.class,
                        compressionCodec != UNCOMPRESSED,
                        tableProperties,
                        () -> {});
        Object row = objectInspector.create();
        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        while (stream(valuesByField).allMatch(Iterator::hasNext)) {
            for (int field = 0; field < fields.size(); field++) {
                Object value = valuesByField[field].next();
                objectInspector.setStructFieldData(row, fields.get(field), value);
            }
            ParquetHiveSerDe serde = new ParquetHiveSerDe();
            serde.initialize(jobConf, tableProperties, null);
            Writable record = serde.serialize(row, objectInspector);
            recordWriter.write(record);
        }
        recordWriter.close(false);
    }

    public static Properties createTableProperties(List<String> columnNames, List<ObjectInspector> objectInspectors)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty(LIST_COLUMNS, Joiner.on(',').join(columnNames));
        orderTableProperties.setProperty(LIST_COLUMN_TYPES, Joiner.on(',').join(transform(objectInspectors, ObjectInspector::getTypeName)));
        return orderTableProperties;
    }

    public static class TempFile
            implements Closeable
    {
        private final File file;

        public TempFile(String prefix, String suffix)
        {
            try {
                file = File.createTempFile(prefix, suffix);
                verify(file.delete());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
        {
            if (!file.delete()) {
                verify(!file.exists());
            }
        }
    }

    private static Iterator<?>[] getIterators(Iterable<?>[] values)
    {
        return stream(values)
                .map(Iterable::iterator)
                .toArray(Iterator<?>[]::new);
    }

    private Iterable<?>[] transformToNulls(Iterable<?>[] values)
    {
        return stream(values)
                .map(v -> transform(v, constant(null)))
                .toArray(Iterable<?>[]::new);
    }

    private static Iterable<?>[] reverse(Iterable<?>[] iterables)
    {
        return stream(iterables)
                .map(ImmutableList::copyOf)
                .map(List::reversed)
                .toArray(Iterable<?>[]::new);
    }

    private static Iterable<?>[] insertNullEvery(int n, Iterable<?>[] iterables)
    {
        return stream(iterables)
                .map(itr -> insertNullEvery(n, itr))
                .toArray(Iterable<?>[]::new);
    }

    static <T> Iterable<T> insertNullEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        };
    }

    private static Object decodeObject(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return type.getObjectValue(SESSION, block, position);
    }

    private static void writeParquetColumnTrino(
            File outputFile,
            List<Type> types,
            List<String> columnNames,
            Iterator<?>[] values,
            int size,
            CompressionCodec compressionCodec,
            ParquetSchemaOptions schemaOptions)
            throws Exception
    {
        checkArgument(types.size() == columnNames.size() && types.size() == values.length);
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                types,
                columnNames,
                schemaOptions.useLegacyDecimalEncoding(),
                schemaOptions.useInt96TimestampEncoding());
        ParquetWriter writer = new ParquetWriter(
                new FileOutputStream(outputFile),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                ParquetWriterOptions.builder()
                        .setMaxPageSize(DataSize.ofBytes(2000))
                        .setMaxPageValueCount(200)
                        .setMaxBlockSize(DataSize.ofBytes(100000))
                        .build(),
                compressionCodec,
                "test-version",
                Optional.of(DateTimeZone.getDefault()),
                Optional.of(new ParquetWriteValidationBuilder(types, columnNames)));

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int i = 0; i < types.size(); ++i) {
            Type type = types.get(i);
            Iterator<?> iterator = values[i];
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);

            for (int j = 0; j < size; ++j) {
                checkState(iterator.hasNext());
                Object value = iterator.next();
                writeValue(type, blockBuilder, value);
            }
        }
        pageBuilder.declarePositions(size);
        writer.write(pageBuilder.build());
        writer.close();
        try {
            writer.validate(new TrinoParquetDataSource(new LocalInputFile(outputFile), new ParquetReaderOptions(), new FileFormatDataSourceStats()));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITE_VALIDATION_FAILED, e);
        }
    }

    private static void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    type.writeLong(blockBuilder, ((SqlDecimal) value).getUnscaledValue().longValue());
                }
                else if (Decimals.overflows(((SqlDecimal) value).getUnscaledValue(), MAX_PRECISION_INT64)) {
                    type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).toBigDecimal().unscaledValue()));
                }
                else {
                    type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).getUnscaledValue().longValue()));
                }
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(blockBuilder, ((Number) value).doubleValue());
            }
            else if (REAL.equals(type)) {
                float floatValue = ((Number) value).floatValue();
                type.writeLong(blockBuilder, Float.floatToIntBits(floatValue));
            }
            else if (type instanceof VarcharType) {
                Slice slice = truncateToLength(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (type instanceof CharType) {
                Slice slice = truncateToLengthAndTrimSpaces(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (VARBINARY.equals(type)) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer(((SqlVarbinary) value).getBytes()));
            }
            else if (DATE.equals(type)) {
                long days = ((SqlDate) value).getDays();
                type.writeLong(blockBuilder, days);
            }
            else if (TIMESTAMP_MILLIS.equals(type) || TIMESTAMP_MICROS.equals(type)) {
                type.writeLong(blockBuilder, ((SqlTimestamp) value).getEpochMicros());
            }
            else if (TIMESTAMP_NANOS.equals(type)) {
                type.writeObject(blockBuilder, new LongTimestamp(((SqlTimestamp) value).getEpochMicros(), ((SqlTimestamp) value).getPicosOfMicros()));
            }
            else {
                if (type instanceof ArrayType) {
                    List<?> array = (List<?>) value;
                    Type elementType = type.getTypeParameters().get(0);
                    ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                        for (Object elementValue : array) {
                            writeValue(elementType, elementBuilder, elementValue);
                        }
                    });
                }
                else if (type instanceof MapType) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Type keyType = type.getTypeParameters().get(0);
                    Type valueType = type.getTypeParameters().get(1);
                    ((MapBlockBuilder) blockBuilder).buildEntry((keyBuilder, valueBuilder) -> {
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            writeValue(keyType, keyBuilder, entry.getKey());
                            writeValue(valueType, valueBuilder, entry.getValue());
                        }
                    });
                }
                else if (type instanceof RowType) {
                    List<?> array = (List<?>) value;
                    List<Type> fieldTypes = type.getTypeParameters();
                    ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
                        for (int fieldId = 0; fieldId < fieldTypes.size(); fieldId++) {
                            Type fieldType = fieldTypes.get(fieldId);
                            writeValue(fieldType, fieldBuilders.get(fieldId), array.get(fieldId));
                        }
                    });
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        }
    }

    // copied from Parquet code to determine the max decimal precision supported by INT32/INT64
    private static long maxPrecision(int numBytes)
    {
        return Math.round(Math.floor(Math.log10(Math.pow(2, 8 * numBytes - 1) - 1)));
    }

    public static class ParquetSchemaOptions
    {
        private final boolean singleLevelArray;
        private final boolean useLegacyDecimalEncoding;
        private final boolean useInt96TimestampEncoding;

        private ParquetSchemaOptions(boolean singleLevelArray, boolean useLegacyDecimalEncoding, boolean useInt96TimestampEncoding)
        {
            this.singleLevelArray = singleLevelArray;
            this.useLegacyDecimalEncoding = useLegacyDecimalEncoding;
            this.useInt96TimestampEncoding = useInt96TimestampEncoding;
        }

        public static ParquetSchemaOptions defaultOptions()
        {
            return new ParquetSchemaOptions(false, HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING, HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING);
        }

        public static ParquetSchemaOptions withSingleLevelArray()
        {
            return new ParquetSchemaOptions(true, HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING, HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING);
        }

        public static ParquetSchemaOptions withInt64BackedTimestamps()
        {
            return new ParquetSchemaOptions(false, false, false);
        }

        public boolean isSingleLevelArray()
        {
            return singleLevelArray;
        }

        public boolean useLegacyDecimalEncoding()
        {
            return useLegacyDecimalEncoding;
        }

        public boolean useInt96TimestampEncoding()
        {
            return useInt96TimestampEncoding;
        }
    }
}
