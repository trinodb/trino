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
package io.trino.hive.formats.rcfile;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.testing.TempFile;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.hadoop.HadoopNative;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.hive.formats.encodings.binary.BinaryColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextColumnEncodingFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Functions.constant;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.advance;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.hive.formats.FormatTestUtils.COMPRESSION;
import static io.trino.hive.formats.FormatTestUtils.assertColumnValueEquals;
import static io.trino.hive.formats.FormatTestUtils.decodeRecordReaderValue;
import static io.trino.hive.formats.FormatTestUtils.getJavaObjectInspector;
import static io.trino.hive.formats.FormatTestUtils.toHiveWriteValue;
import static io.trino.hive.formats.FormatTestUtils.writeTrinoValue;
import static io.trino.hive.formats.ReadWriteUtils.findFirstSyncPosition;
import static io.trino.hive.formats.compression.CompressionKind.LZOP;
import static io.trino.hive.formats.rcfile.RcFileWriter.PRESTO_RCFILE_WRITER_VERSION;
import static io.trino.hive.formats.rcfile.RcFileWriter.PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.mapred.Reporter.NULL;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("StaticPseudoFunctionalStyleMethod")
public class RcFileTester
{
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    static {
        HadoopNative.requireHadoopNative();
    }

    public enum Format
    {
        BINARY {
            @Override
            public Serializer createSerializer()
            {
                return new LazyBinaryColumnarSerDe();
            }

            @Override
            public ColumnEncodingFactory getVectorEncoding()
            {
                return new BinaryColumnEncodingFactory(HIVE_STORAGE_TIME_ZONE);
            }
        },

        TEXT {
            @Override
            public Serializer createSerializer()
            {
                try {
                    ColumnarSerDe columnarSerDe = new ColumnarSerDe();
                    Properties tableProperties = new Properties();
                    tableProperties.setProperty("columns", "test");
                    tableProperties.setProperty("columns.types", "string");
                    columnarSerDe.initialize(new JobConf(false), tableProperties);
                    return columnarSerDe;
                }
                catch (SerDeException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public ColumnEncodingFactory getVectorEncoding()
            {
                return new TextColumnEncodingFactory();
            }
        };

        public abstract Serializer createSerializer();

        public abstract ColumnEncodingFactory getVectorEncoding();
    }

    private boolean structTestsEnabled;
    private boolean mapTestsEnabled;
    private boolean listTestsEnabled;
    private boolean complexStructuralTestsEnabled;
    private boolean readLastBatchOnlyEnabled;
    private List<Optional<CompressionKind>> compressions = ImmutableList.of();

    public static RcFileTester quickTestRcFileReader()
    {
        RcFileTester rcFileTester = new RcFileTester();
        rcFileTester.structTestsEnabled = true;
        rcFileTester.mapTestsEnabled = true;
        rcFileTester.listTestsEnabled = true;
        rcFileTester.complexStructuralTestsEnabled = false;
        rcFileTester.readLastBatchOnlyEnabled = false;
        rcFileTester.compressions = ImmutableList.of(Optional.empty(), Optional.of(CompressionKind.ZSTD));
        return rcFileTester;
    }

    public static RcFileTester fullTestRcFileReader()
    {
        RcFileTester rcFileTester = new RcFileTester();
        rcFileTester.structTestsEnabled = true;
        rcFileTester.mapTestsEnabled = true;
        rcFileTester.listTestsEnabled = true;
        rcFileTester.complexStructuralTestsEnabled = true;
        rcFileTester.readLastBatchOnlyEnabled = true;
        // These compression algorithms were chosen to cover the three different
        // cases: uncompressed, aircompressor, and hadoop compression
        // We assume that the compression algorithms generally work
        rcFileTester.compressions = COMPRESSION;
        return rcFileTester;
    }

    public static void testLzopDisabled()
            throws Exception
    {
        for (Format format : Format.values()) {
            try (TempFile tempFile = new TempFile()) {
                assertThatThrownBy(() -> new RcFileWriter(
                        new FileOutputStream(tempFile.file()),
                        ImmutableList.of(VarcharType.VARCHAR),
                        format.getVectorEncoding(),
                        Optional.of(LZOP),
                        ImmutableMap.of(),
                        false))
                        .isInstanceOf(IllegalArgumentException.class);
            }

            try (TempFile tempFile = new TempFile()) {
                writeRcFileColumnOld(tempFile.file(), format, Optional.of(LZOP), VarcharType.VARCHAR, ImmutableList.of("test").iterator());
                assertThatThrownBy(() -> new RcFileReader(
                        new LocalInputFile(tempFile.file()),
                        format.getVectorEncoding(),
                        ImmutableMap.of(0, VarcharType.VARCHAR),
                        0,
                        tempFile.file().length()))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    public void testRoundTrip(Type type, Iterable<?> writeValues, Format... skipFormats)
            throws Exception
    {
        ImmutableSet<Format> skipFormatsSet = ImmutableSet.copyOf(skipFormats);

        // just the values
        testRoundTripType(type, writeValues, skipFormatsSet);

        // all nulls
        assertRoundTrip(type, transform(writeValues, constant(null)), skipFormatsSet);

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(type, writeValues, skipFormatsSet);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            Iterable<Object> simpleStructs = transform(insertNullEvery(5, writeValues), RcFileTester::toHiveStruct);
            testRoundTripType(
                    RowType.from(ImmutableList.of(RowType.field("field", createRowType(type)))),
                    transform(simpleStructs, Collections::singletonList),
                    skipFormatsSet);
        }

        // values wrapped in map
        if (mapTestsEnabled) {
            testMapRoundTrip(type, writeValues, skipFormatsSet);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(type, writeValues, skipFormatsSet);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(
                    createListType(type),
                    transform(writeValues, RcFileTester::toHiveList),
                    skipFormatsSet);
        }
    }

    private void testStructRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // values in simple struct and mix in some null values
        testRoundTripType(
                createRowType(type),
                transform(insertNullEvery(5, writeValues), RcFileTester::toHiveStruct),
                skipFormats);
    }

    private void testMapRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // json does not support null keys, so we just write the first value
        Object nullKeyWrite = Iterables.getFirst(writeValues, null);

        // values in simple map and mix in some null values
        testRoundTripType(
                createMapType(type),
                transform(insertNullEvery(5, writeValues), value -> toHiveMap(nullKeyWrite, value)),
                skipFormats);
    }

    private void testListRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // values in simple list and mix in some null values
        testRoundTripType(
                createListType(type),
                transform(insertNullEvery(5, writeValues), RcFileTester::toHiveList),
                skipFormats);
    }

    private void testRoundTripType(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // mix in some nulls
        assertRoundTrip(type, insertNullEvery(5, writeValues), skipFormats);
    }

    private void assertRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        List<?> finalValues = Lists.newArrayList(writeValues);

        Set<Format> formats = new LinkedHashSet<>(ImmutableSet.copyOf(Format.values()));
        formats.removeAll(skipFormats);

        for (Format format : formats) {
            for (Optional<CompressionKind> compression : compressions) {
                if (compression.equals(Optional.of(LZOP))) {
                    continue;
                }
                // write old, read new
                try (TempFile tempFile = new TempFile()) {
                    writeRcFileColumnOld(tempFile.file(), format, compression, type, finalValues.iterator());
                    assertFileContentsNew(type, tempFile, format, finalValues, false, ImmutableMap.of());
                }

                // write new, read old and new
                try (TempFile tempFile = new TempFile()) {
                    Map<String, String> metadata = ImmutableMap.of(String.valueOf(ThreadLocalRandom.current().nextLong()), String.valueOf(ThreadLocalRandom.current().nextLong()));
                    writeRcFileColumnNew(tempFile.file(), format, compression, type, finalValues.iterator(), metadata);

                    assertFileContentsOld(type, tempFile, format, finalValues);

                    Map<String, String> expectedMetadata = ImmutableMap.<String, String>builder()
                            .putAll(metadata)
                            .put(PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY, PRESTO_RCFILE_WRITER_VERSION)
                            .buildOrThrow();

                    assertFileContentsNew(type, tempFile, format, finalValues, false, expectedMetadata);

                    if (readLastBatchOnlyEnabled) {
                        assertFileContentsNew(type, tempFile, format, finalValues, true, expectedMetadata);
                    }
                }
            }
        }
    }

    private static void assertFileContentsNew(
            Type type,
            TempFile tempFile,
            Format format,
            List<?> expectedValues,
            boolean readLastBatchOnly,
            Map<String, String> metadata)
            throws IOException
    {
        try (RcFileReader recordReader = createRcFileReader(tempFile, type, format.getVectorEncoding())) {
            assertIndexOf(recordReader, tempFile.file());
            assertEquals(recordReader.getMetadata(), ImmutableMap.builder()
                    .putAll(metadata)
                    .put("hive.io.rcfile.column.number", "1")
                    .buildOrThrow());

            Iterator<?> iterator = expectedValues.iterator();
            int totalCount = 0;
            for (int batchSize = recordReader.advance(); batchSize >= 0; batchSize = recordReader.advance()) {
                totalCount += batchSize;
                if (readLastBatchOnly && totalCount == expectedValues.size()) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                }
                else {
                    Block block = recordReader.readBlock(0);

                    List<Object> data = new ArrayList<>(block.getPositionCount());
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                    }

                    for (int i = 0; i < batchSize; i++) {
                        assertTrue(iterator.hasNext());
                        Object expected = iterator.next();

                        Object actual = data.get(i);
                        assertColumnValueEquals(type, actual, expected);
                    }
                }
            }
            assertFalse(iterator.hasNext());
            assertEquals(recordReader.getRowsRead(), totalCount);
        }
    }

    private static void assertIndexOf(RcFileReader recordReader, File file)
            throws IOException
    {
        List<Long> syncPositionsBruteForce = getSyncPositionsBruteForce(recordReader, file);
        List<Long> syncPositionsSimple = getSyncPositionsSimple(recordReader, file);

        assertEquals(syncPositionsBruteForce, syncPositionsSimple);
    }

    private static List<Long> getSyncPositionsBruteForce(RcFileReader recordReader, File file)
    {
        Slice slice = Slices.allocate(toIntExact(file.length()));
        try (InputStream in = new FileInputStream(file)) {
            slice.setBytes(0, in, slice.length());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        List<Long> syncPositionsBruteForce = new ArrayList<>();
        Slice sync = Slices.allocate(SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG);
        sync.setInt(0, -1);
        sync.setBytes(SIZE_OF_INT, recordReader.getSync());

        long syncPosition = 0;
        while (syncPosition >= 0) {
            syncPosition = slice.indexOf(sync, toIntExact(syncPosition));
            if (syncPosition > 0) {
                syncPositionsBruteForce.add(syncPosition);
                syncPosition++;
            }
        }
        return syncPositionsBruteForce;
    }

    private static List<Long> getSyncPositionsSimple(RcFileReader recordReader, File file)
            throws IOException
    {
        List<Long> syncPositions = new ArrayList<>();
        Slice sync = recordReader.getSync();
        long syncFirst = sync.getLong(0);
        long syncSecond = sync.getLong(8);
        long syncPosition = 0;
        TrinoInputFile inputFile = new LocalInputFile(file);
        while (syncPosition >= 0) {
            syncPosition = findFirstSyncPosition(inputFile, syncPosition, file.length() - syncPosition, syncFirst, syncSecond);
            if (syncPosition > 0) {
                assertEquals(findFirstSyncPosition(inputFile, syncPosition, 1, syncFirst, syncSecond), syncPosition);
                assertEquals(findFirstSyncPosition(inputFile, syncPosition, 2, syncFirst, syncSecond), syncPosition);
                assertEquals(findFirstSyncPosition(inputFile, syncPosition, 10, syncFirst, syncSecond), syncPosition);

                assertEquals(findFirstSyncPosition(inputFile, syncPosition - 1, 1, syncFirst, syncSecond), -1);
                assertEquals(findFirstSyncPosition(inputFile, syncPosition - 2, 2, syncFirst, syncSecond), -1);
                assertEquals(findFirstSyncPosition(inputFile, syncPosition + 1, 1, syncFirst, syncSecond), -1);

                syncPositions.add(syncPosition);
                syncPosition++;
            }
        }
        return syncPositions;
    }

    private static RcFileReader createRcFileReader(TempFile tempFile, Type type, ColumnEncodingFactory encoding)
            throws IOException
    {
        TrinoInputFile rcFileDataSource = new LocalInputFile(tempFile.file());
        RcFileReader rcFileReader = new RcFileReader(
                rcFileDataSource,
                encoding,
                ImmutableMap.of(0, type),
                0,
                tempFile.file().length());

        assertEquals(rcFileReader.getColumnCount(), 1);

        return rcFileReader;
    }

    private static void writeRcFileColumnNew(File outputFile, Format format, Optional<CompressionKind> compression, Type type, Iterator<?> values, Map<String, String> metadata)
            throws Exception
    {
        RcFileWriter writer = new RcFileWriter(
                new FileOutputStream(outputFile),
                ImmutableList.of(type),
                format.getVectorEncoding(),
                compression,
                metadata,
                DataSize.of(100, KILOBYTE),   // use a smaller size to create more row groups
                DataSize.of(200, KILOBYTE),
                true);
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1024);
        while (values.hasNext()) {
            Object value = values.next();
            writeTrinoValue(type, blockBuilder, value);
        }

        writer.write(new Page(blockBuilder.build()));
        writer.close();

        writer.validate(new LocalInputFile(outputFile));
    }

    private static <K extends LongWritable, V extends BytesRefArrayWritable> void assertFileContentsOld(
            Type type,
            TempFile tempFile,
            Format format,
            Iterable<?> expectedValues)
            throws Exception
    {
        JobConf configuration = new JobConf(newEmptyConfiguration());
        configuration.set(READ_COLUMN_IDS_CONF_STR, "0");
        configuration.setBoolean(READ_ALL_COLUMNS, false);

        Properties schema = new Properties();
        schema.setProperty(META_TABLE_COLUMNS, "test");
        schema.setProperty(META_TABLE_COLUMN_TYPES, getJavaObjectInspector(type).getTypeName());

        Deserializer deserializer;
        if (format == Format.BINARY) {
            deserializer = new LazyBinaryColumnarSerDe();
        }
        else {
            deserializer = new ColumnarSerDe();
        }
        deserializer.initialize(configuration, schema);
        configuration.set(SERIALIZATION_LIB, deserializer.getClass().getName());

        InputFormat<K, V> inputFormat = new RCFileInputFormat<>();
        RecordReader<K, V> recordReader = inputFormat.getRecordReader(
                new FileSplit(new Path(tempFile.file().getAbsolutePath()), 0, tempFile.file().length(), (String[]) null),
                configuration,
                NULL);

        K key = recordReader.createKey();
        V value = recordReader.createValue();

        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        StructField field = rowInspector.getStructFieldRef("test");

        Iterator<?> iterator = expectedValues.iterator();
        while (recordReader.next(key, value)) {
            Object expectedValue = iterator.next();

            Object rowData = deserializer.deserialize(value);
            Object actualValue = rowInspector.getStructFieldData(rowData, field);
            actualValue = decodeRecordReaderValue(type, actualValue, format == Format.BINARY ? Optional.of(HIVE_STORAGE_TIME_ZONE) : Optional.empty());
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
        assertFalse(iterator.hasNext());
    }

    private static void writeRcFileColumnOld(File outputFile, Format format, Optional<CompressionKind> compression, Type type, Iterator<?> values)
            throws Exception
    {
        ObjectInspector columnObjectInspector = getJavaObjectInspector(type);
        RecordWriter recordWriter = createRcFileWriterOld(outputFile, compression, columnObjectInspector);

        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", columnObjectInspector);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        Serializer serializer = format.createSerializer();

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", "test");
        tableProperties.setProperty("columns.types", objectInspector.getTypeName());
        serializer.initialize(new JobConf(false), tableProperties);

        while (values.hasNext()) {
            Object value = values.next();
            value = toHiveWriteValue(type, value, format == Format.BINARY ? Optional.of(HIVE_STORAGE_TIME_ZONE) : Optional.empty());
            objectInspector.setStructFieldData(row, fields.get(0), value);

            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
    }

    private static RecordWriter createRcFileWriterOld(File outputFile, Optional<CompressionKind> compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf(false);
        Optional<String> codecName = compression.map(CompressionKind::getHadoopClassName);
        codecName.ifPresent(s -> jobConf.set(COMPRESS_CODEC, s));

        return new RCFileOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                codecName.isPresent(),
                createTableProperties("test", columnObjectInspector.getTypeName()),
                () -> {});
    }

    private static SettableStructObjectInspector createSettableStructObjectInspector(String name, ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(objectInspector));
    }

    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        orderTableProperties.setProperty("file.inputformat", RCFileInputFormat.class.getName());
        return orderTableProperties;
    }

    private static <T> Iterable<T> insertNullEvery(int n, Iterable<T> iterable)
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

    private static RowType createRowType(Type type)
    {
        return RowType.from(ImmutableList.of(
                RowType.field("a", type),
                RowType.field("b", type),
                RowType.field("c", type)));
    }

    private static Object toHiveStruct(Object input)
    {
        List<Object> data = new ArrayList<>();
        data.add(input);
        data.add(input);
        data.add(input);
        return data;
    }

    private static MapType createMapType(Type type)
    {
        return (MapType) TESTING_TYPE_MANAGER.getParameterizedType(MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(type.getTypeSignature()),
                TypeSignatureParameter.typeParameter(type.getTypeSignature())));
    }

    private static Object toHiveMap(Object nullKeyValue, Object input)
    {
        Map<Object, Object> map = new HashMap<>();
        if (input == null) {
            // json doesn't support null keys, so just write the nullKeyValue
            map.put(nullKeyValue, null);
        }
        else {
            map.put(input, input);
        }
        return map;
    }

    private static ArrayType createListType(Type type)
    {
        return new ArrayType(type);
    }

    private static Object toHiveList(Object input)
    {
        return nCopies(4, input);
    }
}
