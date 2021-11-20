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
package io.trino.orc;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcUtil;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcTester.Format.ORC_11;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.orc.TestingOrcPredicate.createOrcPredicate;
import static io.trino.orc.metadata.CompressionKind.LZ4;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.CompressionKind.SNAPPY;
import static io.trino.orc.metadata.CompressionKind.ZLIB;
import static io.trino.orc.metadata.CompressionKind.ZSTD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.rescale;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampTZObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class OrcTester
{
    public static final OrcReaderOptions READER_OPTIONS = new OrcReaderOptions()
            .withMaxReadBlockSize(DataSize.of(1, MEGABYTE))
            .withMaxMergeDistance(DataSize.of(1, MEGABYTE))
            .withMaxBufferSize(DataSize.of(1, MEGABYTE))
            .withStreamBufferSize(DataSize.of(1, MEGABYTE))
            .withTinyStripeThreshold(DataSize.of(1, MEGABYTE));
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    public enum Format
    {
        ORC_12, ORC_11
    }

    private boolean structTestsEnabled;
    private boolean mapTestsEnabled;
    private boolean listTestsEnabled;
    private boolean complexStructuralTestsEnabled;
    private boolean structuralNullTestsEnabled;
    private boolean reverseTestsEnabled;
    private boolean nullTestsEnabled;
    private boolean missingStructFieldsTestsEnabled;
    private boolean skipBatchTestsEnabled;
    private boolean skipStripeTestsEnabled;
    private Set<Format> formats = ImmutableSet.of();
    private Set<CompressionKind> compressions = ImmutableSet.of();

    public static OrcTester quickOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.missingStructFieldsTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.formats = ImmutableSet.of(ORC_12, ORC_11);
        orcTester.compressions = ImmutableSet.of(ZLIB);
        return orcTester;
    }

    public static OrcTester fullOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.complexStructuralTestsEnabled = true;
        orcTester.structuralNullTestsEnabled = true;
        orcTester.reverseTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.missingStructFieldsTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.skipStripeTestsEnabled = true;
        orcTester.formats = ImmutableSet.copyOf(Format.values());
        orcTester.compressions = ImmutableSet.of(NONE, SNAPPY, ZLIB, LZ4, ZSTD);
        return orcTester;
    }

    public void testRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        // just the values
        testRoundTripType(type, readValues);

        // all nulls
        if (nullTestsEnabled) {
            assertRoundTrip(
                    type,
                    readValues.stream()
                            .map(value -> null)
                            .collect(toList()));
        }

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(type, readValues);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(
                    rowType(type, type, type),
                    readValues.stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));
        }

        // values wrapped in map
        if (mapTestsEnabled && type.isComparable()) {
            testMapRoundTrip(type, readValues);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(type, readValues);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(
                    arrayType(type),
                    readValues.stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));
        }
    }

    private void testStructRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type rowType = rowType(type, type, type);
        // values in simple struct
        testRoundTripType(
                rowType,
                values.stream()
                        .map(OrcTester::toHiveStruct)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(
                    rowType,
                    insertNullEvery(5, values).stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));

            // all null values in simple struct
            testRoundTripType(
                    rowType,
                    values.stream()
                            .map(value -> toHiveStruct(null))
                            .collect(toList()));
        }

        if (missingStructFieldsTestsEnabled) {
            Type readType = rowType(type, type, type, type, type, type);
            Type writeType = rowType(type, type, type);

            List<?> writeValues = values.stream()
                    .map(OrcTester::toHiveStruct)
                    .collect(toList());

            List<?> readValues = values.stream()
                    .map(OrcTester::toHiveStructWithNull)
                    .collect(toList());

            assertRoundTrip(writeType, readType, writeValues, readValues);
        }
    }

    private void testMapRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type mapType = mapType(type, type);

        // maps cannot have a null key, so select a value to use for the map key when the value is null
        Object readNullKeyValue = Iterables.getLast(readValues);

        // values in simple map
        testRoundTripType(
                mapType,
                readValues.stream()
                        .map(value -> toHiveMap(value, readNullKeyValue))
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(
                    mapType,
                    insertNullEvery(5, readValues).stream()
                            .map(value -> toHiveMap(value, readNullKeyValue))
                            .collect(toList()));

            // all null values in simple map
            testRoundTripType(
                    mapType,
                    readValues.stream()
                            .map(value -> toHiveMap(null, readNullKeyValue))
                            .collect(toList()));
        }
    }

    private void testListRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type arrayType = arrayType(type);
        // values in simple list
        testRoundTripType(
                arrayType,
                readValues.stream()
                        .map(OrcTester::toHiveList)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(
                    arrayType,
                    insertNullEvery(5, readValues).stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));

            // all null values in simple list
            testRoundTripType(
                    arrayType,
                    readValues.stream()
                            .map(value -> toHiveList(null))
                            .collect(toList()));
        }
    }

    private void testRoundTripType(Type type, List<?> readValues)
            throws Exception
    {
        // forward order
        assertRoundTrip(type, readValues);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(type, reverse(readValues));
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(type, insertNullEvery(5, readValues));

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(type, insertNullEvery(5, reverse(readValues)));
            }
        }
    }

    private void assertRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues);
    }

    private void assertRoundTrip(Type writeType, Type readType, List<?> writeValues, List<?> readValues)
            throws Exception
    {
        OrcWriterStats stats = new OrcWriterStats();
        for (CompressionKind compression : compressions) {
            boolean hiveSupported = (compression != LZ4) && (compression != ZSTD) && !isTimestampTz(writeType) && !isTimestampTz(readType);

            for (Format format : formats) {
                // write Hive, read Trino
                if (hiveSupported) {
                    try (TempFile tempFile = new TempFile()) {
                        writeOrcColumnHive(tempFile.getFile(), format, compression, writeType, writeValues.iterator());
                        assertFileContentsTrino(readType, tempFile, readValues, false, false);
                    }
                }
            }

            // write Trino, read Hive and Trino
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnTrino(tempFile.getFile(), compression, writeType, writeValues.iterator(), stats);

                if (hiveSupported) {
                    assertFileContentsHive(readType, tempFile, readValues);
                }

                assertFileContentsTrino(readType, tempFile, readValues, false, false);

                if (skipBatchTestsEnabled) {
                    assertFileContentsTrino(readType, tempFile, readValues, true, false);
                }

                if (skipStripeTestsEnabled) {
                    assertFileContentsTrino(readType, tempFile, readValues, false, true);
                }
            }
        }

        assertEquals(stats.getWriterSizeInBytes(), 0);
    }

    private static void assertFileContentsTrino(
            Type type,
            TempFile tempFile,
            List<?> expectedValues,
            boolean skipFirstBatch,
            boolean skipStripe)
            throws IOException
    {
        try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, createOrcPredicate(type, expectedValues), type, MAX_BATCH_SIZE)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            boolean isFirst = true;
            int rowsProcessed = 0;
            Iterator<?> iterator = expectedValues.iterator();
            for (Page page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
                int batchSize = page.getPositionCount();
                if (skipStripe && rowsProcessed < 10000) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                }
                else if (skipFirstBatch && isFirst) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                    isFirst = false;
                }
                else {
                    Block block = page.getBlock(0);

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
                assertEquals(recordReader.getReaderPosition(), rowsProcessed);
                assertEquals(recordReader.getFilePosition(), rowsProcessed);
                rowsProcessed += batchSize;
            }
            assertFalse(iterator.hasNext());
            assertNull(recordReader.nextPage());

            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
        }
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertNull(expected);
            return;
        }
        if (type instanceof ArrayType) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
            assertEquals(actualArray.size(), expectedArray.size());

            Type elementType = type.getTypeParameters().get(0);
            for (int i = 0; i < actualArray.size(); i++) {
                Object actualElement = actualArray.get(i);
                Object expectedElement = expectedArray.get(i);
                assertColumnValueEquals(elementType, actualElement, expectedElement);
            }
        }
        else if (type instanceof MapType) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            assertEquals(actualMap.size(), expectedMap.size());

            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            List<Entry<?, ?>> expectedEntries = new ArrayList<>(expectedMap.entrySet());
            for (Entry<?, ?> actualEntry : actualMap.entrySet()) {
                Iterator<Entry<?, ?>> iterator = expectedEntries.iterator();
                while (iterator.hasNext()) {
                    Entry<?, ?> expectedEntry = iterator.next();
                    try {
                        assertColumnValueEquals(keyType, actualEntry.getKey(), expectedEntry.getKey());
                        assertColumnValueEquals(valueType, actualEntry.getValue(), expectedEntry.getValue());
                        iterator.remove();
                    }
                    catch (AssertionError ignored) {
                    }
                }
            }
            assertTrue(expectedEntries.isEmpty(), "Unmatched entries " + expectedEntries);
        }
        else if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();

            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertEquals(actualRow.size(), fieldTypes.size());
            assertEquals(actualRow.size(), expectedRow.size());

            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (type.equals(DOUBLE)) {
            Double actualDouble = (Double) actual;
            Double expectedDouble = (Double) expected;
            if (actualDouble.isNaN()) {
                assertTrue(expectedDouble.isNaN(), "expected double to be NaN");
            }
            else {
                assertEquals(actualDouble, expectedDouble, 0.001);
            }
        }
        else if (!Objects.equals(actual, expected)) {
            assertEquals(actual, expected);
        }
    }

    static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcPredicate predicate, Type type, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
        OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                .orElseThrow(() -> new RuntimeException("File is empty"));

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup().orElse(0), 10_000);

        return orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(type),
                predicate,
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                initialBatchSize,
                RuntimeException::new);
    }

    public static void writeOrcPages(File outputFile, CompressionKind compression, List<Type> types, Iterator<Page> pages, OrcWriterStats stats)
            throws Exception
    {
        List<String> columnNames = IntStream.range(0, types.size())
                .mapToObj(i -> "test" + i)
                .collect(toImmutableList());

        OrcWriter writer = new OrcWriter(
                new OutputStreamOrcDataSink(new FileOutputStream(outputFile)),
                columnNames,
                types,
                OrcType.createRootOrcType(columnNames, types),
                compression,
                new OrcWriterOptions(),
                ImmutableMap.of(),
                true,
                BOTH,
                stats);

        while (pages.hasNext()) {
            writer.write(pages.next());
        }

        writer.close();
        writer.validate(new FileOrcDataSource(outputFile, READER_OPTIONS));
    }

    public static void writeOrcColumnTrino(File outputFile, CompressionKind compression, Type type, Iterator<?> values, OrcWriterStats stats)
            throws Exception
    {
        List<String> columnNames = ImmutableList.of("test");
        List<Type> types = ImmutableList.of(type);

        OrcWriter writer = new OrcWriter(
                new OutputStreamOrcDataSink(new FileOutputStream(outputFile)),
                ImmutableList.of("test"),
                types,
                OrcType.createRootOrcType(columnNames, types),
                compression,
                new OrcWriterOptions(),
                ImmutableMap.of(),
                true,
                BOTH,
                stats);

        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1024);
        while (values.hasNext()) {
            Object value = values.next();
            writeValue(type, blockBuilder, value);
        }

        writer.write(new Page(blockBuilder.build()));
        writer.close();
        writer.validate(new FileOrcDataSource(outputFile, READER_OPTIONS));
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
            else if (Decimals.isShortDecimal(type)) {
                type.writeLong(blockBuilder, ((SqlDecimal) value).toBigDecimal().unscaledValue().longValue());
            }
            else if (Decimals.isLongDecimal(type)) {
                type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).toBigDecimal().unscaledValue()));
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
            else if (TIMESTAMP_MILLIS.equals(type)) {
                type.writeLong(blockBuilder, ((SqlTimestamp) value).getEpochMicros());
            }
            else if (TIMESTAMP_MICROS.equals(type)) {
                long micros = ((SqlTimestamp) value).getEpochMicros();
                type.writeLong(blockBuilder, micros);
            }
            else if (TIMESTAMP_NANOS.equals(type)) {
                SqlTimestamp ts = (SqlTimestamp) value;
                type.writeObject(blockBuilder, new LongTimestamp(ts.getEpochMicros(), ts.getPicosOfMicros()));
            }
            else if (TIMESTAMP_TZ_MILLIS.equals(type)) {
                long millis = ((SqlTimestampWithTimeZone) value).getEpochMillis();
                type.writeLong(blockBuilder, packDateTimeWithZone(millis, UTC_KEY));
            }
            else if (TIMESTAMP_TZ_MICROS.equals(type) || TIMESTAMP_TZ_NANOS.equals(type)) {
                SqlTimestampWithTimeZone ts = (SqlTimestampWithTimeZone) value;
                type.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(ts.getEpochMillis(), ts.getPicosOfMilli(), UTC_KEY));
            }
            else {
                if (type instanceof ArrayType) {
                    List<?> array = (List<?>) value;
                    Type elementType = type.getTypeParameters().get(0);
                    BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Object elementValue : array) {
                        writeValue(elementType, arrayBlockBuilder, elementValue);
                    }
                    blockBuilder.closeEntry();
                }
                else if (type instanceof MapType) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Type keyType = type.getTypeParameters().get(0);
                    Type valueType = type.getTypeParameters().get(1);
                    BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Entry<?, ?> entry : map.entrySet()) {
                        writeValue(keyType, mapBlockBuilder, entry.getKey());
                        writeValue(valueType, mapBlockBuilder, entry.getValue());
                    }
                    blockBuilder.closeEntry();
                }
                else if (type instanceof RowType) {
                    List<?> array = (List<?>) value;
                    List<Type> fieldTypes = type.getTypeParameters();
                    BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
                    for (int fieldId = 0; fieldId < fieldTypes.size(); fieldId++) {
                        Type fieldType = fieldTypes.get(fieldId);
                        writeValue(fieldType, rowBlockBuilder, array.get(fieldId));
                    }
                    blockBuilder.closeEntry();
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        }
    }

    private static void assertFileContentsHive(
            Type type,
            TempFile tempFile,
            Iterable<?> expectedValues)
            throws Exception
    {
        assertFileContentsOrcHive(type, tempFile, expectedValues);
    }

    private static void assertFileContentsOrcHive(
            Type type,
            TempFile tempFile,
            Iterable<?> expectedValues)
            throws Exception
    {
        JobConf configuration = new JobConf(new Configuration(false));
        configuration.set(READ_COLUMN_IDS_CONF_STR, "0");
        configuration.setBoolean(READ_ALL_COLUMNS, false);

        Reader reader = OrcFile.createReader(
                new Path(tempFile.getFile().getAbsolutePath()),
                new ReaderOptions(configuration)
                        .useUTCTimestamp(true));
        RecordReader recordReader = reader.rows();

        StructObjectInspector rowInspector = (StructObjectInspector) reader.getObjectInspector();
        StructField field = rowInspector.getStructFieldRef("test");

        Iterator<?> iterator = expectedValues.iterator();
        Object rowData = null;
        while (recordReader.hasNext()) {
            rowData = recordReader.next(rowData);
            Object expectedValue = iterator.next();

            Object actualValue = rowInspector.getStructFieldData(rowData, field);
            actualValue = decodeRecordReaderValue(type, actualValue);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
        assertFalse(iterator.hasNext());
    }

    private static Object decodeRecordReaderValue(Type type, Object actualValue)
    {
        if (actualValue instanceof BooleanWritable) {
            actualValue = ((BooleanWritable) actualValue).get();
        }
        else if (actualValue instanceof ByteWritable) {
            actualValue = ((ByteWritable) actualValue).get();
        }
        else if (actualValue instanceof BytesWritable) {
            actualValue = new SqlVarbinary(((BytesWritable) actualValue).copyBytes());
        }
        else if (actualValue instanceof DateWritableV2) {
            actualValue = new SqlDate(((DateWritableV2) actualValue).getDays());
        }
        else if (actualValue instanceof DoubleWritable) {
            actualValue = ((DoubleWritable) actualValue).get();
        }
        else if (actualValue instanceof FloatWritable) {
            actualValue = ((FloatWritable) actualValue).get();
        }
        else if (actualValue instanceof IntWritable) {
            actualValue = ((IntWritable) actualValue).get();
        }
        else if (actualValue instanceof HiveCharWritable) {
            actualValue = ((HiveCharWritable) actualValue).getPaddedValue().toString();
        }
        else if (actualValue instanceof LongWritable) {
            actualValue = ((LongWritable) actualValue).get();
        }
        else if (actualValue instanceof ShortWritable) {
            actualValue = ((ShortWritable) actualValue).get();
        }
        else if (actualValue instanceof HiveDecimalWritable) {
            DecimalType decimalType = (DecimalType) type;
            HiveDecimalWritable writable = (HiveDecimalWritable) actualValue;
            // writable messes with the scale so rescale the values to the Trino type
            BigInteger rescaledValue = rescale(writable.getHiveDecimal().unscaledValue(), writable.getScale(), decimalType.getScale());
            actualValue = new SqlDecimal(rescaledValue, decimalType.getPrecision(), decimalType.getScale());
        }
        else if (actualValue instanceof Text) {
            actualValue = actualValue.toString();
        }
        else if (actualValue instanceof TimestampWritableV2) {
            Timestamp timestamp = ((TimestampWritableV2) actualValue).getTimestamp();
            if (type.equals(TIMESTAMP_MILLIS)) {
                actualValue = sqlTimestampOf(3, timestamp.toEpochMilli());
            }
            else if (type.equals(TIMESTAMP_MICROS)) {
                long micros = timestamp.toEpochSecond() * MICROSECONDS_PER_SECOND;
                micros += roundDiv(timestamp.getNanos(), NANOSECONDS_PER_MICROSECOND);
                actualValue = SqlTimestamp.newInstance(6, micros, 0);
            }
            else if (type.equals(TIMESTAMP_NANOS)) {
                long micros = timestamp.toEpochSecond() * MICROSECONDS_PER_SECOND;
                micros += timestamp.getNanos() / NANOSECONDS_PER_MICROSECOND;
                int picosOfMicro = (timestamp.getNanos() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                actualValue = SqlTimestamp.newInstance(9, micros, picosOfMicro);
            }
            else if (type.equals(TIMESTAMP_TZ_MILLIS)) {
                actualValue = SqlTimestampWithTimeZone.newInstance(3, timestamp.toEpochMilli(), 0, UTC_KEY);
            }
            else if (type.equals(TIMESTAMP_TZ_MICROS)) {
                int picosOfMilli = roundDiv(timestamp.getNanos(), NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_MICROSECOND;
                actualValue = SqlTimestampWithTimeZone.newInstance(3, timestamp.toEpochMilli(), picosOfMilli, UTC_KEY);
            }
            else if (type.equals(TIMESTAMP_TZ_NANOS)) {
                int picosOfMilli = (timestamp.getNanos() % NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND;
                actualValue = SqlTimestampWithTimeZone.newInstance(3, timestamp.toEpochMilli(), picosOfMilli, UTC_KEY);
            }
            else {
                throw new IllegalArgumentException("Unsupported timestamp type: " + type);
            }
        }
        else if (actualValue instanceof OrcStruct) {
            List<Object> fields = new ArrayList<>();
            OrcStruct structObject = (OrcStruct) actualValue;
            for (int fieldId = 0; fieldId < structObject.getNumFields(); fieldId++) {
                fields.add(OrcUtil.getFieldValue(structObject, fieldId));
            }
            actualValue = decodeRecordReaderStruct(type, fields);
        }
        else if (actualValue instanceof List) {
            actualValue = decodeRecordReaderList(type, ((List<?>) actualValue));
        }
        else if (actualValue instanceof Map) {
            actualValue = decodeRecordReaderMap(type, (Map<?, ?>) actualValue);
        }
        return actualValue;
    }

    private static List<Object> decodeRecordReaderList(Type type, List<?> list)
    {
        Type elementType = type.getTypeParameters().get(0);
        return list.stream()
                .map(element -> decodeRecordReaderValue(elementType, element))
                .collect(toList());
    }

    private static Object decodeRecordReaderMap(Type type, Map<?, ?> map)
    {
        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);
        Map<Object, Object> newMap = new HashMap<>();
        for (Entry<?, ?> entry : map.entrySet()) {
            newMap.put(decodeRecordReaderValue(keyType, entry.getKey()), decodeRecordReaderValue(valueType, entry.getValue()));
        }
        return newMap;
    }

    private static List<Object> decodeRecordReaderStruct(Type type, List<?> fields)
    {
        List<Type> fieldTypes = type.getTypeParameters();
        List<Object> newFields = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fieldTypes.get(i);
            Object field = fields.get(i);
            newFields.add(decodeRecordReaderValue(fieldType, field));
        }

        for (int j = fields.size(); j < fieldTypes.size(); j++) {
            newFields.add(null);
        }

        return newFields;
    }

    public static DataSize writeOrcColumnHive(File outputFile, Format format, CompressionKind compression, Type type, Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter = createOrcRecordWriter(outputFile, format, compression, type);
        return writeOrcFileColumnHive(outputFile, recordWriter, type, values);
    }

    public static DataSize writeOrcFileColumnHive(File outputFile, RecordWriter recordWriter, Type type, Iterator<?> values)
            throws Exception
    {
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", type);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        Serializer serializer = new OrcSerde();

        while (values.hasNext()) {
            Object value = values.next();
            value = preprocessWriteValueHive(type, value);
            objectInspector.setStructFieldData(row, fields.get(0), value);

            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
        return succinctBytes(outputFile.length());
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
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
            return javaStringObjectInspector;
        }
        if (type instanceof CharType) {
            int charLength = ((CharType) type).getLength();
            return new JavaHiveCharObjectInspector(getCharTypeInfo(charLength));
        }
        if (type instanceof VarbinaryType) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(DATE)) {
            return javaDateObjectInspector;
        }
        if (type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS) || type.equals(TIMESTAMP_NANOS)) {
            return javaTimestampObjectInspector;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS) || type.equals(TIMESTAMP_TZ_MICROS) || type.equals(TIMESTAMP_TZ_NANOS)) {
            return javaTimestampTZObjectInspector;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        if (type instanceof ArrayType) {
            return getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        if (type instanceof MapType) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        if (type instanceof RowType) {
            return getStandardStructObjectInspector(
                    type.getTypeSignature().getParameters().stream()
                            .map(parameter -> parameter.getNamedTypeSignature().getName().get())
                            .collect(toList()),
                    type.getTypeParameters().stream()
                            .map(OrcTester::getJavaObjectInspector)
                            .collect(toList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static Object preprocessWriteValueHive(Type type, Object value)
    {
        if (value == null) {
            return null;
        }

        if (type.equals(BOOLEAN)) {
            return value;
        }
        if (type.equals(TINYINT)) {
            return ((Number) value).byteValue();
        }
        if (type.equals(SMALLINT)) {
            return ((Number) value).shortValue();
        }
        if (type.equals(INTEGER)) {
            return ((Number) value).intValue();
        }
        if (type.equals(BIGINT)) {
            return ((Number) value).longValue();
        }
        if (type.equals(REAL)) {
            return ((Number) value).floatValue();
        }
        if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        if (type instanceof VarcharType) {
            return value;
        }
        if (type instanceof CharType) {
            return new HiveChar((String) value, ((CharType) type).getLength());
        }
        if (type.equals(VARBINARY)) {
            return ((SqlVarbinary) value).getBytes();
        }
        if (type.equals(DATE)) {
            return Date.ofEpochDay(((SqlDate) value).getDays());
        }
        if (type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS) || type.equals(TIMESTAMP_NANOS)) {
            LocalDateTime dateTime = ((SqlTimestamp) value).toLocalDateTime();
            return Timestamp.ofEpochSecond(dateTime.toEpochSecond(ZoneOffset.UTC), dateTime.getNano());
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS) || type.equals(TIMESTAMP_TZ_MICROS) || type.equals(TIMESTAMP_TZ_NANOS)) {
            SqlTimestampWithTimeZone timestamp = (SqlTimestampWithTimeZone) value;
            int nanosOfMilli = roundDiv(timestamp.getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND);
            return Timestamp.ofEpochMilli(timestamp.getEpochMillis(), nanosOfMilli);
        }
        if (type instanceof DecimalType) {
            return HiveDecimal.create(((SqlDecimal) value).toBigDecimal());
        }
        if (type instanceof ArrayType) {
            Type elementType = type.getTypeParameters().get(0);
            return ((List<?>) value).stream()
                    .map(element -> preprocessWriteValueHive(elementType, element))
                    .collect(toList());
        }
        if (type instanceof MapType) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            Map<Object, Object> newMap = new HashMap<>();
            for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                newMap.put(preprocessWriteValueHive(keyType, entry.getKey()), preprocessWriteValueHive(valueType, entry.getValue()));
            }
            return newMap;
        }
        if (type instanceof RowType) {
            List<?> fieldValues = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> newStruct = new ArrayList<>();
            for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                newStruct.add(preprocessWriteValueHive(fieldTypes.get(fieldId), fieldValues.get(fieldId)));
            }
            return newStruct;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, Type type)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        OrcConf.WRITE_FORMAT.setString(jobConf, format == ORC_12 ? "0.12" : "0.11");
        OrcConf.COMPRESS.setString(jobConf, compression.name());

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                createTableProperties("test", getJavaObjectInspector(type).getTypeName()),
                () -> {});
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, Type type)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(getJavaObjectInspector(type)));
    }

    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        orderTableProperties.setProperty("orc.bloom.filter.columns", name);
        orderTableProperties.setProperty("orc.bloom.filter.fpp", "0.50");
        orderTableProperties.setProperty("orc.bloom.filter.write.version", "original");
        return orderTableProperties;
    }

    private static <T> List<T> reverse(List<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
    }

    private static <T> List<T> insertNullEvery(int n, List<T> iterable)
    {
        return newArrayList(() -> new AbstractIterator<T>()
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
        });
    }

    private static List<Object> toHiveStruct(Object input)
    {
        return asList(input, input, input);
    }

    private static List<Object> toHiveStructWithNull(Object input)
    {
        return asList(input, input, input, null, null, null);
    }

    private static Map<Object, Object> toHiveMap(Object input, Object nullKeyValue)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(input != null ? input : nullKeyValue, input);
        return map;
    }

    private static List<Object> toHiveList(Object input)
    {
        return asList(input, input, input, input);
    }

    private static Type arrayType(Type elementType)
    {
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.typeParameter(elementType.getTypeSignature())));
    }

    private static Type mapType(Type keyType, Type valueType)
    {
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.typeParameter(keyType.getTypeSignature()), TypeSignatureParameter.typeParameter(valueType.getTypeSignature())));
    }

    private static Type rowType(Type... fieldTypes)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            String filedName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeSignatureParameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(filedName)), fieldType.getTypeSignature())));
        }
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }

    private static boolean isTimestampTz(Type type)
    {
        if (type instanceof TimestampWithTimeZoneType) {
            return true;
        }
        if (type instanceof ArrayType) {
            return isTimestampTz(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return isTimestampTz(((MapType) type).getKeyType()) || isTimestampTz(((MapType) type).getValueType());
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream()
                    .map(RowType.Field::getType)
                    .anyMatch(OrcTester::isTimestampTz);
        }
        return false;
    }
}
