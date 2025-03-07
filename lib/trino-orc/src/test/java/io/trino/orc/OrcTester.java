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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.hive.orc.OrcConf;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
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
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.IOConstants;
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
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
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
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
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
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BINARY;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.trino.orc.reader.ColumnReaders.ICEBERG_BINARY_TYPE;
import static io.trino.orc.reader.ColumnReaders.ICEBERG_LONG_TYPE;
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
import static io.trino.spi.type.TimeType.TIME_MICROS;
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
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

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
            boolean hiveSupported = (compression != LZ4) && (compression != ZSTD)
                    && !containsTimeMicros(writeType) && !containsTimeMicros(readType)
                    && !isTimestampTz(writeType) && !isTimestampTz(readType)
                    && !isUuid(writeType) && !isUuid(readType);

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

        assertThat(stats.getWriterSizeInBytes()).isEqualTo(0);
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
            assertThat(recordReader.getReaderPosition()).isEqualTo(0);
            assertThat(recordReader.getFilePosition()).isEqualTo(0);

            boolean isFirst = true;
            int rowsProcessed = 0;
            Iterator<?> iterator = expectedValues.iterator();
            for (SourcePage page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
                int batchSize = page.getPositionCount();
                if (skipStripe && rowsProcessed < 10000) {
                    assertThat(advance(iterator, batchSize)).isEqualTo(batchSize);
                }
                else if (skipFirstBatch && isFirst) {
                    assertThat(advance(iterator, batchSize)).isEqualTo(batchSize);
                    isFirst = false;
                }
                else {
                    Block block = page.getBlock(0);

                    List<Object> data = new ArrayList<>(block.getPositionCount());
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                    }

                    for (int i = 0; i < batchSize; i++) {
                        assertThat(iterator.hasNext()).isTrue();
                        Object expected = iterator.next();
                        Object actual = data.get(i);
                        assertColumnValueEquals(type, actual, expected);
                    }
                }
                assertThat(recordReader.getReaderPosition()).isEqualTo(rowsProcessed);
                assertThat(recordReader.getFilePosition()).isEqualTo(rowsProcessed);
                rowsProcessed += batchSize;
            }
            assertThat(iterator.hasNext()).isFalse();
            assertThat(recordReader.nextPage()).isNull();

            assertThat(recordReader.getReaderPosition()).isEqualTo(rowsProcessed);
            assertThat(recordReader.getFilePosition()).isEqualTo(rowsProcessed);
        }
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertThat(expected).isNull();
            return;
        }
        if (type instanceof ArrayType) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
            assertThat(actualArray).hasSize(expectedArray.size());

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
            assertThat(actualMap).hasSize(expectedMap.size());

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
                    catch (AssertionError _) {
                    }
                }
            }
            assertThat(expectedEntries.isEmpty())
                    .describedAs("Unmatched entries " + expectedEntries)
                    .isTrue();
        }
        else if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();

            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertThat(actualRow).hasSize(fieldTypes.size());
            assertThat(actualRow).hasSize(expectedRow.size());

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
                assertThat(expectedDouble.isNaN())
                        .describedAs("expected double to be NaN")
                        .isTrue();
            }
            else {
                assertThat(actualDouble).isCloseTo(expectedDouble, offset(0.001));
            }
        }
        else if (type.equals(UUID)) {
            UUID actualUUID = java.util.UUID.fromString((String) actual);
            assertThat(actualUUID).isEqualTo(expected);
        }
        else if (!Objects.equals(actual, expected)) {
            assertThat(actual).isEqualTo(expected);
        }
    }

    static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcPredicate predicate, Type type, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
        OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                .orElseThrow(() -> new RuntimeException("File is empty"));

        assertThat(orcReader.getColumnNames()).isEqualTo(ImmutableList.of("test"));
        assertThat(orcReader.getFooter().getRowsInRowGroup().orElse(0)).isEqualTo(10_000);

        return orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(type),
                false,
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
                OutputStreamOrcDataSink.create(new LocalOutputFile(outputFile)),
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

        ColumnMetadata<OrcType> orcType = OrcType.createRootOrcType(columnNames, types, Optional.of(mappedType -> {
            if (UUID.equals(mappedType)) {
                return Optional.of(new OrcType(
                        BINARY,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(ICEBERG_BINARY_TYPE, "UUID")));
            }
            if (TIME_MICROS.equals(mappedType)) {
                return Optional.of(new OrcType(
                    LONG,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(ICEBERG_LONG_TYPE, "TIME")));
            }
            return Optional.empty();
        }));

        OrcWriter writer = new OrcWriter(
                OutputStreamOrcDataSink.create(new LocalOutputFile(outputFile)),
                ImmutableList.of("test"),
                types,
                orcType,
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
            else if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    type.writeLong(blockBuilder, ((SqlDecimal) value).toBigDecimal().unscaledValue().longValue());
                }
                else {
                    type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).toBigDecimal().unscaledValue()));
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
            else if (UUID.equals(type)) {
                type.writeSlice(blockBuilder, javaUuidToTrinoUuid((java.util.UUID) value));
            }
            else if (DATE.equals(type)) {
                long days = ((SqlDate) value).getDays();
                type.writeLong(blockBuilder, days);
            }
            else if (TIME_MICROS.equals(type)) {
                type.writeLong(blockBuilder, ((SqlTime) value).getPicos());
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
                    ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                        for (Object elementValue : array) {
                            writeValue(elementType, elementBuilder, elementValue);
                        }
                    });
                }
                else if (type instanceof MapType mapType) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Type keyType = mapType.getKeyType();
                    Type valueType = mapType.getValueType();
                    ((MapBlockBuilder) blockBuilder).buildEntry((keyBuilder, valueBuilder) -> {
                        map.forEach((key, value1) -> {
                            writeValue(keyType, keyBuilder, key);
                            writeValue(valueType, valueBuilder, value1);
                        });
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
        assertThat(iterator.hasNext()).isFalse();
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
            if (UUID.equals(type)) {
                ByteBuffer bytes = ByteBuffer.wrap(((BytesWritable) actualValue).copyBytes());
                actualValue = new UUID(bytes.getLong(), bytes.getLong()).toString();
            }
            else {
                actualValue = new SqlVarbinary(((BytesWritable) actualValue).copyBytes());
            }
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
        else if (actualValue instanceof HiveDecimalWritable writable) {
            DecimalType decimalType = (DecimalType) type;
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
        else if (actualValue instanceof OrcStruct structObject) {
            List<Object> fields = new ArrayList<>();
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

    public static void writeOrcColumnHive(File outputFile, Format format, CompressionKind compression, Type type, Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter = createOrcRecordWriter(outputFile, format, compression, type);
        writeOrcColumnHive(recordWriter, type, values);
    }

    public static void writeOrcColumnHive(RecordWriter recordWriter, Type type, Iterator<?> values)
            throws Exception
    {
        StandardStructObjectInspector objectInspector = getStandardStructObjectInspector(ImmutableList.of("test"), ImmutableList.of(getJavaObjectInspector(type)));
        writeOrcColumnsHive(
                recordWriter,
                objectInspector,
                ImmutableList.of(type),
                StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(values, Spliterator.ORDERED), false)
                        .map(value -> (Function<Integer, Object>) (fieldIndex) -> value)
                        .iterator());
    }

    public static void writeOrcColumnsHiveFile(
            File outputFile,
            OrcTester.Format format,
            CompressionKind compression,
            List<String> names,
            List<Type> types,
            Iterator<Function<Integer, Object>> values)
            throws Exception
    {
        StandardStructObjectInspector objectInspector = getStandardStructObjectInspector(names,
                types.stream().map(OrcTester::getJavaObjectInspector).collect(toImmutableList()));

        writeOrcColumnsHive(
                createOrcRecordWriter(outputFile, format, compression, objectInspector),
                objectInspector,
                types,
                values);
    }

    public static void writeOrcColumnsHive(
            RecordWriter recordWriter,
            StandardStructObjectInspector objectInspector,
            List<Type> types,
            Iterator<Function<Integer, Object>> values)
            throws Exception
    {
        requireNonNull(types, "types is null");
        requireNonNull(objectInspector, "objectInspector is null");

        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        int fieldCount = fields.size();
        checkArgument(fieldCount == types.size(), "Field and type count must be the same");

        Serializer serializer = new OrcSerde();

        while (values.hasNext()) {
            Function<Integer, Object> valueGetter = values.next();
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                Object value = preprocessWriteValueHive(types.get(fieldIndex), valueGetter.apply(fieldIndex));
                objectInspector.setStructFieldData(row, fields.get(fieldIndex), value);
            }
            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
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
        if (type instanceof CharType charType) {
            return new JavaHiveCharObjectInspector(getCharTypeInfo(charType.getLength()));
        }
        if (type instanceof VarbinaryType) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(UUID)) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(DATE)) {
            return javaDateObjectInspector;
        }
        if (type.equals(TIME_MICROS)) {
            return javaLongObjectInspector;
        }
        if (type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS) || type.equals(TIMESTAMP_NANOS)) {
            return javaTimestampObjectInspector;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS) || type.equals(TIMESTAMP_TZ_MICROS) || type.equals(TIMESTAMP_TZ_NANOS)) {
            return javaTimestampTZObjectInspector;
        }
        if (type instanceof DecimalType decimalType) {
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
        if (type.equals(UUID)) {
            return javaUuidToTrinoUuid((java.util.UUID) value).getBytes();
        }
        if (type.equals(DATE)) {
            return Date.ofEpochDay(((SqlDate) value).getDays());
        }
        if (type.equals(TIME_MICROS)) {
            return ((SqlTime) value).getPicos() / PICOSECONDS_PER_MICROSECOND;
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
        StandardStructObjectInspector objectInspector = getStandardStructObjectInspector(ImmutableList.of("test"), ImmutableList.of(getJavaObjectInspector(type)));

        return RecordWriterBuilder.builder(outputFile, format, compression)
                .withColumns(objectInspector)
                .withBloomFilter(objectInspector, 0.50)
                .build();
    }

    private static RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, StandardStructObjectInspector objectInspector)
            throws IOException
    {
        return RecordWriterBuilder.builder(outputFile, format, compression)
                .withColumns(objectInspector)
                .withStripeSize(120000L)
                .build();
    }

    private static class RecordWriterBuilder
    {
        private final JobConf jobConf;
        private final File file;

        private final Class<? extends Writable> valueClass = Text.class;
        private final CompressionKind compression;
        private final Progressable reporter = () -> {};
        private final Properties tableProperties = new Properties();

        private RecordWriterBuilder(File file, Format format, CompressionKind compression)
        {
            this.jobConf = new JobConf(new Configuration(false));
            this.file = file;
            this.compression = compression;
            OrcConf.WRITE_FORMAT.setString(jobConf, format == ORC_12 ? "0.12" : "0.11");
            OrcConf.COMPRESS.setString(jobConf, compression.name());
        }

        public static RecordWriterBuilder builder(File file, Format format, CompressionKind compression)
        {
            return new RecordWriterBuilder(file, format, compression);
        }

        public RecordWriterBuilder withColumns(StandardStructObjectInspector objectInspector)
        {
            List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
            tableProperties.setProperty(IOConstants.COLUMNS, fields.stream()
                    .map(StructField::getFieldName)
                    .collect(Collectors.joining(",")));
            tableProperties.setProperty(IOConstants.COLUMNS_TYPES, fields.stream()
                    .map(field -> field.getFieldObjectInspector().getTypeName())
                    .collect(Collectors.joining(":")));
            return this;
        }

        public RecordWriterBuilder withStripeSize(long stripeSizeBytes)
        {
            tableProperties.setProperty(OrcConf.STRIPE_SIZE.getAttribute(), Long.toString(stripeSizeBytes));
            return this;
        }

        public RecordWriterBuilder withBloomFilter(StandardStructObjectInspector objectInspector, Double falsePositiveProbability)
        {
            List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
            tableProperties.setProperty(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), fields.stream()
                    .map(StructField::getFieldName)
                    .collect(Collectors.joining(",")));
            tableProperties.setProperty(OrcConf.BLOOM_FILTER_FPP.getAttribute(), Double.toString(falsePositiveProbability));
            tableProperties.setProperty(OrcConf.BLOOM_FILTER_WRITE_VERSION.getAttribute(), "original");
            return this;
        }

        FileSinkOperator.RecordWriter build()
                throws IOException
        {
            return new OrcOutputFormat().getHiveRecordWriter(
                    jobConf,
                    new Path(file.toURI()),
                    valueClass,
                    compression != NONE,
                    tableProperties,
                    reporter);
        }
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, Type type)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(getJavaObjectInspector(type)));
    }

    private static <T> List<T> reverse(List<T> iterable)
    {
        return ImmutableList.copyOf(iterable).reverse();
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
            String fieldName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeSignatureParameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(fieldName)), fieldType.getTypeSignature())));
        }
        return TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }

    private static boolean containsTimeMicros(Type type)
    {
        if (type.equals(TIME_MICROS)) {
            return true;
        }
        if (type instanceof ArrayType arrayType) {
            return containsTimeMicros(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return containsTimeMicros(mapType.getKeyType()) || containsTimeMicros(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .anyMatch(OrcTester::containsTimeMicros);
        }
        return false;
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

    private static boolean isUuid(Type type)
    {
        if (type.equals(UUID)) {
            return true;
        }
        if (type instanceof ArrayType) {
            return isUuid(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return isUuid(((MapType) type).getKeyType()) || isUuid(((MapType) type).getValueType());
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream()
                    .map(RowType.Field::getType)
                    .anyMatch(OrcTester::isUuid);
        }
        return false;
    }
}
