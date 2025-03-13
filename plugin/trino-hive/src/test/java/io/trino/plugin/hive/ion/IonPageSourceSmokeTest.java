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
package io.trino.plugin.hive.ion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.WriterKind;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.ION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.projectedColumn;
import static io.trino.plugin.hive.HiveTestUtils.toHiveBaseColumnHandle;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.ion.IonSerDeProperties.BINARY_ENCODING;
import static io.trino.plugin.hive.ion.IonSerDeProperties.FAIL_ON_OVERFLOW_PROPERTY;
import static io.trino.plugin.hive.ion.IonSerDeProperties.FAIL_ON_OVERFLOW_PROPERTY_DEFAULT;
import static io.trino.plugin.hive.ion.IonSerDeProperties.IGNORE_MALFORMED;
import static io.trino.plugin.hive.ion.IonSerDeProperties.IGNORE_MALFORMED_DEFAULT;
import static io.trino.plugin.hive.ion.IonSerDeProperties.ION_ENCODING_PROPERTY;
import static io.trino.plugin.hive.ion.IonSerDeProperties.ION_SERIALIZE_NULL_AS_DEFAULT;
import static io.trino.plugin.hive.ion.IonSerDeProperties.ION_SERIALIZE_NULL_AS_PROPERTY;
import static io.trino.plugin.hive.ion.IonSerDeProperties.ION_TIMESTAMP_OFFSET_DEFAULT;
import static io.trino.plugin.hive.ion.IonSerDeProperties.ION_TIMESTAMP_OFFSET_PROPERTY;
import static io.trino.plugin.hive.ion.IonSerDeProperties.STRICT_PATH_TYPING_PROPERTY;
import static io.trino.plugin.hive.ion.IonSerDeProperties.TEXT_ENCODING;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.stream.Collectors.toList;

/**
 * Most basic test to reflect PageSource-fu is wired up correctly.
 */
public class IonPageSourceSmokeTest
{
    // In the Ion binary format, a value stream is always start with binary version marker. This help distinguish Ion binary
    // data from other formats, including Ion text format.
    private static final byte[] BINARY_VERSION_MARKER = {(byte) 0xE0, (byte) 0x01, (byte) 0x00, (byte) 0XEA};
    private static final String EXPECTED_TEXT = "{foo:3,bar:6}";

    public static final String TEST_ION_LOCATION = "memory:///test.ion";
    public static final List<HiveColumnHandle> FOO_BAR_COLUMNS = List.of(
            toHiveBaseColumnHandle("foo", INTEGER, 0),
            toHiveBaseColumnHandle("bar", VARCHAR, 1));

    @Test
    public void testReadTwoValues()
            throws IOException
    {
        assertRowCount(
                FOO_BAR_COLUMNS,
                FOO_BAR_COLUMNS,
                "{ foo: 31, bar: baz } { foo: 31, bar: \"baz\" }",
                2);
    }

    @Test
    public void testReadArray()
            throws IOException
    {
        List<HiveColumnHandle> tablesColumns = List.of(
                toHiveBaseColumnHandle("my_seq", new ArrayType(BOOLEAN), 0));

        assertRowCount(
                tablesColumns,
                tablesColumns,
                "{ my_seq: ( true false ) } { my_seq: [false, false, true] }",
                2);
    }

    @Test
    public void testStrictAndLaxPathTyping()
            throws IOException
    {
        TestFixture defaultFixture = new TestFixture(FOO_BAR_COLUMNS);
        defaultFixture.assertRowCount("37 null.timestamp []", 3);

        TestFixture laxFixture = new TestFixture(FOO_BAR_COLUMNS);
        laxFixture.withSerdeProperty(STRICT_PATH_TYPING_PROPERTY, "false");
        laxFixture.assertRowCount("37 null.timestamp []", 3);

        TestFixture strictFixture = new TestFixture(FOO_BAR_COLUMNS);
        strictFixture.withSerdeProperty(STRICT_PATH_TYPING_PROPERTY, "true");

        Assertions.assertThrows(RuntimeException.class, () ->
                strictFixture.assertRowCount("37 null.timestamp []", 3));
    }

    @Test
    public void testPathExtraction()
            throws IOException
    {
        TestFixture fixture = new TestFixture(List.of(toHiveBaseColumnHandle("bar", INTEGER, 0)))
                .withSerdeProperty("ion.bar.path_extractor", "(foo bar)");

        // these would result in errors if we tried to extract the bar field from the root instead of the nested bar
        fixture.assertRowCount("{ foo: { bar: 17 }, bar: not_this_bar } { foo: { bar: 31 }, bar: not_this_bar }", 2);
    }

    @Test
    public void testCaseSensitive()
            throws IOException
    {
        TestFixture fixture = new TestFixture(List.of(toHiveBaseColumnHandle("bar", INTEGER, 0)))
                .withSerdeProperty("ion.path_extractor.case_sensitive", "true");

        // this would result in errors if we tried to extract the BAR field
        fixture.assertRowCount("{ BAR: should_be_skipped } { bar: 17 }", 2);
    }

    @Test
    public void testProjectedColumn()
            throws IOException
    {
        final RowType spamType = RowType.rowType(field("nested_to_prune", INTEGER), field("eggs", INTEGER));
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("spam", spamType, 0),
                toHiveBaseColumnHandle("ham", BOOLEAN, 1));
        List<HiveColumnHandle> projectedColumns = List.of(
                projectedColumn(tableColumns.get(0), "eggs"));

        assertRowCount(
                tableColumns,
                projectedColumns,
                // the data below reflects that "ham" is not decoded, that column is pruned
                // "nested_to_prune" is decoded, however, because nested fields are not pruned, yet.
                // so this test will fail if you change that to something other than an int
                "{ spam: { nested_to_prune: 31, eggs: 12 }, ham: exploding }",
                1);
    }

    @Test
    public void testPageSourceTelemetryUncompressed()
            throws IOException
    {
        TestFixture fixture = new TestFixture(FOO_BAR_COLUMNS);
        int bytes = fixture.writeIonTextFile("{ foo: 17, bar: baz } { foo: 31, bar: qux }");

        ConnectorPageSource pageSource = fixture.getPageSource();

        Assertions.assertNotNull(pageSource.getNextSourcePage());
        Assertions.assertEquals(2, pageSource.getCompletedPositions().getAsLong());
        Assertions.assertEquals(bytes, pageSource.getCompletedBytes());
        Assertions.assertTrue(pageSource.getReadTimeNanos() > 0);
    }

    @Test
    public void testPageSourceTelemetryCompressed()
            throws IOException
    {
        TestFixture fixture = new TestFixture(FOO_BAR_COLUMNS);
        int bytes = fixture.writeZstdCompressedIonText("{ foo: 17, bar: baz } { foo: 31, bar: qux }");

        ConnectorPageSource pageSource = fixture.getPageSource();

        Assertions.assertNotNull(pageSource.getNextSourcePage());
        Assertions.assertEquals(2, pageSource.getCompletedPositions().getAsLong());
        Assertions.assertEquals(bytes, pageSource.getCompletedBytes());
        Assertions.assertTrue(pageSource.getReadTimeNanos() > 0);
    }

    private static Stream<Map.Entry<String, String>> propertiesWithDefaults()
    {
        return Stream.of(
                entry(FAIL_ON_OVERFLOW_PROPERTY, FAIL_ON_OVERFLOW_PROPERTY_DEFAULT),
                entry(IGNORE_MALFORMED, IGNORE_MALFORMED_DEFAULT),
                entry(ION_TIMESTAMP_OFFSET_PROPERTY, ION_TIMESTAMP_OFFSET_DEFAULT),
                entry(ION_SERIALIZE_NULL_AS_PROPERTY, ION_SERIALIZE_NULL_AS_DEFAULT));
    }

    private static Stream<Map.Entry<String, String>> propertiesWithValues()
    {
        return Stream.of(
                entry(FAIL_ON_OVERFLOW_PROPERTY, "false"),
                entry(IGNORE_MALFORMED, "true"),
                entry(ION_TIMESTAMP_OFFSET_PROPERTY, "01:00"),
                entry(ION_SERIALIZE_NULL_AS_PROPERTY, "TYPED"),
                // These entries represent column-specific properties that are not supported.
                // Any presence of these properties in the schema will result in an empty PageSource,
                // regardless of their assigned values.
                entry("ion.foo.fail_on_overflow", "property_value"),
                entry("ion.foo.serialize_as", "property_value"));
    }

    private static Map.Entry<String, String> entry(String key, String value)
    {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    @ParameterizedTest
    @MethodSource("propertiesWithValues")
    void testPropertiesWithValues(Map.Entry<String, String> property)
            throws IOException
    {
        TestFixture fixture = new TestFixture(FOO_BAR_COLUMNS)
                .withSerdeProperty(property.getKey(), property.getValue());

        Assertions.assertThrows(TrinoException.class, fixture::getOptionalFileWriter);

        fixture.writeIonTextFile("{ }");
        Assertions.assertThrows(TrinoException.class, fixture::getOptionalPageSource);
    }

    @ParameterizedTest
    @MethodSource("propertiesWithDefaults")
    void testPropertiesWithDefaults(Map.Entry<String, String> property)
            throws IOException
    {
        TestFixture fixture = new TestFixture(FOO_BAR_COLUMNS)
                .withSerdeProperty(property.getKey(), property.getValue());

        Assertions.assertTrue(
                fixture.getOptionalFileWriter().isPresent(),
                "Expected present file writer when there are unsupported Serde properties");

        fixture.writeIonTextFile("");
        Assertions.assertTrue(
                fixture.getOptionalPageSource().isPresent(),
                "Expected present page source when there are unsupported Serde properties");
    }

    @Test
    public void testTextEncoding()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", INTEGER, 0));

        assertEncoding(tableColumns, TEXT_ENCODING);
    }

    @Test
    public void testBinaryEncoding()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", INTEGER, 0));

        assertEncoding(tableColumns, BINARY_ENCODING);
    }

    @Test
    public void testBadEncodingName()
            throws IOException
    {
        TestFixture fixture = new TestFixture(FOO_BAR_COLUMNS)
                .withSerdeProperty(ION_ENCODING_PROPERTY, "unknown_encoding_name");

        Assertions.assertThrows(TrinoException.class, fixture::getFileWriter);
    }

    private void assertEncoding(List<HiveColumnHandle> tableColumns,
            String encoding)
            throws IOException
    {
        TestFixture fixture = new TestFixture(tableColumns)
                .withSerdeProperty(ION_ENCODING_PROPERTY, encoding);

        writeTestData(fixture.getFileWriter());
        byte[] inputStreamBytes = fixture.getTrinoInputFile()
                .newStream()
                .readAllBytes();

        if (encoding.equals(BINARY_ENCODING)) {
            // Check if the first 4 bytes is binary version marker
            Assertions.assertArrayEquals(Arrays.copyOfRange(inputStreamBytes, 0, 4), BINARY_VERSION_MARKER);
        }
        else {
            Assertions.assertEquals(new String(inputStreamBytes, StandardCharsets.UTF_8), EXPECTED_TEXT);
        }
    }

    private void assertRowCount(List<HiveColumnHandle> tableColumns, List<HiveColumnHandle> projectedColumns, String ionText, int rowCount)
            throws IOException
    {
        TestFixture fixture = new TestFixture(tableColumns, projectedColumns);
        fixture.assertRowCount(ionText, rowCount);
    }

    private static void writeTestData(FileWriter ionFileWriter)
    {
        ionFileWriter.appendRows(new Page(
                RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {3}), 1),
                RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {6}), 1)));
        ionFileWriter.commit();
    }

    private static class TestFixture
    {
        private TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        private Location fileLocation = Location.of(TEST_ION_LOCATION);
        private Map<String, String> tableProperties = new HashMap<>();
        private List<HiveColumnHandle> columns;
        private List<HiveColumnHandle> projections;

        private ConnectorSession session;

        TestFixture(List<HiveColumnHandle> columns)
        {
            this(columns, columns);
        }

        TestFixture(List<HiveColumnHandle> columns, List<HiveColumnHandle> projections)
        {
            this.columns = columns;
            this.projections = projections;
            tableProperties.put(LIST_COLUMNS, columns.stream()
                    .map(HiveColumnHandle::getName)
                    .collect(Collectors.joining(",")));
            tableProperties.put(LIST_COLUMN_TYPES, columns.stream().map(HiveColumnHandle::getHiveType)
                    .map(HiveType::toString)
                    .collect(Collectors.joining(",")));
        }

        TestFixture withSerdeProperty(String key, String value)
        {
            tableProperties.put(key, value);
            return this;
        }

        Optional<ConnectorPageSource> getOptionalPageSource()
                throws IOException
        {
            IonPageSourceFactory pageSourceFactory = new IonPageSourceFactory(fileSystemFactory);

            long length = fileSystemFactory.create(getSession()).newInputFile(fileLocation).length();
            long nowMillis = Instant.now().toEpochMilli();

            List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                    "",
                    ImmutableList.of(),
                    projections,
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    fileLocation.toString(),
                    OptionalInt.empty(),
                    length,
                    nowMillis);

            return HivePageSourceProvider.createHivePageSource(
                    ImmutableSet.of(pageSourceFactory),
                    getSession(),
                    fileLocation,
                    OptionalInt.empty(),
                    0,
                    length,
                    length,
                    nowMillis,
                    new Schema(ION.getSerde(), false, tableProperties),
                    TupleDomain.all(),
                    TESTING_TYPE_MANAGER,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    NO_ACID_TRANSACTION,
                    columnMappings);
        }

        ConnectorPageSource getPageSource()
                throws IOException
        {
            return getOptionalPageSource().orElseThrow();
        }

        ConnectorSession getSession()
        {
            if (session == null) {
                session = getHiveSession(new HiveConfig());
            }
            return session;
        }

        int writeIonTextFile(String ionText)
                throws IOException
        {
            TrinoOutputFile outputFile = fileSystemFactory.create(getSession()).newOutputFile(fileLocation);
            byte[] bytes = ionText.getBytes(StandardCharsets.UTF_8);
            outputFile.createOrOverwrite(bytes);

            return bytes.length;
        }

        int writeZstdCompressedIonText(String ionText)
                throws IOException
        {
            fileLocation = Location.of(TEST_ION_LOCATION + ".zst");
            TrinoOutputFile outputFile = fileSystemFactory.create(getSession()).newOutputFile(fileLocation);

            Slice textSlice = Slices.wrappedBuffer(ionText.getBytes(StandardCharsets.UTF_8));
            Slice compressed = CompressionKind.ZSTD.createCodec().createValueCompressor().compress(textSlice);
            outputFile.createOrOverwrite(compressed.getBytes());

            return compressed.length();
        }

        Optional<FileWriter> getOptionalFileWriter()
        {
            return new IonFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER)
                    .createFileWriter(
                            fileLocation,
                            columns.stream().map(HiveColumnHandle::getName).collect(toList()),
                            ION.toStorageFormat(),
                            HiveCompressionCodec.NONE,
                            tableProperties,
                            getSession(),
                            OptionalInt.empty(),
                            NO_ACID_TRANSACTION,
                            false,
                            WriterKind.INSERT);
        }

        FileWriter getFileWriter()
        {
            return getOptionalFileWriter().orElseThrow();
        }

        TrinoInputFile getTrinoInputFile()
        {
            return fileSystemFactory.create(getSession())
                    .newInputFile(fileLocation);
        }

        void assertRowCount(String ionText, int rowCount)
                throws IOException
        {
            writeIonTextFile(ionText);

            try (ConnectorPageSource pageSource = getPageSource()) {
                final MaterializedResult result = MaterializedResult.materializeSourceDataStream(
                        getSession(),
                        pageSource,
                        projections.stream().map(HiveColumnHandle::getType).toList());
                Assertions.assertEquals(rowCount, result.getRowCount());
            }
        }
    }
}
