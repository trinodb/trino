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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.WriterKind;
import io.trino.spi.Page;
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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.ION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.projectedColumn;
import static io.trino.plugin.hive.HiveTestUtils.toHiveBaseColumnHandle;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.ion.IonWriterOptions.BINARY_ENCODING;
import static io.trino.plugin.hive.ion.IonWriterOptions.ION_ENCODING_PROPERTY;
import static io.trino.plugin.hive.ion.IonWriterOptions.TEXT_ENCODING;
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

    @Test
    public void testReadTwoValues()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", VARCHAR, 1));

        assertRowCount(
                tableColumns,
                tableColumns,
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

    private void assertEncoding(List<HiveColumnHandle> tableColumns,
                                String encoding)
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of(TEST_ION_LOCATION);
        ConnectorSession session = getHiveSession(new HiveConfig());
        writeTestData(session, fileSystemFactory, location, encoding, tableColumns);
        byte[] inputStreamBytes = fileSystemFactory.create(session)
                .newInputFile(location)
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

    @Test
    public void testPageSourceWithNativeTrinoDisabled()
            throws IOException
    {
        List<HiveColumnHandle> tableColumns = List.of(
                toHiveBaseColumnHandle("foo", INTEGER, 0),
                toHiveBaseColumnHandle("bar", VARCHAR, 1));
        String ionText = "{ foo: 31, bar: baz } { foo: 31, bar: \"baz\" }";

        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of(TEST_ION_LOCATION);

        // by default Ion native Trino integration is disabled
        HiveConfig hiveConfig = new HiveConfig();

        final ConnectorSession session = getHiveSession(hiveConfig);

        writeIonTextFile(ionText, location, fileSystemFactory.create(session));

        final Optional<ReaderPageSource> pageSource = createReaderPageSource(fileSystemFactory,
                hiveConfig, location, tableColumns, session);

        Assertions.assertTrue(pageSource.isEmpty(), "Expected empty page source when native Trino is disabled");
    }

    private void assertRowCount(List<HiveColumnHandle> tableColumns, List<HiveColumnHandle> projectedColumns, String ionText, int rowCount)
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of(TEST_ION_LOCATION);

        HiveConfig hiveConfig = new HiveConfig();
        // enable Ion native trino integration for testing while the implementation is in progress
        hiveConfig.setIonNativeTrinoEnabled(true);

        final ConnectorSession session = getHiveSession(hiveConfig);

        writeIonTextFile(ionText, location, fileSystemFactory.create(session));

        try (ConnectorPageSource pageSource = createConnectorPageSource(fileSystemFactory, hiveConfig, location, tableColumns,
                projectedColumns, session)) {
            final MaterializedResult result = MaterializedResult.materializeSourceDataStream(session, pageSource, projectedColumns.stream().map(HiveColumnHandle::getType).toList());
            Assertions.assertEquals(rowCount, result.getRowCount());
        }
    }

    /**
     * todo: At some point, we might need to combine writeIonTextFile with this method and add logic to write iontext to Page.
     */
    private static void writeTestData(ConnectorSession session, TrinoFileSystemFactory fileSystemFactory, Location location, String encoding, List<HiveColumnHandle> tableColumns)
            throws IOException
    {
        FileWriter ionFileWriter = new IonFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER)
                .createFileWriter(
                        location,
                        tableColumns.stream().map(HiveColumnHandle::getName).collect(toList()),
                        ION.toStorageFormat(),
                        HiveCompressionCodec.NONE,
                        getTablePropertiesWithEncoding(tableColumns, encoding),
                        session,
                        OptionalInt.empty(),
                        NO_ACID_TRANSACTION,
                        false,
                        WriterKind.INSERT)
                .orElseThrow();
        ionFileWriter.appendRows(new Page(
                RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {3}), 1),
                RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {6}), 1)));
        ionFileWriter.commit();
    }

    private int writeIonTextFile(String ionText, Location location, TrinoFileSystem fileSystem)
            throws IOException
    {
        final TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        int written = 0;
        try (OutputStream outputStream = outputFile.create()) {
            byte[] bytes = ionText.getBytes(StandardCharsets.UTF_8);
            outputStream.write(bytes);
            outputStream.flush();
            written = bytes.length;
        }
        return written;
    }

    /**
     * todo: this is very similar to what's in TestOrcPredicates, factor out.
     */
    private static ConnectorPageSource createConnectorPageSource(
            TrinoFileSystemFactory fileSystemFactory,
            HiveConfig hiveConfig,
            Location location,
            List<HiveColumnHandle> tableColumns,
            List<HiveColumnHandle> projectedColumns,
            ConnectorSession session)
            throws IOException
    {
        final PageSourceParameters pageSourceParameters = preparePageSourceParameters(
                fileSystemFactory, hiveConfig, location, tableColumns, projectedColumns, session);

        return HivePageSourceProvider.createHivePageSource(
                        ImmutableSet.of(pageSourceParameters.factory()),
                        session,
                        location,
                        OptionalInt.empty(),
                        0,
                        pageSourceParameters.length(),
                        pageSourceParameters.length(),
                        pageSourceParameters.nowMillis(),
                        new Schema(ION.getSerde(), false, pageSourceParameters.tableProperties()),
                        TupleDomain.all(),
                        TESTING_TYPE_MANAGER,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        NO_ACID_TRANSACTION,
                        pageSourceParameters.columnMappings())
                .orElseThrow();
    }

    private static Optional<ReaderPageSource> createReaderPageSource(TrinoFileSystemFactory fileSystemFactory,
                                                                     HiveConfig hiveConfig, Location location,
                                                                     List<HiveColumnHandle> tableColumns,
                                                                     ConnectorSession session)
            throws IOException
    {
        final PageSourceParameters pageSourceParameters = preparePageSourceParameters(
                fileSystemFactory, hiveConfig, location, tableColumns, ImmutableList.of(), session);

        return pageSourceParameters.factory().createPageSource(
                session,
                location,
                0,
                pageSourceParameters.length(),
                pageSourceParameters.length(),
                pageSourceParameters.nowMillis(),
                new Schema(ION.getSerde(), false, pageSourceParameters.tableProperties()),
                tableColumns,
                TupleDomain.all(),
                Optional.empty(),
                OptionalInt.empty(),
                false,
                NO_ACID_TRANSACTION);
    }

    private static PageSourceParameters preparePageSourceParameters(TrinoFileSystemFactory fileSystemFactory,
                                                                         HiveConfig hiveConfig, Location location,
                                                                         List<HiveColumnHandle> tableColumns,
                                                                         List<HiveColumnHandle> projectedColumns,
                                                                         ConnectorSession session)
            throws IOException
    {
        IonPageSourceFactory factory = new IonPageSourceFactory(fileSystemFactory, hiveConfig);

        long length = fileSystemFactory.create(session).newInputFile(location).length();
        long nowMillis = Instant.now().toEpochMilli();

        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                "",
                ImmutableList.of(),
                projectedColumns,
                ImmutableList.of(),
                ImmutableMap.of(),
                location.toString(),
                OptionalInt.empty(),
                length,
                nowMillis);

        return new PageSourceParameters(factory, length, nowMillis, columnMappings, getTablePropertiesWithEncoding(tableColumns, BINARY_ENCODING));
    }

    private record PageSourceParameters(IonPageSourceFactory factory, long length, long nowMillis, List<HivePageSourceProvider.ColumnMapping> columnMappings, Map<String, String> tableProperties)
    { }

    /**
     * Creates table properties for IonFileWriter with encoding flag.
     */
    private static Map<String, String> getTablePropertiesWithEncoding(List<HiveColumnHandle> tableColumns, String encoding)
    {
        return ImmutableMap.<String, String>builder()
                .put(LIST_COLUMNS, tableColumns.stream().map(HiveColumnHandle::getName).collect(Collectors.joining(",")))
                .put(LIST_COLUMN_TYPES, tableColumns.stream().map(HiveColumnHandle::getHiveType).map(HiveType::toString).collect(Collectors.joining(",")))
                .put(ION_ENCODING_PROPERTY, encoding)
                .buildOrThrow();
    }
}
