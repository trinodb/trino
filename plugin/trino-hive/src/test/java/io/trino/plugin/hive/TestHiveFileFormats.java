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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriterOptions;
import io.trino.plugin.hive.avro.AvroFileWriterFactory;
import io.trino.plugin.hive.avro.AvroPageSourceFactory;
import io.trino.plugin.hive.line.CsvFileWriterFactory;
import io.trino.plugin.hive.line.CsvPageSourceFactory;
import io.trino.plugin.hive.line.JsonFileWriterFactory;
import io.trino.plugin.hive.line.JsonPageSourceFactory;
import io.trino.plugin.hive.line.OpenXJsonFileWriterFactory;
import io.trino.plugin.hive.line.OpenXJsonPageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFileWriterFactory;
import io.trino.plugin.hive.line.SimpleTextFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleTextFileWriterFactory;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetFileWriterFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.CSV;
import static io.trino.plugin.hive.HiveStorageFormat.JSON;
import static io.trino.plugin.hive.HiveStorageFormat.OPENX_JSON;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveStorageFormat.RCBINARY;
import static io.trino.plugin.hive.HiveStorageFormat.RCTEXT;
import static io.trino.plugin.hive.HiveStorageFormat.SEQUENCEFILE;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.createGenericHiveRecordCursorProvider;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.getTypes;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.testing.StructuralTestUtil.rowBlockOf;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

// Failing on multiple threads because of org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper
// uses a single record writer across all threads.
// For example org.apache.parquet.column.values.factory.DefaultValuesWriterFactory#DEFAULT_V1_WRITER_FACTORY is shared mutable state.
@Test(singleThreaded = true)
public class TestHiveFileFormats
        extends AbstractTestHiveFileFormats
{
    private static final FileFormatDataSourceStats STATS = new FileFormatDataSourceStats();
    private static final ConnectorSession PARQUET_SESSION = getHiveSession(createParquetHiveConfig(false));
    private static final ConnectorSession PARQUET_SESSION_USE_NAME = getHiveSession(createParquetHiveConfig(true));

    private static final TrinoFileSystemFactory FILE_SYSTEM_FACTORY = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
    private static final HivePageSourceFactory PARQUET_PAGE_SOURCE_FACTORY = new ParquetPageSourceFactory(FILE_SYSTEM_FACTORY, STATS, new ParquetReaderConfig(), new HiveConfig());

    @DataProvider(name = "rowCount")
    public static Object[][] rowCountProvider()
    {
        return new Object[][] {{0}, {1000}};
    }

    @DataProvider(name = "validRowAndFileSizePadding")
    public static Object[][] validFileSizePaddingProvider()
    {
        return new Object[][] {{0, 0L}, {0, 16L}, {10, 1L}, {1000, 64L}};
    }

    @BeforeClass(alwaysRun = true)
    public void setUp()
    {
        // ensure the expected timezone is configured for this VM
        assertEquals(TimeZone.getDefault().getID(),
                "America/Bahia_Banderas",
                "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testTextFile(int rowCount, long fileSizePadding)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // t_map_null_key_* must be disabled because Trino cannot produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toList());

        assertThatFileFormat(TEXTFILE)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .withFileWriterFactory(new SimpleTextFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new SimpleTextFilePageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testSequenceFile(int rowCount, long fileSizePadding)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // t_map_null_key_* must be disabled because Trino cannot produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toList());

        assertThatFileFormat(SEQUENCEFILE)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .withFileWriterFactory(new SimpleSequenceFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test")))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new SimpleSequenceFilePageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testCsvFile(int rowCount, long fileSizePadding)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // CSV table only support Hive string columns. Notice that CSV does not allow to store null, it uses an empty string instead.
                .filter(column -> column.isPartitionKey() || ("string".equals(column.getType()) && !column.getName().contains("_null_")))
                .collect(toImmutableList());

        assertTrue(testColumns.size() > 5);

        assertThatFileFormat(CSV)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .withFileWriterFactory(new CsvFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new CsvPageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test
    public void testCsvFileWithNullAndValue()
            throws Exception
    {
        assertThatFileFormat(CSV)
                .withColumns(ImmutableList.of(
                        new TestColumn("t_null_string", javaStringObjectInspector, null, utf8Slice("")), // null was converted to empty string!
                        new TestColumn("t_string", javaStringObjectInspector, "test", utf8Slice("test"))))
                .withRowsCount(2)
                .withFileWriterFactory(new CsvFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new CsvPageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testJson(int rowCount, long fileSizePadding)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // binary is not supported
                .filter(column -> !column.getName().equals("t_binary"))
                // non-string map keys are not supported
                .filter(column -> !column.getName().equals("t_map_tinyint"))
                .filter(column -> !column.getName().equals("t_map_smallint"))
                .filter(column -> !column.getName().equals("t_map_int"))
                .filter(column -> !column.getName().equals("t_map_bigint"))
                .filter(column -> !column.getName().equals("t_map_float"))
                .filter(column -> !column.getName().equals("t_map_double"))
                // null map keys are not supported
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                // decimal(38) is broken or not supported
                .filter(column -> !column.getName().equals("t_decimal_precision_38"))
                .filter(column -> !column.getName().equals("t_map_decimal_precision_38"))
                .filter(column -> !column.getName().equals("t_array_decimal_precision_38"))
                .collect(toList());

        assertThatFileFormat(JSON)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .withFileWriterFactory(new JsonFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new JsonPageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testOpenXJson(int rowCount, long fileSizePadding)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // null map keys are not supported
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toList());

        assertThatFileFormat(OPENX_JSON)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                // openx serde is not available for testing
                .withSkipGenericWriterTest()
                .withFileWriterFactory(new OpenXJsonFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER))
                .isReadableByPageSource(new OpenXJsonPageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testRcTextPageSource(int rowCount, long fileSizePadding)
            throws Exception
    {
        assertThatFileFormat(RCTEXT)
                .withColumns(TEST_COLUMNS)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testRcTextOptimizedWriter(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // t_map_null_key_* must be disabled because Trino cannot produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toImmutableList());

        assertThatFileFormat(RCTEXT)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileWriterFactory(new RcFileFileWriterFactory(FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testRcBinaryPageSource(int rowCount)
            throws Exception
    {
        // RCBinary does not support complex type as key of a map and interprets empty VARCHAR as nulls
        // Hive binary writers are broken for timestamps
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !testColumn.getName().equals("t_empty_varchar"))
                .filter(TestHiveFileFormats::withoutTimestamps)
                .collect(toList());

        assertThatFileFormat(RCBINARY)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testRcBinaryOptimizedWriter(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // RCBinary interprets empty VARCHAR as nulls
                .filter(testColumn -> !testColumn.getName().equals("t_empty_varchar"))
                // t_map_null_key_* must be disabled because Trino cannot produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toList());

        // Hive cannot read timestamps from old files
        List<TestColumn> testColumnsNoTimestamps = testColumns.stream()
                .filter(TestHiveFileFormats::withoutTimestamps)
                .collect(toList());

        assertThatFileFormat(RCBINARY)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                // generic Hive writer corrupts timestamps
                .withSkipGenericWriterTest()
                .withFileWriterFactory(new RcFileFileWriterFactory(FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE))
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()))
                .withColumns(testColumnsNoTimestamps)
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testOrc(int rowCount, long fileSizePadding)
            throws Exception
    {
        assertThatFileFormat(ORC)
                .withColumns(TEST_COLUMNS)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .isReadableByPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testOrcOptimizedWriter(int rowCount, long fileSizePadding)
            throws Exception
    {
        HiveSessionProperties hiveSessionProperties = new HiveSessionProperties(
                new HiveConfig(),
                new HiveFormatsConfig(),
                new OrcReaderConfig(),
                new OrcWriterConfig()
                        .setValidationPercentage(100.0),
                new ParquetReaderConfig(),
                new ParquetWriterConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(hiveSessionProperties.getSessionProperties())
                .build();

        // A Trino page cannot contain a map with null keys, so a page based writer cannot write null keys
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toList());

        assertThatFileFormat(ORC)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileSizePadding(fileSizePadding)
                .withFileWriterFactory(new OrcFileWriterFactory(TESTING_TYPE_MANAGER, new NodeVersion("test"), STATS, new OrcWriterOptions(), HDFS_FILE_SYSTEM_FACTORY))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC));
    }

    @Test(dataProvider = "rowCount")
    public void testOrcUseColumnNames(int rowCount)
            throws Exception
    {
        ConnectorSession session = getHiveSession(new HiveConfig(), new OrcReaderConfig().setUseColumnNames(true));

        // Hive binary writers are broken for timestamps
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(TestHiveFileFormats::withoutTimestamps)
                .collect(toImmutableList());

        assertThatFileFormat(ORC)
                .withWriteColumns(testColumns)
                .withRowsCount(rowCount)
                .withReadColumns(Lists.reverse(testColumns))
                .withSession(session)
                .isReadableByPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC));
    }

    @Test(dataProvider = "rowCount")
    public void testOrcUseColumnNameLowerCaseConversion(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumnsUpperCase = TEST_COLUMNS.stream()
                .map(testColumn -> new TestColumn(testColumn.getName().toUpperCase(Locale.ENGLISH), testColumn.getObjectInspector(), testColumn.getWriteValue(), testColumn.getExpectedValue(), testColumn.isPartitionKey()))
                .collect(toList());
        ConnectorSession session = getHiveSession(new HiveConfig(), new OrcReaderConfig().setUseColumnNames(true));

        assertThatFileFormat(ORC)
                .withWriteColumns(testColumnsUpperCase)
                .withRowsCount(rowCount)
                .withReadColumns(TEST_COLUMNS)
                .withSession(session)
                .isReadableByPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC));
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testAvro(int rowCount, long fileSizePadding)
            throws Exception
    {
        assertThatFileFormat(AVRO)
                .withColumns(getTestColumnsSupportedByAvro())
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .withFileWriterFactory(new AvroFileWriterFactory(FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test_version")))
                .isReadableByPageSource(new AvroPageSourceFactory(FILE_SYSTEM_FACTORY))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "rowCount")
    public void testAvroFileInSymlinkTable(int rowCount)
            throws Exception
    {
        File file = File.createTempFile("trino_test", AVRO.name());
        //noinspection ResultOfMethodCallIgnored
        file.delete();
        try {
            FileSplit split = createTestFileHive(file.getAbsolutePath(), AVRO, HiveCompressionCodec.NONE, getTestColumnsSupportedByAvro(), rowCount);
            Properties splitProperties = new Properties();
            splitProperties.setProperty(FILE_INPUT_FORMAT, SymlinkTextInputFormat.class.getName());
            splitProperties.setProperty(SERIALIZATION_LIB, AVRO.getSerde());
            testCursorProvider(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), split, splitProperties, getTestColumnsSupportedByAvro(), SESSION, file.length(), rowCount);
            testPageSourceFactory(new AvroPageSourceFactory(FILE_SYSTEM_FACTORY), split, AVRO, getTestColumnsSupportedByAvro(), SESSION, file.length(), rowCount);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    private static List<TestColumn> getTestColumnsSupportedByAvro()
    {
        // Avro only supports String for Map keys, and doesn't support smallint or tinyint.
        return TEST_COLUMNS.stream()
                .filter(column -> !column.getName().startsWith("t_map_") || column.getName().equals("t_map_string"))
                .filter(column -> !column.getName().endsWith("_smallint"))
                .filter(column -> !column.getName().endsWith("_tinyint"))
                .collect(toList());
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testParquetPageSource(int rowCount, long fileSizePadding)
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withSession(PARQUET_SESSION)
                .withRowsCount(rowCount)
                .withFileSizePadding(fileSizePadding)
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);
    }

    @Test(dataProvider = "validRowAndFileSizePadding")
    public void testParquetPageSourceGzip(int rowCount, long fileSizePadding)
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withSession(PARQUET_SESSION)
                .withCompressionCodec(HiveCompressionCodec.GZIP)
                .withFileSizePadding(fileSizePadding)
                .withRowsCount(rowCount)
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);
    }

    @Test(dataProvider = "rowCount")
    public void testParquetWriter(int rowCount)
            throws Exception
    {
        ConnectorSession session = getHiveSession(new HiveConfig(), new ParquetWriterConfig().setValidationPercentage(100));

        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        assertThatFileFormat(PARQUET)
                .withSession(session)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileWriterFactory(new ParquetFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, new NodeVersion("test-version"), TESTING_TYPE_MANAGER, new HiveConfig(), STATS))
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);
    }

    @Test(dataProvider = "rowCount")
    public void testParquetPageSourceSchemaEvolution(int rowCount)
            throws Exception
    {
        List<TestColumn> writeColumns = getTestColumnsSupportedByParquet();

        // test index-based access
        List<TestColumn> readColumns = writeColumns.stream()
                .map(column -> new TestColumn(
                        column.getName() + "_new",
                        column.getObjectInspector(),
                        column.getWriteValue(),
                        column.getExpectedValue(),
                        column.isPartitionKey()))
                .collect(toList());
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withSession(PARQUET_SESSION)
                .withRowsCount(rowCount)
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);

        // test name-based access
        readColumns = Lists.reverse(writeColumns);
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withSession(PARQUET_SESSION_USE_NAME)
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);
    }

    private static List<TestColumn> getTestColumnsSupportedByParquet()
    {
        // Write of complex hive data to Parquet is broken
        // TODO: empty arrays or maps with null keys don't seem to work
        // Parquet does not support DATE
        // Hive binary writers are broken for timestamps
        return TEST_COLUMNS.stream()
                .filter(TestHiveFileFormats::withoutTimestamps)
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .filter(column -> !column.getName().equals("t_null_array_int"))
                .filter(column -> !column.getName().equals("t_array_empty"))
                .filter(column -> column.isPartitionKey() || (
                        !hasType(column.getObjectInspector(), PrimitiveCategory.DATE)) &&
                        !hasType(column.getObjectInspector(), PrimitiveCategory.SHORT) &&
                        !hasType(column.getObjectInspector(), PrimitiveCategory.BYTE))
                .collect(toList());
    }

    @Test
    public void testTruncateVarcharColumn()
            throws Exception
    {
        TestColumn writeColumn = new TestColumn("varchar_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(4)), new HiveVarchar("test", 4), utf8Slice("test"));
        TestColumn readColumn = new TestColumn("varchar_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(3)), new HiveVarchar("tes", 3), utf8Slice("tes"));

        assertThatFileFormat(RCTEXT)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(RCBINARY)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(ORC)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC));

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withSession(PARQUET_SESSION)
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);

        assertThatFileFormat(AVRO)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withFileWriterFactory(new AvroFileWriterFactory(FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test_version")))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new AvroPageSourceFactory(FILE_SYSTEM_FACTORY));

        assertThatFileFormat(SEQUENCEFILE)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withFileWriterFactory(new SimpleSequenceFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test")))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new SimpleSequenceFilePageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));

        assertThatFileFormat(TEXTFILE)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withFileWriterFactory(new SimpleTextFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new SimpleTextFilePageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testAvroProjectedColumns(int rowCount)
            throws Exception
    {
        List<TestColumn> supportedColumns = getTestColumnsSupportedByAvro();
        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(AVRO)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withFileWriterFactory(new AvroFileWriterFactory(FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test_version")))
                .isReadableByRecordCursorPageSource(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new AvroPageSourceFactory(FILE_SYSTEM_FACTORY));
    }

    @Test(dataProvider = "rowCount")
    public void testParquetProjectedColumns(int rowCount)
            throws Exception
    {
        List<TestColumn> supportedColumns = getTestColumnsSupportedByParquet();
        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withSession(PARQUET_SESSION)
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);

        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withSession(PARQUET_SESSION_USE_NAME)
                .isReadableByPageSource(PARQUET_PAGE_SOURCE_FACTORY);
    }

    @Test(dataProvider = "rowCount")
    public void testORCProjectedColumns(int rowCount)
            throws Exception
    {
        List<TestColumn> supportedColumns = TEST_COLUMNS;
        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        ConnectorSession session = getHiveSession(new HiveConfig(), new OrcReaderConfig().setUseColumnNames(true));
        assertThatFileFormat(ORC)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .isReadableByPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC));

        assertThatFileFormat(ORC)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC));
    }

    @Test(dataProvider = "rowCount")
    public void testSequenceFileProjectedColumns(int rowCount)
            throws Exception
    {
        List<TestColumn> supportedColumns = TEST_COLUMNS.stream()
                .filter(column -> !column.getName().equals("t_map_null_key_complex_key_value"))
                .collect(toList());

        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(SEQUENCEFILE)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withFileWriterFactory(new SimpleSequenceFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test")))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new SimpleSequenceFilePageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testTextFileProjectedColumns(int rowCount)
            throws Exception
    {
        List<TestColumn> supportedColumns = TEST_COLUMNS.stream()
                // t_map_null_key_* must be disabled because Trino cannot produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toList());

        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(TEXTFILE)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withFileWriterFactory(new SimpleTextFileWriterFactory(HDFS_FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER))
                .isReadableByRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new SimpleTextFilePageSourceFactory(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testRCTextProjectedColumns(int rowCount)
            throws Exception
    {
        List<TestColumn> supportedColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> {
                    // TODO: This is a bug in the RC text reader
                    // RC file does not support complex type as key of a map
                    return !testColumn.getName().equals("t_struct_null")
                            && !testColumn.getName().equals("t_map_null_key_complex_key_value");
                })
                .collect(toImmutableList());

        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(RCTEXT)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .isReadableByRecordCursorPageSource(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "rowCount")
    public void testRCTextProjectedColumnsPageSource(int rowCount)
            throws Exception
    {
        List<TestColumn> supportedColumns = TEST_COLUMNS;
        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(RCTEXT)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testRCBinaryProjectedColumns(int rowCount)
            throws Exception
    {
        // RCBinary does not support complex type as key of a map and interprets empty VARCHAR as nulls
        List<TestColumn> supportedColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> {
                    String name = testColumn.getName();
                    return !name.equals("t_map_null_key_complex_key_value") && !name.equals("t_empty_varchar");
                })
                .collect(toList());

        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(RCBINARY)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                // generic Hive writer corrupts timestamps
                .withSkipGenericWriterTest()
                .withFileWriterFactory(new RcFileFileWriterFactory(FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE))
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test(dataProvider = "rowCount")
    public void testRCBinaryProjectedColumnsPageSource(int rowCount)
            throws Exception
    {
        // RCBinary does not support complex type as key of a map and interprets empty VARCHAR as nulls
        List<TestColumn> supportedColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !testColumn.getName().equals("t_empty_varchar"))
                .collect(toList());

        List<TestColumn> regularColumns = getRegularColumns(supportedColumns);
        List<TestColumn> partitionColumns = getPartitionColumns(supportedColumns);

        // Created projected columns for all regular supported columns
        ImmutableList.Builder<TestColumn> writeColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TestColumn> readeColumnsBuilder = ImmutableList.builder();
        generateProjectedColumns(regularColumns, writeColumnsBuilder, readeColumnsBuilder);

        List<TestColumn> writeColumns = writeColumnsBuilder.addAll(partitionColumns).build();
        List<TestColumn> readColumns = readeColumnsBuilder.addAll(partitionColumns).build();

        assertThatFileFormat(RCBINARY)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                // generic Hive writer corrupts timestamps
                .withSkipGenericWriterTest()
                .withFileWriterFactory(new RcFileFileWriterFactory(FILE_SYSTEM_FACTORY, TESTING_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE))
                .isReadableByPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()));
    }

    @Test
    public void testFailForLongVarcharPartitionColumn()
            throws Exception
    {
        TestColumn partitionColumn = new TestColumn("partition_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(3)), "test", utf8Slice("tes"), true);
        TestColumn varcharColumn = new TestColumn("varchar_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(3)), new HiveVarchar("tes", 3), utf8Slice("tes"));

        List<TestColumn> columns = ImmutableList.of(partitionColumn, varcharColumn);

        HiveErrorCode expectedErrorCode = HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
        String expectedMessage = "Invalid partition value 'test' for varchar(3) partition key: partition_column";

        assertThatFileFormat(RCTEXT)
                .withColumns(columns)
                .isFailingForPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()), expectedErrorCode, expectedMessage)
                .isFailingForRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(RCBINARY)
                .withColumns(columns)
                .isFailingForPageSource(new RcFilePageSourceFactory(FILE_SYSTEM_FACTORY, new HiveConfig()), expectedErrorCode, expectedMessage)
                .isFailingForRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(ORC)
                .withColumns(columns)
                .isFailingForPageSource(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC), expectedErrorCode, expectedMessage);

        assertThatFileFormat(PARQUET)
                .withColumns(columns)
                .withSession(PARQUET_SESSION)
                .isFailingForPageSource(PARQUET_PAGE_SOURCE_FACTORY, expectedErrorCode, expectedMessage);

        assertThatFileFormat(SEQUENCEFILE)
                .withColumns(columns)
                .isFailingForRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(TEXTFILE)
                .withColumns(columns)
                .isFailingForRecordCursor(createGenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);
    }

    private void testRecordPageSource(
            HiveRecordCursorProvider cursorProvider,
            FileSplit split,
            HiveStorageFormat storageFormat,
            List<TestColumn> testReadColumns,
            ConnectorSession session,
            long fileSize,
            int rowCount)
            throws Exception
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, storageFormat.getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, storageFormat.getSerde());
        ConnectorPageSource pageSource = createPageSourceFromCursorProvider(cursorProvider, split, splitProperties, fileSize, testReadColumns, session);
        checkPageSource(pageSource, testReadColumns, getTypes(getColumnHandles(testReadColumns)), rowCount);
    }

    private void testCursorProvider(
            HiveRecordCursorProvider cursorProvider,
            FileSplit split,
            HiveStorageFormat storageFormat,
            List<TestColumn> testReadColumns,
            ConnectorSession session,
            long fileSize,
            int rowCount)
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, storageFormat.getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, storageFormat.getSerde());
        testCursorProvider(cursorProvider, split, splitProperties, testReadColumns, session, fileSize, rowCount);
    }

    private void testCursorProvider(
            HiveRecordCursorProvider cursorProvider,
            FileSplit split,
            Properties splitProperties,
            List<TestColumn> testReadColumns,
            ConnectorSession session,
            long fileSize,
            int rowCount)
    {
        ConnectorPageSource pageSource = createPageSourceFromCursorProvider(cursorProvider, split, splitProperties, fileSize, testReadColumns, session);
        RecordCursor cursor = ((RecordPageSource) pageSource).getCursor();
        checkCursor(cursor, testReadColumns, rowCount);
    }

    private ConnectorPageSource createPageSourceFromCursorProvider(
            HiveRecordCursorProvider cursorProvider,
            FileSplit split,
            Properties splitProperties,
            long fileSize,
            List<TestColumn> testReadColumns,
            ConnectorSession session)
    {
        // Use full columns in split properties
        ImmutableList.Builder<String> splitPropertiesColumnNames = ImmutableList.builder();
        ImmutableList.Builder<String> splitPropertiesColumnTypes = ImmutableList.builder();
        Set<String> baseColumnNames = new HashSet<>();

        for (TestColumn testReadColumn : testReadColumns) {
            String name = testReadColumn.getBaseName();
            if (!baseColumnNames.contains(name) && !testReadColumn.isPartitionKey()) {
                baseColumnNames.add(name);
                splitPropertiesColumnNames.add(name);
                splitPropertiesColumnTypes.add(testReadColumn.getBaseObjectInspector().getTypeName());
            }
        }

        splitProperties.setProperty(
                "columns",
                splitPropertiesColumnNames.build().stream()
                        .collect(Collectors.joining(",")));

        splitProperties.setProperty(
                "columns.types",
                splitPropertiesColumnTypes.build().stream()
                        .collect(Collectors.joining(",")));

        List<HivePartitionKey> partitionKeys = testReadColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(input -> new HivePartitionKey(input.getName(), (String) input.getWriteValue()))
                .collect(toList());

        String partitionName = String.join("/", partitionKeys.stream()
                .map(partitionKey -> format("%s=%s", partitionKey.getName(), partitionKey.getValue()))
                .collect(toImmutableList()));

        Configuration configuration = newEmptyConfiguration();
        configuration.set("io.compression.codecs", LzoCodec.class.getName() + "," + LzopCodec.class.getName());

        List<HiveColumnHandle> columnHandles = getColumnHandles(testReadColumns);
        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                partitionName,
                partitionKeys,
                columnHandles,
                ImmutableList.of(),
                TableToPartitionMapping.empty(),
                split.getPath().toString(),
                OptionalInt.empty(),
                fileSize,
                Instant.now().toEpochMilli());

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(),
                ImmutableSet.of(cursorProvider),
                configuration,
                session,
                Location.of(split.getPath().toString()),
                OptionalInt.empty(),
                split.getStart(),
                split.getLength(),
                fileSize,
                splitProperties,
                TupleDomain.all(),
                columnHandles,
                TESTING_TYPE_MANAGER,
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                false,
                NO_ACID_TRANSACTION,
                columnMappings);

        return pageSource.get();
    }

    private void testPageSourceFactory(
            HivePageSourceFactory sourceFactory,
            FileSplit split,
            HiveStorageFormat storageFormat,
            List<TestColumn> testReadColumns,
            ConnectorSession session,
            long fileSize,
            int rowCount)
            throws IOException
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, storageFormat.getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, storageFormat.getSerde());

        // Use full columns in split properties
        ImmutableList.Builder<String> splitPropertiesColumnNames = ImmutableList.builder();
        ImmutableList.Builder<String> splitPropertiesColumnTypes = ImmutableList.builder();
        Set<String> baseColumnNames = new HashSet<>();

        for (TestColumn testReadColumn : testReadColumns) {
            String name = testReadColumn.getBaseName();
            if (!baseColumnNames.contains(name) && !testReadColumn.isPartitionKey()) {
                baseColumnNames.add(name);
                splitPropertiesColumnNames.add(name);
                splitPropertiesColumnTypes.add(testReadColumn.getBaseObjectInspector().getTypeName());
            }
        }

        splitProperties.setProperty("columns", splitPropertiesColumnNames.build().stream().collect(Collectors.joining(",")));
        splitProperties.setProperty("columns.types", splitPropertiesColumnTypes.build().stream().collect(Collectors.joining(",")));

        List<HivePartitionKey> partitionKeys = testReadColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(input -> new HivePartitionKey(input.getName(), (String) input.getWriteValue()))
                .collect(toList());

        String partitionName = String.join("/", partitionKeys.stream()
                .map(partitionKey -> format("%s=%s", partitionKey.getName(), partitionKey.getValue()))
                .collect(toImmutableList()));

        List<HiveColumnHandle> columnHandles = getColumnHandles(testReadColumns);

        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                partitionName,
                partitionKeys,
                columnHandles,
                ImmutableList.of(),
                TableToPartitionMapping.empty(),
                split.getPath().toString(),
                OptionalInt.empty(),
                fileSize,
                Instant.now().toEpochMilli());

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(sourceFactory),
                ImmutableSet.of(),
                newEmptyConfiguration(),
                session,
                Location.of(split.getPath().toString()),
                OptionalInt.empty(),
                split.getStart(),
                split.getLength(),
                fileSize,
                splitProperties,
                TupleDomain.all(),
                columnHandles,
                TESTING_TYPE_MANAGER,
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                false,
                NO_ACID_TRANSACTION,
                columnMappings);

        assertTrue(pageSource.isPresent());

        checkPageSource(pageSource.get(), testReadColumns, getTypes(columnHandles), rowCount);
    }

    public static boolean hasType(ObjectInspector objectInspector, PrimitiveCategory... types)
    {
        if (objectInspector instanceof PrimitiveObjectInspector primitiveInspector) {
            PrimitiveCategory primitiveCategory = primitiveInspector.getPrimitiveCategory();
            for (PrimitiveCategory type : types) {
                if (primitiveCategory == type) {
                    return true;
                }
            }
            return false;
        }
        if (objectInspector instanceof ListObjectInspector listInspector) {
            return hasType(listInspector.getListElementObjectInspector(), types);
        }
        if (objectInspector instanceof MapObjectInspector mapInspector) {
            return hasType(mapInspector.getMapKeyObjectInspector(), types) ||
                    hasType(mapInspector.getMapValueObjectInspector(), types);
        }
        if (objectInspector instanceof StructObjectInspector structObjectInspector) {
            for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                if (hasType(field.getFieldObjectInspector(), types)) {
                    return true;
                }
            }
            return false;
        }
        throw new IllegalArgumentException("Unknown object inspector type " + objectInspector);
    }

    private static boolean withoutNullMapKeyTests(TestColumn testColumn)
    {
        String name = testColumn.getName();
        return !name.equals("t_map_null_key") &&
                !name.equals("t_map_null_key_complex_key_value") &&
                !name.equals("t_map_null_key_complex_value");
    }

    private static boolean withoutTimestamps(TestColumn testColumn)
    {
        String name = testColumn.getName();
        return !name.equals("t_timestamp") &&
                !name.equals("t_map_timestamp") &&
                !name.equals("t_array_timestamp");
    }

    private FileFormatAssertion assertThatFileFormat(HiveStorageFormat hiveStorageFormat)
    {
        return new FileFormatAssertion(hiveStorageFormat.name())
                .withStorageFormat(hiveStorageFormat);
    }

    private static HiveConfig createParquetHiveConfig(boolean useParquetColumnNames)
    {
        return new HiveConfig()
                .setUseParquetColumnNames(useParquetColumnNames);
    }

    private void generateProjectedColumns(List<TestColumn> childColumns, ImmutableList.Builder<TestColumn> testFullColumnsBuilder, ImmutableList.Builder<TestColumn> testDereferencedColumnsBuilder)
    {
        for (int i = 0; i < childColumns.size(); i++) {
            TestColumn childColumn = childColumns.get(i);
            checkState(childColumn.getDereferenceIndices().size() == 0);
            ObjectInspector newObjectInspector = getStandardStructObjectInspector(
                    ImmutableList.of("field0"),
                    ImmutableList.of(childColumn.getObjectInspector()));

            HiveType hiveType = (HiveType.valueOf(childColumn.getObjectInspector().getTypeName()));
            Type trinoType = hiveType.getType(TESTING_TYPE_MANAGER);

            List<Object> list = new ArrayList<>();
            list.add(childColumn.getWriteValue());

            TestColumn newProjectedColumn = new TestColumn(
                    "new_col" + i, newObjectInspector,
                    ImmutableList.of("field0"),
                    ImmutableList.of(0),
                    childColumn.getObjectInspector(),
                    childColumn.getWriteValue(),
                    childColumn.getExpectedValue(),
                    false);

            TestColumn newFullColumn = new TestColumn("new_col" + i, newObjectInspector, list, rowBlockOf(ImmutableList.of(trinoType), childColumn.getExpectedValue()));

            testFullColumnsBuilder.add(newFullColumn);
            testDereferencedColumnsBuilder.add(newProjectedColumn);
        }
    }

    private List<TestColumn> getRegularColumns(List<TestColumn> columns)
    {
        return columns.stream()
                .filter(column -> !column.isPartitionKey())
                .collect(toImmutableList());
    }

    private List<TestColumn> getPartitionColumns(List<TestColumn> columns)
    {
        return columns.stream()
                .filter(TestColumn::isPartitionKey)
                .collect(toImmutableList());
    }

    private class FileFormatAssertion
    {
        private final String formatName;
        private HiveStorageFormat storageFormat;
        private HiveCompressionCodec compressionCodec = HiveCompressionCodec.NONE;
        private List<TestColumn> writeColumns;
        private List<TestColumn> readColumns;
        private ConnectorSession session = SESSION;
        private int rowsCount = 1000;
        private boolean skipGenericWrite;
        private HiveFileWriterFactory fileWriterFactory;
        private long fileSizePadding;

        private FileFormatAssertion(String formatName)
        {
            this.formatName = requireNonNull(formatName, "formatName is null");
        }

        public FileFormatAssertion withStorageFormat(HiveStorageFormat storageFormat)
        {
            this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
            return this;
        }

        public FileFormatAssertion withCompressionCodec(HiveCompressionCodec compressionCodec)
        {
            this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
            return this;
        }

        public FileFormatAssertion withSkipGenericWriterTest()
        {
            this.skipGenericWrite = true;
            return this;
        }

        public FileFormatAssertion withFileWriterFactory(HiveFileWriterFactory fileWriterFactory)
        {
            this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
            return this;
        }

        public FileFormatAssertion withColumns(List<TestColumn> inputColumns)
        {
            withWriteColumns(inputColumns);
            withReadColumns(inputColumns);
            return this;
        }

        public FileFormatAssertion withWriteColumns(List<TestColumn> writeColumns)
        {
            this.writeColumns = requireNonNull(writeColumns, "writeColumns is null");
            return this;
        }

        public FileFormatAssertion withReadColumns(List<TestColumn> readColumns)
        {
            this.readColumns = requireNonNull(readColumns, "readColumns is null");
            return this;
        }

        public FileFormatAssertion withRowsCount(int rowsCount)
        {
            this.rowsCount = rowsCount;
            return this;
        }

        public FileFormatAssertion withSession(ConnectorSession session)
        {
            this.session = requireNonNull(session, "session is null");
            return this;
        }

        public FileFormatAssertion withFileSizePadding(long fileSizePadding)
        {
            this.fileSizePadding = fileSizePadding;
            return this;
        }

        public FileFormatAssertion isReadableByPageSource(HivePageSourceFactory pageSourceFactory)
                throws Exception
        {
            assertRead(Optional.of(pageSourceFactory), Optional.empty(), false);
            return this;
        }

        public FileFormatAssertion isReadableByRecordCursorPageSource(HiveRecordCursorProvider cursorProvider)
                throws Exception
        {
            assertRead(Optional.empty(), Optional.of(cursorProvider), true);
            return this;
        }

        public FileFormatAssertion isReadableByRecordCursor(HiveRecordCursorProvider cursorProvider)
                throws Exception
        {
            assertRead(Optional.empty(), Optional.of(cursorProvider), false);
            return this;
        }

        public FileFormatAssertion isFailingForPageSource(HivePageSourceFactory pageSourceFactory, HiveErrorCode expectedErrorCode, String expectedMessage)
                throws Exception
        {
            assertFailure(Optional.of(pageSourceFactory), Optional.empty(), expectedErrorCode, expectedMessage, false);
            return this;
        }

        public FileFormatAssertion isFailingForRecordCursor(HiveRecordCursorProvider cursorProvider, HiveErrorCode expectedErrorCode, String expectedMessage)
                throws Exception
        {
            assertFailure(Optional.empty(), Optional.of(cursorProvider), expectedErrorCode, expectedMessage, false);
            return this;
        }

        private void assertRead(Optional<HivePageSourceFactory> pageSourceFactory, Optional<HiveRecordCursorProvider> cursorProvider, boolean withRecordPageSource)
                throws Exception
        {
            assertNotNull(storageFormat, "storageFormat must be specified");
            assertNotNull(writeColumns, "writeColumns must be specified");
            assertNotNull(readColumns, "readColumns must be specified");
            assertNotNull(session, "session must be specified");
            assertTrue(rowsCount >= 0, "rowsCount must be non-negative");

            String compressionSuffix = compressionCodec.getHiveCompressionKind()
                    .map(CompressionKind::getFileExtension)
                    .orElse("");

            File file = File.createTempFile("trino_test", formatName + compressionSuffix);
            file.delete();
            for (boolean testFileWriter : ImmutableList.of(false, true)) {
                try {
                    FileSplit split;
                    if (testFileWriter) {
                        if (fileWriterFactory == null) {
                            continue;
                        }
                        split = createTestFileTrino(file.getAbsolutePath(), storageFormat, compressionCodec, writeColumns, session, rowsCount, fileWriterFactory);
                    }
                    else {
                        if (skipGenericWrite) {
                            continue;
                        }
                        split = createTestFileHive(file.getAbsolutePath(), storageFormat, compressionCodec, writeColumns, rowsCount);
                    }

                    long fileSize = split.getLength() + fileSizePadding;
                    if (pageSourceFactory.isPresent()) {
                        testPageSourceFactory(pageSourceFactory.get(), split, storageFormat, readColumns, session, fileSize, rowsCount);
                    }
                    if (cursorProvider.isPresent()) {
                        if (withRecordPageSource) {
                            testRecordPageSource(cursorProvider.get(), split, storageFormat, readColumns, session, fileSize, rowsCount);
                        }
                        else {
                            testCursorProvider(cursorProvider.get(), split, storageFormat, readColumns, session, fileSize, rowsCount);
                        }
                    }
                }
                finally {
                    //noinspection ResultOfMethodCallIgnored
                    file.delete();
                }
            }
        }

        private void assertFailure(
                Optional<HivePageSourceFactory> pageSourceFactory,
                Optional<HiveRecordCursorProvider> cursorProvider,
                HiveErrorCode expectedErrorCode,
                String expectedMessage,
                boolean withRecordPageSource)
        {
            assertTrinoExceptionThrownBy(() -> assertRead(pageSourceFactory, cursorProvider, withRecordPageSource))
                    .hasErrorCode(expectedErrorCode)
                    .hasMessage(expectedMessage);
        }
    }
}
