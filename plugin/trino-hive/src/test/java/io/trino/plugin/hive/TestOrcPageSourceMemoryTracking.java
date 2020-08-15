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
import io.airlift.slice.Slice;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Split;
import io.trino.operator.DriverContext;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.SourceOperator;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.TableScanOperator.TableScanOperatorFactory;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.RowExpression;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.NullMemoryManager;
import org.apache.orc.impl.WriterImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertBetweenInclusive;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.ZLIB;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestOrcPageSourceMemoryTracking
{
    private static final String ORC_RECORD_WRITER = OrcOutputFormat.class.getName() + "$OrcRecordWriter";
    private static final Constructor<? extends RecordWriter> WRITER_CONSTRUCTOR = getOrcWriterConstructor();
    private static final Configuration CONFIGURATION = new Configuration(false);
    private static final int NUM_ROWS = 50000;
    private static final int STRIPE_ROWS = 20000;
    private static final FunctionManager functionManager = createTestingFunctionManager();
    private static final ExpressionCompiler EXPRESSION_COMPILER = new ExpressionCompiler(functionManager, new PageFunctionCompiler(functionManager, 0));
    private static final ConnectorSession UNCACHED_SESSION = HiveTestUtils.getHiveSession(new HiveConfig(), new OrcReaderConfig().setTinyStripeThreshold(DataSize.of(0, BYTE)));
    private static final ConnectorSession CACHED_SESSION = SESSION;

    private final Random random = new Random();
    private final List<TestColumn> testColumns = ImmutableList.<TestColumn>builder()
            .add(new TestColumn("p_empty_string", javaStringObjectInspector, () -> "", true))
            .add(new TestColumn("p_string", javaStringObjectInspector, () -> Long.toHexString(random.nextLong()), false))
            .build();

    private File tempFile;
    private TestPreparer testPreparer;

    @DataProvider(name = "rowCount")
    public static Object[][] rowCount()
    {
        return new Object[][] {{50_000}, {10_000}, {5_000}};
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        tempFile = File.createTempFile("trino_test_orc_page_source_memory_tracking", "orc");
        tempFile.delete();
        testPreparer = new TestPreparer(tempFile.getAbsolutePath());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tempFile.delete();
    }

    @Test
    public void testPageSourceUncached()
            throws Exception
    {
        testPageSource(false);
    }

    @Test
    public void testPageSourceCached()
            throws Exception
    {
        testPageSource(true);
    }

    private void testPageSource(boolean useCache)
            throws Exception
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        ConnectorPageSource pageSource = testPreparer.newPageSource(stats, useCache ? CACHED_SESSION : UNCACHED_SESSION);

        if (useCache) {
            // file is fully cached
            assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize(), testPreparer.getFileSize() + 200);
        }
        else {
            assertEquals(pageSource.getMemoryUsage(), 0);
        }

        long memoryUsage = -1;
        int totalRows = 0;
        while (totalRows < 20000) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);

            if (memoryUsage == -1) {
                // Memory usage before lazy-loading the block
                if (useCache) {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize(), testPreparer.getFileSize() + 2000);
                }
                else {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), 0L, 1000L);
                }
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                memoryUsage = pageSource.getMemoryUsage();
                // Memory usage after lazy-loading the actual block
                if (useCache) {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize() + 270_000, testPreparer.getFileSize() + 280_000);
                }
                else {
                    assertBetweenInclusive(memoryUsage, 460_000L, 469_999L);
                }
            }
            else {
                assertEquals(pageSource.getMemoryUsage(), memoryUsage);
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                assertEquals(pageSource.getMemoryUsage(), memoryUsage);
            }
            totalRows += page.getPositionCount();
        }

        memoryUsage = -1;
        while (totalRows < 40000) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);

            if (memoryUsage == -1) {
                // Memory usage before lazy-loading the block
                if (useCache) {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize(), testPreparer.getFileSize() + 2000);
                }
                else {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), 0L, 1000L);
                }
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                memoryUsage = pageSource.getMemoryUsage();
                // Memory usage after lazy-loading the actual block
                if (useCache) {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize() + 270_000, testPreparer.getFileSize() + 280_000);
                }
                else {
                    assertBetweenInclusive(memoryUsage, 460_000L, 469_999L);
                }
            }
            else {
                assertEquals(pageSource.getMemoryUsage(), memoryUsage);
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                assertEquals(pageSource.getMemoryUsage(), memoryUsage);
            }
            totalRows += page.getPositionCount();
        }

        memoryUsage = -1;
        while (totalRows < NUM_ROWS) {
            assertFalse(pageSource.isFinished());
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            Block block = page.getBlock(1);

            if (memoryUsage == -1) {
                // Memory usage before lazy-loading the block
                if (useCache) {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize(), testPreparer.getFileSize() + 2000);
                }
                else {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), 0L, 1000L);
                }
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                memoryUsage = pageSource.getMemoryUsage();
                // Memory usage after loading the actual block
                if (useCache) {
                    assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize() + 260_000, testPreparer.getFileSize() + 270_000);
                }
                else {
                    assertBetweenInclusive(memoryUsage, 360_000L, 369_999L);
                }
            }
            else {
                assertEquals(pageSource.getMemoryUsage(), memoryUsage);
                createUnboundedVarcharType().getSlice(block, block.getPositionCount() - 1); // trigger loading for lazy block
                assertEquals(pageSource.getMemoryUsage(), memoryUsage);
            }
            totalRows += page.getPositionCount();
        }

        assertFalse(pageSource.isFinished());
        assertNull(pageSource.getNextPage());
        assertTrue(pageSource.isFinished());
        if (useCache) {
            // file is fully cached
            assertBetweenInclusive(pageSource.getMemoryUsage(), testPreparer.getFileSize(), testPreparer.getFileSize() + 200);
        }
        else {
            assertEquals(pageSource.getMemoryUsage(), 0);
        }
        pageSource.close();
    }

    @Test(dataProvider = "rowCount")
    public void testMaxReadBytes(int rowCount)
            throws Exception
    {
        int maxReadBytes = 1_000;
        HiveSessionProperties hiveSessionProperties = new HiveSessionProperties(
                new HiveConfig(),
                new OrcReaderConfig()
                        .setMaxBlockSize(DataSize.ofBytes(maxReadBytes)),
                new OrcWriterConfig(),
                new ParquetReaderConfig(),
                new ParquetWriterConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(hiveSessionProperties.getSessionProperties())
                .build();
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();

        // Build a table where every row gets larger, so we can test that the "batchSize" reduces
        int numColumns = 5;
        int step = 250;
        ImmutableList.Builder<TestColumn> columnBuilder = ImmutableList.<TestColumn>builder()
                .add(new TestColumn("p_empty_string", javaStringObjectInspector, () -> "", true));
        GrowingTestColumn[] dataColumns = new GrowingTestColumn[numColumns];
        for (int i = 0; i < numColumns; i++) {
            dataColumns[i] = new GrowingTestColumn("p_string" + "_" + i, javaStringObjectInspector, () -> Long.toHexString(random.nextLong()), false, step * (i + 1));
            columnBuilder.add(dataColumns[i]);
        }
        List<TestColumn> testColumns = columnBuilder.build();
        File tempFile = File.createTempFile("trino_test_orc_page_source_max_read_bytes", "orc");
        tempFile.delete();

        TestPreparer testPreparer = new TestPreparer(tempFile.getAbsolutePath(), testColumns, rowCount, rowCount);
        ConnectorPageSource pageSource = testPreparer.newPageSource(stats, session);

        try {
            int positionCount = 0;
            while (true) {
                Page page = pageSource.getNextPage();
                if (pageSource.isFinished()) {
                    break;
                }
                assertNotNull(page);
                page = page.getLoadedPage();
                positionCount += page.getPositionCount();
                // assert upper bound is tight
                // ignore the first MAX_BATCH_SIZE rows given the sizes are set when loading the blocks
                if (positionCount > MAX_BATCH_SIZE) {
                    // either the block is bounded by maxReadBytes or we just load one single large block
                    // an error margin MAX_BATCH_SIZE / step is needed given the block sizes are increasing
                    assertTrue(page.getSizeInBytes() < maxReadBytes * (MAX_BATCH_SIZE / step) || 1 == page.getPositionCount());
                }
            }

            // verify the stats are correctly recorded
            Distribution distribution = stats.getMaxCombinedBytesPerRow().getAllTime();
            assertEquals((int) distribution.getCount(), 1);
            // the block is VariableWidthBlock that contains valueIsNull and offsets arrays as overhead
            assertEquals((int) distribution.getMax(), Arrays.stream(dataColumns).mapToInt(GrowingTestColumn::getMaxSize).sum() + (Integer.BYTES + Byte.BYTES) * numColumns);
            pageSource.close();
        }
        finally {
            tempFile.delete();
        }
    }

    @Test
    public void testTableScanOperator()
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        DriverContext driverContext = testPreparer.newDriverContext();
        SourceOperator operator = testPreparer.newTableScanOperator(driverContext);

        assertEquals(driverContext.getMemoryUsage(), 0);

        long memoryUsage = -1;
        int totalRows = 0;
        while (totalRows < 20000) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            if (memoryUsage == -1) {
                memoryUsage = driverContext.getMemoryUsage();
                assertBetweenInclusive(memoryUsage, 460000L, 469999L);
            }
            else {
                assertEquals(driverContext.getMemoryUsage(), memoryUsage);
            }
            totalRows += page.getPositionCount();
        }

        memoryUsage = -1;
        while (totalRows < 40000) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            if (memoryUsage == -1) {
                memoryUsage = driverContext.getMemoryUsage();
                assertBetweenInclusive(memoryUsage, 460000L, 469999L);
            }
            else {
                assertEquals(driverContext.getMemoryUsage(), memoryUsage);
            }
            totalRows += page.getPositionCount();
        }

        memoryUsage = -1;
        while (totalRows < NUM_ROWS) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);
            page.getBlock(1);
            if (memoryUsage == -1) {
                memoryUsage = driverContext.getMemoryUsage();
                assertBetweenInclusive(memoryUsage, 360000L, 369999L);
            }
            else {
                assertEquals(driverContext.getMemoryUsage(), memoryUsage);
            }
            totalRows += page.getPositionCount();
        }

        assertFalse(operator.isFinished());
        assertNull(operator.getOutput());
        assertTrue(operator.isFinished());
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test
    public void testScanFilterAndProjectOperator()
    {
        // Numbers used in assertions in this test may change when implementation is modified,
        // feel free to change them if they break in the future

        DriverContext driverContext = testPreparer.newDriverContext();
        SourceOperator operator = testPreparer.newScanFilterAndProjectOperator(driverContext);

        assertEquals(driverContext.getMemoryUsage(), 0);

        int totalRows = 0;
        while (totalRows < NUM_ROWS) {
            assertFalse(operator.isFinished());
            Page page = operator.getOutput();
            assertNotNull(page);

            // memory usage varies depending on stripe alignment
            long memoryUsage = driverContext.getMemoryUsage();
            assertTrue(memoryUsage < 1000 || (memoryUsage > 150_000 && memoryUsage < 630_000), format("Memory usage (%s) outside of bounds", memoryUsage));

            totalRows += page.getPositionCount();
        }

        // done... in the current implementation finish is not set until output returns a null page
        assertNull(operator.getOutput());
        assertTrue(operator.isFinished());
        assertBetweenInclusive(driverContext.getMemoryUsage(), 0L, 500L);
    }

    private class TestPreparer
    {
        private final FileSplit fileSplit;
        private final Properties schema;
        private final List<HiveColumnHandle> columns;
        private final List<Type> types;
        private final String partitionName;
        private final List<HivePartitionKey> partitionKeys;
        private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TestOrcPageSourceMemoryTracking-executor-%s"));
        private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("TestOrcPageSourceMemoryTracking-scheduledExecutor-%s"));

        public TestPreparer(String tempFilePath)
                throws Exception
        {
            this(tempFilePath, testColumns, NUM_ROWS, STRIPE_ROWS);
        }

        public TestPreparer(String tempFilePath, List<TestColumn> testColumns, int numRows, int stripeRows)
                throws Exception
        {
            OrcSerde serde = new OrcSerde();
            schema = new Properties();
            schema.setProperty("columns",
                    testColumns.stream()
                            .map(TestColumn::getName)
                            .collect(Collectors.joining(",")));
            schema.setProperty("columns.types",
                    testColumns.stream()
                            .map(TestColumn::getType)
                            .collect(Collectors.joining(",")));
            schema.setProperty(FILE_INPUT_FORMAT, OrcInputFormat.class.getName());
            schema.setProperty(SERIALIZATION_LIB, serde.getClass().getName());

            partitionKeys = testColumns.stream()
                    .filter(TestColumn::isPartitionKey)
                    .map(input -> new HivePartitionKey(input.getName(), (String) input.getWriteValue()))
                    .collect(toList());

            partitionName = String.join("/", partitionKeys.stream()
                    .map(partitionKey -> format("%s=%s", partitionKey.getName(), partitionKey.getValue()))
                    .collect(toImmutableList()));

            ImmutableList.Builder<HiveColumnHandle> columnsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            int nextHiveColumnIndex = 0;
            for (TestColumn testColumn : testColumns) {
                int columnIndex = testColumn.isPartitionKey() ? -1 : nextHiveColumnIndex++;

                ObjectInspector inspector = testColumn.getObjectInspector();
                HiveType hiveType = HiveType.valueOf(inspector.getTypeName());
                Type type = hiveType.getType(TESTING_TYPE_MANAGER);

                columnsBuilder.add(createBaseColumn(testColumn.getName(), columnIndex, hiveType, type, testColumn.isPartitionKey() ? PARTITION_KEY : REGULAR, Optional.empty()));
                typesBuilder.add(type);
            }
            columns = columnsBuilder.build();
            types = typesBuilder.build();

            fileSplit = createTestFile(tempFilePath, serde, null, testColumns, numRows, stripeRows);
        }

        public long getFileSize()
        {
            return fileSplit.getLength();
        }

        public ConnectorPageSource newPageSource()
        {
            return newPageSource(new FileFormatDataSourceStats(), UNCACHED_SESSION);
        }

        public ConnectorPageSource newPageSource(FileFormatDataSourceStats stats, ConnectorSession session)
        {
            OrcPageSourceFactory orcPageSourceFactory = new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_ENVIRONMENT, stats, UTC);

            List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                    partitionName,
                    partitionKeys,
                    columns,
                    ImmutableList.of(),
                    TableToPartitionMapping.empty(),
                    fileSplit.getPath(),
                    OptionalInt.empty(),
                    fileSplit.getLength(),
                    Instant.now().toEpochMilli());

            return HivePageSourceProvider.createHivePageSource(
                    ImmutableSet.of(orcPageSourceFactory),
                    ImmutableSet.of(),
                    new Configuration(false),
                    session,
                    fileSplit.getPath(),
                    OptionalInt.empty(),
                    fileSplit.getStart(),
                    fileSplit.getLength(),
                    fileSplit.getLength(),
                    schema,
                    TupleDomain.all(),
                    columns,
                    TESTING_TYPE_MANAGER,
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    false,
                    NO_ACID_TRANSACTION,
                    columnMappings).orElseThrow();
        }

        public SourceOperator newTableScanOperator(DriverContext driverContext)
        {
            ConnectorPageSource pageSource = newPageSource();
            SourceOperatorFactory sourceOperatorFactory = new TableScanOperatorFactory(
                    0,
                    new PlanNodeId("0"),
                    (session, split, table, columnHandles, dynamicFilter) -> pageSource,
                    TEST_TABLE_HANDLE,
                    columns.stream().map(ColumnHandle.class::cast).collect(toImmutableList()),
                    DynamicFilter.EMPTY);
            SourceOperator operator = sourceOperatorFactory.createOperator(driverContext);
            operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
            return operator;
        }

        public SourceOperator newScanFilterAndProjectOperator(DriverContext driverContext)
        {
            ConnectorPageSource pageSource = newPageSource();
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            for (int i = 0; i < types.size(); i++) {
                projectionsBuilder.add(field(i, types.get(i)));
            }
            Supplier<CursorProcessor> cursorProcessor = EXPRESSION_COMPILER.compileCursorProcessor(Optional.empty(), projectionsBuilder.build(), "key");
            Supplier<PageProcessor> pageProcessor = EXPRESSION_COMPILER.compilePageProcessor(Optional.empty(), projectionsBuilder.build());
            SourceOperatorFactory sourceOperatorFactory = new ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    new PlanNodeId("0"),
                    (session, split, table, columnHandles, dynamicFilter) -> pageSource,
                    cursorProcessor,
                    pageProcessor,
                    TEST_TABLE_HANDLE,
                    columns.stream().map(ColumnHandle.class::cast).collect(toList()),
                    DynamicFilter.EMPTY,
                    types,
                    DataSize.ofBytes(0),
                    0);
            SourceOperator operator = sourceOperatorFactory.createOperator(driverContext);
            operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
            operator.noMoreSplits();
            return operator;
        }

        private DriverContext newDriverContext()
        {
            return createTaskContext(executor, scheduledExecutor, testSessionBuilder().build())
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }
    }

    private static FileSplit createTestFile(
            String filePath,
            Serializer serializer,
            String compressionCodec,
            List<TestColumn> testColumns,
            int numRows,
            int stripeRows)
            throws Exception
    {
        // filter out partition keys, which are not written to the file
        testColumns = testColumns.stream()
                .filter(column -> !column.isPartitionKey())
                .collect(toImmutableList());

        Properties tableProperties = new Properties();
        tableProperties.setProperty(
                "columns",
                testColumns.stream()
                        .map(TestColumn::getName)
                        .collect(Collectors.joining(",")));

        tableProperties.setProperty(
                "columns.types",
                testColumns.stream()
                        .map(TestColumn::getType)
                        .collect(Collectors.joining(",")));

        serializer.initialize(CONFIGURATION, tableProperties);

        JobConf jobConf = new JobConf();
        if (compressionCodec != null) {
            CompressionCodec codec = new CompressionCodecFactory(CONFIGURATION).getCodecByName(compressionCodec);
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.toString());
        }

        RecordWriter recordWriter = createRecordWriter(new Path(filePath), CONFIGURATION);

        try {
            SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(
                    testColumns.stream()
                            .map(TestColumn::getName)
                            .collect(toImmutableList()),
                    testColumns.stream()
                            .map(TestColumn::getObjectInspector)
                            .collect(toImmutableList()));

            Object row = objectInspector.create();

            List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

            for (int rowNumber = 0; rowNumber < numRows; rowNumber++) {
                for (int i = 0; i < testColumns.size(); i++) {
                    Object writeValue = testColumns.get(i).getWriteValue();
                    if (writeValue instanceof Slice) {
                        writeValue = ((Slice) writeValue).getBytes();
                    }
                    objectInspector.setStructFieldData(row, fields.get(i), writeValue);
                }

                Writable record = serializer.serialize(row, objectInspector);
                recordWriter.write(record);
                if (rowNumber % stripeRows == stripeRows - 1) {
                    flushStripe(recordWriter);
                }
            }
        }
        finally {
            recordWriter.close(false);
        }

        Path path = new Path(filePath);
        path.getFileSystem(CONFIGURATION).setVerifyChecksum(true);
        File file = new File(filePath);
        return new FileSplit(path, 0, file.length(), new String[0]);
    }

    private static void flushStripe(RecordWriter recordWriter)
    {
        try {
            Field writerField = OrcOutputFormat.class.getClassLoader()
                    .loadClass(ORC_RECORD_WRITER)
                    .getDeclaredField("writer");
            writerField.setAccessible(true);
            Writer writer = (Writer) writerField.get(recordWriter);
            Method flushStripe = WriterImpl.class.getDeclaredMethod("flushStripe");
            flushStripe.setAccessible(true);
            flushStripe.invoke(writer);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static RecordWriter createRecordWriter(Path target, Configuration conf)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader())) {
            WriterOptions options = OrcFile.writerOptions(conf)
                    .memory(new NullMemoryManager())
                    .compress(ZLIB);

            try {
                return WRITER_CONSTRUCTOR.newInstance(target, options);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Constructor<? extends RecordWriter> getOrcWriterConstructor()
    {
        try {
            Constructor<? extends RecordWriter> constructor = OrcOutputFormat.class.getClassLoader()
                    .loadClass(ORC_RECORD_WRITER)
                    .asSubclass(RecordWriter.class)
                    .getDeclaredConstructor(Path.class, WriterOptions.class);
            constructor.setAccessible(true);
            return constructor;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestColumn
    {
        private final String name;
        private final ObjectInspector objectInspector;
        private final Supplier<?> writeValue;
        private final boolean partitionKey;

        public TestColumn(String name, ObjectInspector objectInspector, Supplier<?> writeValue, boolean partitionKey)
        {
            this.name = requireNonNull(name, "name is null");
            this.objectInspector = requireNonNull(objectInspector, "objectInspector is null");
            this.writeValue = writeValue;
            this.partitionKey = partitionKey;
        }

        public String getName()
        {
            return name;
        }

        public String getType()
        {
            return objectInspector.getTypeName();
        }

        public ObjectInspector getObjectInspector()
        {
            return objectInspector;
        }

        public Object getWriteValue()
        {
            return writeValue.get();
        }

        public boolean isPartitionKey()
        {
            return partitionKey;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("TestColumn{");
            sb.append("name='").append(name).append('\'');
            sb.append(", objectInspector=").append(objectInspector);
            sb.append(", partitionKey=").append(partitionKey);
            sb.append('}');
            return sb.toString();
        }
    }

    public static final class GrowingTestColumn
            extends TestColumn
    {
        private final Supplier<String> writeValue;
        private final int step;
        private int counter;
        private int maxSize;

        public GrowingTestColumn(String name, ObjectInspector objectInspector, Supplier<String> writeValue, boolean partitionKey, int step)
        {
            super(name, objectInspector, writeValue, partitionKey);
            this.writeValue = writeValue;
            this.counter = step;
            this.step = step;
        }

        @Override
        public Object getWriteValue()
        {
            StringBuilder builder = new StringBuilder();
            String source = writeValue.get();
            builder.append(source.repeat(Math.max(0, counter / step)));
            counter++;
            if (builder.length() > maxSize) {
                maxSize = builder.length();
            }
            return builder.toString();
        }

        public int getMaxSize()
        {
            return maxSize;
        }
    }
}
