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
package io.trino.plugin.hive.benchmark;

import com.google.common.collect.ImmutableList;
import io.trino.hadoop.HadoopNative;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormat.TRINO_ORC;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

/**
 * Benchmarks the read operations with projection pushdown in Hive. This is useful for comparing the following for different formats
 * - Performance difference between reading row columns v/s reading projected VARCHAR subfields
 * - Performance difference between reading base VARCHAR columns v/s reading projected VARCHAR subfields
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 50)
@Warmup(iterations = 20)
@Fork(3)
public class BenchmarkProjectionPushdownHive
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final String LETTERS = "abcdefghijklmnopqrstuvwxyz";
    private static final int TOTAL_ROW_COUNT = 10_000;
    private static final int POSITIONS_PER_PAGE = 1_000;
    private static final int DEFAULT_ARRAY_SIZE = 30;

    // Write Strategies
    private static final String TOP_LEVEL = "toplevel";
    private static final String STRUCT = "struct";

    // Read Strategies
    private static final String WITH_PUSHDOWN = "with_pushdown";
    private static final String WITHOUT_PUSHDOWN = "without_pushdown";

    // Types
    private static final String ROW_OF_STRINGS = "ROW(f0 VARCHAR, f1 VARCHAR, f2 VARCHAR)";
    private static final String NESTED_STRUCT = "ROW(" +
            "f0 VARCHAR, " +
            "f1 VARCHAR, " +
            "f2 VARCHAR, " +
            "f3 ARRAY(ROW(f0 VARCHAR, f1 VARCHAR, f2 VARCHAR)))";

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        private final Random random = new Random();

        private List<Type> columnTypesToWrite;
        private List<String> columnNamesToWrite;
        private List<HiveType> columnHiveTypesToWrite;

        // Layout of the file columns: either in a single struct OR as "flattened" into top-level columns
        @Param({STRUCT, TOP_LEVEL})
        private String writeStrategy = STRUCT;

        @Param({WITHOUT_PUSHDOWN, WITH_PUSHDOWN})
        private String readStrategy = WITH_PUSHDOWN;

        // This should be a row typed column
        @Param({ROW_OF_STRINGS, NESTED_STRUCT})
        private String columnTypeString = ROW_OF_STRINGS;

        @Param("1")
        private int readColumnCount = 1;

        @Param({"TRINO_ORC", "TRINO_PARQUET"})
        private BenchmarkFileFormat benchmarkFileFormat = TRINO_ORC;

        private FileFormat fileFormat;
        private TestData dataToWrite;
        private File dataFile;

        private final File targetDir = createTempDir("trino-benchmark");

        @Setup
        public void setup()
                throws IOException
        {
            fileFormat = benchmarkFileFormat.getFormat();
            Type columnType = TESTING_TYPE_MANAGER.fromSqlType(columnTypeString);
            checkState(columnType instanceof RowType, "expected column to have RowType");

            if (STRUCT.equals(writeStrategy)) {
                columnTypesToWrite = ImmutableList.of(columnType);
                columnHiveTypesToWrite = ImmutableList.of(toHiveType(columnType));
                columnNamesToWrite = ImmutableList.of("column_0");
            }
            else if (TOP_LEVEL.equals(writeStrategy)) {
                List<Type> fieldTypes = ((RowType) columnType).getTypeParameters();
                columnTypesToWrite = ImmutableList.copyOf(fieldTypes);
                columnHiveTypesToWrite = columnTypesToWrite.stream()
                        .map(HiveType::toHiveType)
                        .collect(toImmutableList());
                columnNamesToWrite = IntStream.range(0, columnTypesToWrite.size())
                        .mapToObj(Integer::toString)
                        .map("column_"::concat)
                        .collect(toImmutableList());
            }
            else {
                throw new UnsupportedOperationException(format("Write strategy %s not supported", writeStrategy));
            }

            checkState(columnTypesToWrite.stream().allMatch(BenchmarkProjectionPushdownHive::isSupportedType), "Type not supported for benchmark");
            dataToWrite = createTestData(columnTypesToWrite, columnNamesToWrite);

            targetDir.mkdirs();
            dataFile = new File(targetDir, UUID.randomUUID().toString());
            writeData(dataFile);
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(targetDir.toPath(), ALLOW_INSECURE);
        }

        private void writeData(File targetFile)
                throws IOException
        {
            List<Page> inputPages = dataToWrite.getPages();
            try (FormatWriter formatWriter = fileFormat.createFileFormatWriter(
                    SESSION,
                    targetFile,
                    dataToWrite.getColumnNames(),
                    dataToWrite.getColumnTypes(),
                    HiveCompressionCodec.ZSTD)) {
                for (Page page : inputPages) {
                    formatWriter.writePage(page);
                }
            }
        }

        private ConnectorPageSource createPageSource()
        {
            if (TOP_LEVEL.equals(writeStrategy)) {
                List<ColumnHandle> readColumns = IntStream.range(0, readColumnCount).boxed()
                        .map(index -> HiveColumnHandle.createBaseColumn(
                                columnNamesToWrite.get(index),
                                0,
                                columnHiveTypesToWrite.get(index),
                                columnTypesToWrite.get(index),
                                HiveColumnHandle.ColumnType.REGULAR,
                                Optional.empty()))
                        .collect(toImmutableList());

                return fileFormat.createGenericReader(SESSION, HDFS_ENVIRONMENT, dataFile, readColumns, columnNamesToWrite, columnTypesToWrite);
            }

            if (STRUCT.equals(writeStrategy)) {
                // Unflattened schema has one ROW type column
                checkState(columnTypesToWrite.size() == 1);

                HiveColumnHandle baseColumn = HiveColumnHandle.createBaseColumn(
                        columnNamesToWrite.get(0),
                        0,
                        columnHiveTypesToWrite.get(0),
                        columnTypesToWrite.get(0),
                        HiveColumnHandle.ColumnType.REGULAR,
                        Optional.empty());

                List<ColumnHandle> readColumnHandles;
                if (WITH_PUSHDOWN.equals(readStrategy)) {
                    readColumnHandles = IntStream.range(0, readColumnCount).boxed()
                            .map(i -> createProjectedColumnHandle(baseColumn, ImmutableList.of(i)))
                            .collect(toImmutableList());
                }
                else if (WITHOUT_PUSHDOWN.equals(readStrategy)) {
                    readColumnHandles = ImmutableList.of(baseColumn);
                }
                else {
                    throw new UnsupportedOperationException(format("Read strategy %s not supported", readStrategy));
                }

                return fileFormat.createGenericReader(SESSION, HDFS_ENVIRONMENT, dataFile, readColumnHandles, columnNamesToWrite, columnTypesToWrite);
            }

            throw new UnsupportedOperationException(format("Write strategy %s not supported", writeStrategy));
        }

        private TestData createTestData(List<Type> columnTypes, List<String> columnNames)
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            int pageCount = TOTAL_ROW_COUNT / POSITIONS_PER_PAGE;

            for (int i = 0; i < pageCount; i++) {
                Block[] blocks = new Block[columnTypes.size()];

                for (int column = 0; column < columnTypes.size(); column++) {
                    Type type = columnTypes.get(column);
                    blocks[column] = createBlock(type, POSITIONS_PER_PAGE);
                }

                pages.add(new Page(blocks));
            }

            return new TestData(columnNames, columnTypes, pages.build());
        }

        private Block createBlock(Type type, int rowCount)
        {
            checkState(isSupportedType(type), format("Type %s not supported", type.getDisplayName()));

            if (type instanceof RowType) {
                List<Type> parameters = type.getTypeParameters();
                Block[] fieldBlocks = new Block[parameters.size()];

                for (int field = 0; field < parameters.size(); field++) {
                    fieldBlocks[field] = createBlock(parameters.get(field), rowCount);
                }

                return RowBlock.fromFieldBlocks(rowCount, Optional.empty(), fieldBlocks);
            }
            if (type instanceof VarcharType) {
                BlockBuilder builder = VARCHAR.createBlockBuilder(null, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    VARCHAR.writeString(builder, generateRandomString(random, 500));
                }
                return builder.build();
            }
            if (type instanceof ArrayType arrayType) {
                Type elementType = arrayType.getElementType();

                BlockBuilder blockBuilder = type.createBlockBuilder(null, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    Block elementBlock = createBlock(elementType, DEFAULT_ARRAY_SIZE);
                    type.writeObject(blockBuilder, elementBlock);
                }

                return blockBuilder.build();
            }

            throw new UnsupportedOperationException("Only VARCHAR, ROW and ARRAY types supported");
        }
    }

    @Benchmark
    public List<Page> readPages(BenchmarkContext context)
    {
        List<Page> pages = new ArrayList<>(100);
        ConnectorPageSource pageSource = context.createPageSource();

        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page != null) {
                pages.add(page.getLoadedPage());
            }
        }

        return pages;
    }

    @Test
    public void testBenchmark()
            throws IOException
    {
        BenchmarkContext context = new BenchmarkContext();
        try {
            context.setup();
            readPages(context);
        }
        catch (Throwable t) {
            throw new RuntimeException("Benchmark execution failed", t);
        }
        finally {
            context.tearDown();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkProjectionPushdownHive.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }

    @SuppressWarnings("SameParameterValue")
    private static File createTempDir(String prefix)
    {
        try {
            return createTempDirectory(prefix).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String generateRandomString(Random random, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = LETTERS.charAt(random.nextInt(LETTERS.length()));
        }
        return new String(chars);
    }

    private static boolean isSupportedType(Type type)
    {
        if (type == VARCHAR) {
            return true;
        }
        if (type instanceof RowType) {
            return type.getTypeParameters().stream().allMatch(BenchmarkProjectionPushdownHive::isSupportedType);
        }
        if (type instanceof ArrayType) {
            return isSupportedType(((ArrayType) type).getElementType());
        }

        return false;
    }
}
