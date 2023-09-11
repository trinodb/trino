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
import io.airlift.slice.Slices;
import io.trino.hadoop.HadoopNative;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.tpch.OrderColumn;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.results.RunResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormatsUtils.MIN_DATA_SIZE;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormatsUtils.createTempDir;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormatsUtils.createTpchDataSet;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormatsUtils.nextRandomBetween;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormatsUtils.printResults;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 50)
@Warmup(iterations = 20)
@Fork(3)
@SuppressWarnings({"UseOfSystemOutOrSystemErr", "ResultOfMethodCallIgnored"})
public class BenchmarkHiveFileFormat
{
    private static final ConnectorSession SESSION = getHiveSession(new HiveConfig());

    static {
        HadoopNative.requireHadoopNative();
    }

    @Param({
            "LINEITEM",
            "BIGINT_SEQUENTIAL",
            "BIGINT_RANDOM",
            "VARCHAR_SMALL",
            "VARCHAR_LARGE",
            "VARCHAR_DICTIONARY",
            "MAP_VARCHAR_DOUBLE",
            "LARGE_MAP_VARCHAR_DOUBLE",
            "MAP_INT_DOUBLE",
            "LARGE_MAP_INT_DOUBLE",
            "LARGE_ARRAY_VARCHAR"})
    private DataSet dataSet;

    @Param({
            "NONE",
            "SNAPPY",
            "GZIP"})
    private HiveCompressionCodec compression;

    @Param({
            "TRINO_RCBINARY",
            "TRINO_RCTEXT",
            "TRINO_ORC",
            "TRINO_PARQUET"})
    private BenchmarkFileFormat benchmarkFileFormat;

    private FileFormat fileFormat;
    private TestData data;
    private File dataFile;

    private final File targetDir = createTempDir("trino-benchmark");

    public BenchmarkHiveFileFormat()
    {
    }

    public BenchmarkHiveFileFormat(DataSet dataSet, HiveCompressionCodec compression, BenchmarkFileFormat fileFormat)
    {
        this.dataSet = dataSet;
        this.compression = compression;
        this.benchmarkFileFormat = fileFormat;
    }

    @Setup
    public void setup()
            throws IOException
    {
        fileFormat = benchmarkFileFormat.getFormat();
        data = dataSet.createTestData(fileFormat);

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

    @SuppressWarnings("PublicField")
    @AuxCounters
    @State(Scope.Thread)
    public static class CompressionCounter
    {
        public long inputSize;
        public long outputSize;
    }

    @Benchmark
    public List<Page> read(CompressionCounter counter)
            throws IOException
    {
        if (!fileFormat.supports(data)) {
            throw new RuntimeException(fileFormat + " does not support data set " + dataSet);
        }
        List<Page> pages = new ArrayList<>(100);
        try (ConnectorPageSource pageSource = fileFormat.createFileFormatReader(
                SESSION,
                HDFS_ENVIRONMENT,
                dataFile,
                data.getColumnNames(),
                data.getColumnTypes())) {
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    pages.add(page.getLoadedPage());
                }
            }
        }
        counter.inputSize += data.getSize();
        counter.outputSize += dataFile.length();
        return pages;
    }

    @Benchmark
    public File write(CompressionCounter counter)
            throws IOException
    {
        File targetFile = new File(targetDir, UUID.randomUUID().toString());
        writeData(targetFile);
        counter.inputSize += data.getSize();
        counter.outputSize += targetFile.length();
        return targetFile;
    }

    private void writeData(File targetFile)
            throws IOException
    {
        List<Page> inputPages = data.getPages();
        try (FormatWriter formatWriter = fileFormat.createFileFormatWriter(
                SESSION,
                targetFile,
                data.getColumnNames(),
                data.getColumnTypes(),
                compression)) {
            for (Page page : inputPages) {
                formatWriter.writePage(page);
            }
        }
    }

    public enum DataSet
    {
        LINEITEM {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, LINE_ITEM, LINE_ITEM.getColumns());
            }
        },
        BIGINT_SEQUENTIAL {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.ORDER_KEY);
            }
        },
        BIGINT_RANDOM {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.CUSTOMER_KEY);
            }
        },
        VARCHAR_SMALL {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.CLERK);
            }
        },
        VARCHAR_LARGE {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.CLERK);
            }
        },
        VARCHAR_DICTIONARY {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.ORDER_PRIORITY);
            }
        },
        MAP_VARCHAR_DOUBLE {
            private static final int MIN_ENTRIES = 1;
            private static final int MAX_ENTRIES = 5;

            @Override
            public TestData createTestData(FileFormat format)
            {
                MapType type = new MapType(VARCHAR, DOUBLE, TESTING_TYPE_MANAGER.getTypeOperators());
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();

                int[] keys = {1, 2, 3, 4, 5};

                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    MapBlockBuilder builder = (MapBlockBuilder) pageBuilder.getBlockBuilder(0);
                    builder.buildEntry((keyBuilder, valueBuilder) -> {
                        int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                        IntArrays.shuffle(keys, random);
                        for (int entryId = 0; entryId < entries; entryId++) {
                            VARCHAR.writeSlice(keyBuilder, Slices.utf8Slice("key" + keys[entryId]));
                            DOUBLE.writeDouble(valueBuilder, random.nextDouble());
                        }
                    });

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        LARGE_MAP_VARCHAR_DOUBLE {
            private static final int MIN_ENTRIES = 5_000;
            private static final int MAX_ENTRIES = 15_000;

            @Override
            public TestData createTestData(FileFormat format)
            {
                MapType type = new MapType(VARCHAR, DOUBLE, TESTING_TYPE_MANAGER.getTypeOperators());
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();
                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    MapBlockBuilder builder = (MapBlockBuilder) pageBuilder.getBlockBuilder(0);
                    builder.buildEntry((keyBuilder, valueBuilder) -> {
                        int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                        for (int entryId = 0; entryId < entries; entryId++) {
                            VARCHAR.writeSlice(keyBuilder, Slices.utf8Slice("key" + random.nextInt(10_000_000)));
                            DOUBLE.writeDouble(valueBuilder, random.nextDouble());
                        }
                    });

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        MAP_INT_DOUBLE {
            private static final int MIN_ENTRIES = 1;
            private static final int MAX_ENTRIES = 5;

            @Override
            public TestData createTestData(FileFormat format)
            {
                MapType type = new MapType(INTEGER, DOUBLE, TESTING_TYPE_MANAGER.getTypeOperators());
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();

                int[] keys = {1, 2, 3, 4, 5};

                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    MapBlockBuilder builder = (MapBlockBuilder) pageBuilder.getBlockBuilder(0);
                    builder.buildEntry((keyBuilder, valueBuilder) -> {
                        int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                        IntArrays.shuffle(keys, random);
                        for (int entryId = 0; entryId < entries; entryId++) {
                            INTEGER.writeLong(keyBuilder, keys[entryId]);
                            DOUBLE.writeDouble(valueBuilder, random.nextDouble());
                        }
                    });

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        LARGE_MAP_INT_DOUBLE {
            private static final int MIN_ENTRIES = 5_000;
            private static final int MAX_ENTRIES = 15_0000;

            @Override
            public TestData createTestData(FileFormat format)
            {
                MapType type = new MapType(INTEGER, DOUBLE, TESTING_TYPE_MANAGER.getTypeOperators());
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();
                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    MapBlockBuilder builder = (MapBlockBuilder) pageBuilder.getBlockBuilder(0);
                    builder.buildEntry((keyBuilder, valueBuilder) -> {
                        int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                        for (int entryId = 0; entryId < entries; entryId++) {
                            INTEGER.writeLong(keyBuilder, random.nextInt(10_000_000));
                            DOUBLE.writeDouble(valueBuilder, random.nextDouble());
                        }
                    });

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        LARGE_ARRAY_VARCHAR {
            private static final int MIN_ENTRIES = 5_000;
            private static final int MAX_ENTRIES = 15_0000;

            @Override
            public TestData createTestData(FileFormat format)
            {
                Type type = new ArrayType(createUnboundedVarcharType());
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();
                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                    ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
                        int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                        for (int entryId = 0; entryId < entries; entryId++) {
                            createUnboundedVarcharType().writeSlice(elementBuilder, Slices.utf8Slice("key" + random.nextInt(10_000_000)));
                        }
                    });

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        };

        public abstract TestData createTestData(FileFormat format);
    }

    public static void main(String[] args)
            throws Exception
    {
        Collection<RunResult> results = benchmark(BenchmarkHiveFileFormat.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();

        printResults(results);
    }
}
