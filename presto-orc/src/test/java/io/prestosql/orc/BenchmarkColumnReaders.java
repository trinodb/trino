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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.prestosql.execution.buffer.BenchmarkDataGenerator;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.execution.buffer.BenchmarkDataGenerator.createValues;
import static io.prestosql.execution.buffer.BenchmarkDataGenerator.randomAsciiString;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.writeOrcColumnPresto;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.plugin.tpch.TpchTables.getTableColumns;
import static io.prestosql.plugin.tpch.TpchTables.getTablePages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.file.Files.readAllBytes;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkColumnReaders.ROWS)
public class BenchmarkColumnReaders
{
    private static final DecimalType SHORT_DECIMAL_TYPE = createDecimalType(10, 5);
    private static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(30, 5);
    public static final int ROWS = 10_000_000;
    private static final int DICTIONARY = 22;

    @Benchmark
    public Object readBoolean(BooleanBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readByte(TinyIntBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readShortDecimal(ShortDecimalBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readLongDecimal(LongDecimalBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readDoubleNoNull(DoubleBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readFloat(FloatBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readLong(BigintBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readInt(IntegerBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readShort(SmallintBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readSliceDirect(VarcharDirectBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readSliceDictionary(VarcharDictionaryBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readTimestamp(TimestampBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readLineitem(LineitemBenchmarkData data)
            throws Exception
    {
        List<Page> pages = new ArrayList<>();
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            for (Page page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
                pages.add(page.getLoadedPage());
            }
        }
        return pages;
    }

    private Object readFirstColumn(OrcRecordReader recordReader)
            throws IOException
    {
        List<Block> blocks = new ArrayList<>();
        for (Page page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
            blocks.add(page.getBlock(0).getLoadedBlock());
        }
        return blocks;
    }

    public abstract static class TypeBenchmarkData
            extends BenchmarkData
    {
        @Param({"0", ".5", "1"})
        private double nullChance;

        public void setup(Type type, Function<Random, ?> valueGenerator)
                throws Exception
        {
            setup(type, createValues(ROWS, valueGenerator, nullChance));
        }

        public void setup(Type type, Function<Random, ?> valueGenerator, double nullChance)
                throws Exception
        {
            setup(type, createValues(ROWS, valueGenerator, nullChance));
        }
    }

    public abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private List<Type> types;
        private File temporaryDirectory;
        private File orcFile;
        private OrcDataSource dataSource;

        @Param({"NONE", "ZLIB"})
        private String compression = "NONE";

        public void setup(List<Type> types, Iterator<Page> pages)
                throws Exception
        {
            this.types = types;
            temporaryDirectory = createTempDir();
            orcFile = new File(temporaryDirectory, randomUUID().toString());
            OrcTester.writeOrcPages(orcFile, CompressionKind.valueOf(compression), types, pages, new OrcWriterStats());

            dataSource = new MemoryOrcDataSource(new OrcDataSourceId(orcFile.getPath()), Slices.wrappedBuffer(readAllBytes(orcFile.toPath())));
        }

        public void setup(Type type, Iterator<?> values)
                throws Exception
        {
            this.types = ImmutableList.of(type);
            temporaryDirectory = createTempDir();
            orcFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnPresto(orcFile, NONE, type, values, new OrcWriterStats());

            dataSource = new MemoryOrcDataSource(new OrcDataSourceId(orcFile.getPath()), Slices.wrappedBuffer(readAllBytes(orcFile.toPath())));
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcReader orcReader = OrcReader.createOrcReader(dataSource, new OrcReaderOptions())
                    .orElseThrow(() -> new RuntimeException("File is empty"));
            return orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    types,
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    RuntimeException::new);
        }
    }

    @State(Thread)
    public static class BooleanBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BOOLEAN, Random::nextBoolean);
        }
    }

    @State(Thread)
    public static class TinyIntBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TINYINT, BenchmarkDataGenerator::randomByte);
        }
    }

    @State(Thread)
    public static class ShortDecimalBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(SHORT_DECIMAL_TYPE, BenchmarkColumnReaders::randomShortDecimal);
        }
    }

    @State(Thread)
    public static class LongDecimalBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(LONG_DECIMAL_TYPE, BenchmarkDataGenerator::randomLongDecimal);
        }
    }

    @State(Thread)
    public static class DoubleBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(DOUBLE, Random::nextDouble);
        }
    }

    @State(Thread)
    public static class FloatBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(REAL, Random::nextFloat);
        }
    }

    @State(Thread)
    public static class BigintBenchmarkData
            extends TypeBenchmarkData
    {
        public static BigintBenchmarkData create(double nullChance)
                throws Exception
        {
            BigintBenchmarkData data = new BigintBenchmarkData();
            data.setup(BIGINT, Random::nextLong, nullChance);
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, Random::nextLong);
        }
    }

    @State(Thread)
    public static class IntegerBenchmarkData
            extends TypeBenchmarkData
    {
        public static IntegerBenchmarkData create(double nullChance)
                throws Exception
        {
            IntegerBenchmarkData data = new IntegerBenchmarkData();
            data.setup(INTEGER, Random::nextInt, nullChance);
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(INTEGER, Random::nextInt);
        }
    }

    @State(Thread)
    public static class SmallintBenchmarkData
            extends TypeBenchmarkData
    {
        public static SmallintBenchmarkData create(double nullChance)
                throws Exception
        {
            SmallintBenchmarkData data = new SmallintBenchmarkData();
            data.setup(SMALLINT, BenchmarkDataGenerator::randomShort, nullChance);
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(SMALLINT, BenchmarkDataGenerator::randomShort);
        }
    }

    @State(Thread)
    public static class VarcharDirectBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, BenchmarkDataGenerator::randomAsciiString);
        }
    }

    @State(Thread)
    public static class VarcharDictionaryBenchmarkData
            extends TypeBenchmarkData
    {
        private List<String> dictionary;

        @Setup
        public void setup()
                throws Exception
        {
            dictionary = createDictionary(random);
            setup(VARCHAR, random -> dictionary.get(random.nextInt(dictionary.size())));
        }
    }

    private static List<String> createDictionary(Random random)
    {
        List<String> dictionary = new ArrayList<>();
        for (int dictionaryIndex = 0; dictionaryIndex < DICTIONARY; dictionaryIndex++) {
            dictionary.add(randomAsciiString(random));
        }
        return dictionary;
    }

    @State(Thread)
    public static class TimestampBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TIMESTAMP_MILLIS, BenchmarkColumnReaders::randomTimestamp);
        }
    }

    @State(Thread)
    public static class LineitemBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(
                    getTableColumns("lineitem"),
                    getTablePages("lineitem", 0.1));
        }
    }

    private static SqlDecimal randomShortDecimal(Random random)
    {
        return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), 10, 5);
    }

    private static SqlTimestamp randomTimestamp(Random random)
    {
        return SqlTimestamp.fromMillis(3, random.nextLong());
    }

    static {
        try {
            // call all versions of the long stream reader to pollute the profile
            BenchmarkColumnReaders benchmark = new BenchmarkColumnReaders();
            benchmark.readLong(BigintBenchmarkData.create(.5));
            benchmark.readLong(BigintBenchmarkData.create(0));
            benchmark.readInt(IntegerBenchmarkData.create(.5));
            benchmark.readInt(IntegerBenchmarkData.create(0));
            benchmark.readShort(SmallintBenchmarkData.create(.5));
            benchmark.readShort(SmallintBenchmarkData.create(0));
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkColumnReaders.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
