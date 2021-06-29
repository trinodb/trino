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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.orc.metadata.CompressionKind;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcTester.writeOrcColumnTrino;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.plugin.tpch.TpchTables.getTableColumns;
import static io.trino.plugin.tpch.TpchTables.getTablePages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
    private static final int MAX_STRING = 19;
    private static final Collection<?> NULL_VALUES = Collections.nCopies(ROWS, null);

    @Benchmark
    public Object readBooleanNoNull(BooleanNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readBooleanWithNull(BooleanWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readAllNull(AllNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readByteNoNull(TinyIntNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readByteWithNull(TinyIntWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readShortDecimalNoNull(ShortDecimalNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readShortDecimalWithNull(ShortDecimalWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readLongDecimalNoNull(LongDecimalNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readLongDecimalWithNull(LongDecimalWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readDoubleNoNull(DoubleNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readDoubleWithNull(DoubleWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readFloatNoNull(FloatNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readFloatWithNull(FloatWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readLongNoNull(BigintNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readLongWithNull(BigintWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readIntNoNull(IntegerNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readIntWithNull(IntegerWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readShortNoNull(SmallintNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readShortWithNull(SmallintWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readSliceDirectNoNull(VarcharDirectNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readSliceDirectWithNull(VarcharDirectWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readSliceDictionaryNoNull(VarcharDictionaryNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readSliceDictionaryWithNull(VarcharDictionaryWithNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readTimestampNoNull(TimestampNoNullBenchmarkData data)
            throws Exception
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            return readFirstColumn(recordReader);
        }
    }

    @Benchmark
    public Object readTimestampWithNull(TimestampWithNullBenchmarkData data)
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
            writeOrcColumnTrino(orcFile, NONE, type, values, new OrcWriterStats());

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
    public static class AllNullBenchmarkData
            extends BenchmarkData
    {
        @SuppressWarnings("unused")
        @Param({
                "boolean",

                "tinyint",
                "integer",
                "bigint",
                "decimal(10,5)",

                "timestamp",

                "real",
                "double",

                "varchar",
                "varbinary",
        })
        private String typeName;

        @Setup
        public void setup()
                throws Exception
        {
            Type type = createTestMetadataManager().fromSqlType(typeName);
            setup(type, createValues());
        }

        private Iterator<?> createValues()
        {
            return NULL_VALUES.iterator();
        }
    }

    @State(Thread)
    public static class BooleanNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BOOLEAN, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class BooleanWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BOOLEAN, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean() ? random.nextBoolean() : null);
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class TinyIntNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TINYINT, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Long.valueOf(random.nextLong()).byteValue());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class TinyIntWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TINYINT, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Long.valueOf(random.nextLong()).byteValue());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class ShortDecimalNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(SHORT_DECIMAL_TYPE, createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), 10, 5));
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class ShortDecimalWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(SHORT_DECIMAL_TYPE, createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10000000000L), 10, 5));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class LongDecimalNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(LONG_DECIMAL_TYPE, createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(new BigInteger(96, random), 30, 5));
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class LongDecimalWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(LONG_DECIMAL_TYPE, createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(new BigInteger(96, random), 30, 5));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class DoubleNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(DOUBLE, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextDouble());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class DoubleWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(DOUBLE, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextDouble());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class FloatNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(REAL, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Float> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextFloat());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class FloatWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(REAL, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Float> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextFloat());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class BigintNoNullBenchmarkData
            extends BenchmarkData
    {
        public static BigintNoNullBenchmarkData create()
                throws Exception
        {
            BigintNoNullBenchmarkData data = new BigintNoNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextLong());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class BigintWithNullBenchmarkData
            extends BenchmarkData
    {
        public static BigintWithNullBenchmarkData create()
                throws Exception
        {
            BigintWithNullBenchmarkData data = new BigintWithNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextLong());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class IntegerNoNullBenchmarkData
            extends BenchmarkData
    {
        public static IntegerNoNullBenchmarkData create()
                throws Exception
        {
            IntegerNoNullBenchmarkData data = new IntegerNoNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(INTEGER, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextInt());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class IntegerWithNullBenchmarkData
            extends BenchmarkData
    {
        public static IntegerWithNullBenchmarkData create()
                throws Exception
        {
            IntegerWithNullBenchmarkData data = new IntegerWithNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(INTEGER, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextInt());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class SmallintNoNullBenchmarkData
            extends BenchmarkData
    {
        public static SmallintNoNullBenchmarkData create()
                throws Exception
        {
            SmallintNoNullBenchmarkData data = new SmallintNoNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(SMALLINT, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Short> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add((short) random.nextInt());
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class SmallintWithNullBenchmarkData
            extends BenchmarkData
    {
        public static SmallintWithNullBenchmarkData create()
                throws Exception
        {
            SmallintWithNullBenchmarkData data = new SmallintWithNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(SMALLINT, createValues());
        }

        private Iterator<?> createValues()
        {
            List<Short> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add((short) random.nextInt());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class VarcharDirectNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, createValues());
        }

        private Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(randomAsciiString(random));
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class VarcharDirectWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, createValues());
        }

        private Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(randomAsciiString(random));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class VarcharDictionaryNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, createValues());
        }

        private Iterator<?> createValues()
        {
            List<String> dictionary = createDictionary(random);

            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(dictionary.get(random.nextInt(dictionary.size())));
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class VarcharDictionaryWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, createValues());
        }

        private Iterator<?> createValues()
        {
            List<String> dictionary = createDictionary(random);

            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(dictionary.get(random.nextInt(dictionary.size())));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
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

    // this is not appropriate for benchmarking with compression
    private static String randomAsciiString(Random random)
    {
        char[] value = new char[random.nextInt(MAX_STRING)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    @State(Thread)
    public static class TimestampNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TIMESTAMP_MILLIS, createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlTimestamp> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(SqlTimestamp.fromMillis(3, (random.nextLong())));
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class TimestampWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TIMESTAMP_MILLIS, createValues());
        }

        private Iterator<?> createValues()
        {
            List<SqlTimestamp> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(SqlTimestamp.fromMillis(3, random.nextLong()));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
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

    static {
        try {
            // call all versions of the long stream reader to pollute the profile
            BenchmarkColumnReaders benchmark = new BenchmarkColumnReaders();
            benchmark.readLongNoNull(BigintNoNullBenchmarkData.create());
            benchmark.readLongWithNull(BigintWithNullBenchmarkData.create());
            benchmark.readIntNoNull(IntegerNoNullBenchmarkData.create());
            benchmark.readIntWithNull(IntegerWithNullBenchmarkData.create());
            benchmark.readShortNoNull(SmallintNoNullBenchmarkData.create());
            benchmark.readShortWithNull(SmallintWithNullBenchmarkData.create());
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkColumnReaders.class).run();
    }
}
