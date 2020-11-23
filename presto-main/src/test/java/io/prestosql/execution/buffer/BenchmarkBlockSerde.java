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
package io.prestosql.execution.buffer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.prestosql.execution.buffer.PagesSerde.PagesSerdeContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.TestingBlockEncodingSerde;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.execution.buffer.PagesSerdeUtil.readPages;
import static io.prestosql.execution.buffer.PagesSerdeUtil.writePages;
import static io.prestosql.plugin.tpch.TpchTables.getTablePages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkBlockSerde.ROWS)
public class BenchmarkBlockSerde
{
    private static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(30, 5);

    public static final int ROWS = 10_000_000;
    private static final int MAX_STRING = 19;

    @Benchmark
    public Object serializeLongDecimal(LongDecimalBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLongDecimal(LongDecimalBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeInt96(LongTimestampBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeInt96(LongTimestampBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeLong(BigintBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLong(BigintBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeInteger(IntegerBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeInteger(IntegerBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeShort(SmallintBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeShort(SmallintBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeByte(TinyintBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeByte(TinyintBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeSliceDirect(VarcharDirectBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeSliceDirect(VarcharDirectBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeLineitem(LineitemBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLineitem(LineitemBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    private static List<SerializedPage> serializePages(BenchmarkData data)
    {
        PagesSerdeContext context = new PagesSerdeContext();
        return data.getPages().stream()
                .map(page -> data.getPagesSerde().serialize(context, page))
                .collect(toImmutableList());
    }

    public abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private Slice dataSource;
        private PagesSerde pagesSerde;
        private List<Page> pages;

        public void setup(Iterator<Page> pagesIterator)
                throws Exception
        {
            pagesSerde = new PagesSerdeFactory(new TestingBlockEncodingSerde(), false).createPagesSerde();

            this.pages = ImmutableList.copyOf(pagesIterator);
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0);
            writePages(pagesSerde, new OutputStreamSliceOutput(sliceOutput), pages.listIterator());
            dataSource = sliceOutput.slice();
        }

        public void setup(Type type, Iterator<?> values)
                throws Exception
        {
            pagesSerde = new PagesSerdeFactory(new TestingBlockEncodingSerde(), false).createPagesSerde();
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builder();
            while (values.hasNext()) {
                Object value = values.next();
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else if (BIGINT.equals(type)) {
                    BIGINT.writeLong(blockBuilder, ((Number) value).longValue());
                }
                else if (Decimals.isLongDecimal(type)) {
                    type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) value).toBigDecimal().unscaledValue()));
                }
                else if (type instanceof VarcharType) {
                    Slice slice = truncateToLength(utf8Slice((String) value), type);
                    type.writeSlice(blockBuilder, slice);
                }
                else if (TIMESTAMP_PICOS.equals(type)) {
                    TIMESTAMP_PICOS.writeObject(blockBuilder, value);
                }
                else if (INTEGER.equals(type)) {
                    blockBuilder.writeInt((int) value);
                }
                else if (SMALLINT.equals(type)) {
                    blockBuilder.writeShort((short) value);
                }
                else if (TINYINT.equals(type)) {
                    blockBuilder.writeByte((byte) value);
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
                pageBuilder.declarePosition();
                if (pageBuilder.isFull()) {
                    pagesBuilder.add(pageBuilder.build());
                    pageBuilder.reset();
                    blockBuilder = pageBuilder.getBlockBuilder(0);
                }
            }
            if (pageBuilder.getPositionCount() > 0) {
                pagesBuilder.add(pageBuilder.build());
            }

            pages = pagesBuilder.build();
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0);
            writePages(pagesSerde, new OutputStreamSliceOutput(sliceOutput), pages.iterator());
            dataSource = sliceOutput.slice();
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public PagesSerde getPagesSerde()
        {
            return pagesSerde;
        }

        public Slice getDataSource()
        {
            return dataSource;
        }
    }

    public abstract static class TypeBenchmarkData<T>
            extends BenchmarkData
    {
        private final Type type;
        private final Function<Random, T> valueGenerator;

        @Param({"0", ".01", ".10", ".50", ".90", ".99"})
        private double nullChance;

        public TypeBenchmarkData(Type type, Function<Random, T> valueGenerator)
        {
            this.type = requireNonNull(type, "type is null");
            this.valueGenerator = requireNonNull(valueGenerator, "valueGenerator is null");
        }

        protected double getNullChance()
        {
            return nullChance;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(type, createValues());
        }

        private Iterator<T> createValues()
        {
            List<T> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextDouble() > getNullChance()) {
                    values.add(valueGenerator.apply(random));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class LongDecimalBenchmarkData
            extends TypeBenchmarkData<SqlDecimal>
    {
        public LongDecimalBenchmarkData()
        {
            super(LONG_DECIMAL_TYPE, BenchmarkBlockSerde::randomLongDecimal);
        }
    }

    @State(Thread)
    public static class LongTimestampBenchmarkData
            extends TypeBenchmarkData<LongTimestamp>
    {
        public LongTimestampBenchmarkData()
        {
            super(TIMESTAMP_PICOS, BenchmarkBlockSerde::randomTimestamp);
        }
    }

    @State(Thread)
    public static class BigintBenchmarkData
            extends TypeBenchmarkData<Long>
    {
        public BigintBenchmarkData()
        {
            super(BIGINT, Random::nextLong);
        }
    }

    @State(Thread)
    public static class IntegerBenchmarkData
            extends TypeBenchmarkData<Integer>
    {
        public IntegerBenchmarkData()
        {
            super(INTEGER, Random::nextInt);
        }
    }

    @State(Thread)
    public static class SmallintBenchmarkData
            extends TypeBenchmarkData<Short>
    {
        public SmallintBenchmarkData()
        {
            super(SMALLINT, random -> (short) random.nextInt(0xFFFF));
        }
    }

    @State(Thread)
    public static class TinyintBenchmarkData
            extends TypeBenchmarkData<Byte>
    {
        public TinyintBenchmarkData()
        {
            super(TINYINT, random -> (byte) random.nextInt(0xFF));
        }
    }

    @State(Thread)
    public static class VarcharDirectBenchmarkData
            extends TypeBenchmarkData<String>
    {
        public VarcharDirectBenchmarkData()
        {
            super(VARCHAR, BenchmarkBlockSerde::randomAsciiString);
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
            setup(getTablePages("lineitem", 0.1));
        }
    }

    private static String randomAsciiString(Random random)
    {
        char[] value = new char[random.nextInt(MAX_STRING)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    private static SqlDecimal randomLongDecimal(Random random)
    {
        return new SqlDecimal(new BigInteger(96, random), 30, 5);
    }

    private static LongTimestamp randomTimestamp(Random random)
    {
        return new LongTimestamp(random.nextLong(), random.nextInt(PICOSECONDS_PER_MICROSECOND));
    }

    @Test
    public void test()
            throws Exception
    {
        LineitemBenchmarkData data = new LineitemBenchmarkData();
        data.setup();
        deserializeLineitem(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBlockSerde.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
