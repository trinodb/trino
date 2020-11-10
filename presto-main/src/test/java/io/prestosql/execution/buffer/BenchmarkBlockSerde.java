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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.execution.buffer.PagesSerdeUtil.readPages;
import static io.prestosql.execution.buffer.PagesSerdeUtil.writePages;
import static io.prestosql.plugin.tpch.TpchTables.getTablePages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.truncateToLength;
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
    public Object serializeLongDecimalNoNull(LongDecimalNoNullBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLongDecimalNoNull(LongDecimalNoNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeLongDecimalWithNull(LongDecimalWithNullBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLongDecimalWithNull(LongDecimalWithNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeLongNoNull(BigintNoNullBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLongNoNull(BigintNoNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeLongWithNull(BigintWithNullBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLongWithNull(BigintWithNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeSliceDirectNoNull(VarcharDirectNoNullBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeSliceDirectNoNull(VarcharDirectNoNullBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getPagesSerde(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeSliceDirectWithNull(VarcharDirectWithNullBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeSliceDirectWithNull(VarcharDirectWithNullBenchmarkData data)
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
            pagesSerde = new TestingPagesSerdeFactory(new TestingBlockEncodingSerde(), false).createPagesSerde();

            this.pages = ImmutableList.copyOf(pagesIterator);
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0);
            writePages(pagesSerde, new OutputStreamSliceOutput(sliceOutput), pages.listIterator());
            dataSource = sliceOutput.slice();
        }

        public void setup(Type type, Iterator<?> values)
                throws Exception
        {
            pagesSerde = new TestingPagesSerdeFactory(new TestingBlockEncodingSerde(), false).createPagesSerde();
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
    public static class BigintNoNullBenchmarkData
            extends BenchmarkData
    {
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
