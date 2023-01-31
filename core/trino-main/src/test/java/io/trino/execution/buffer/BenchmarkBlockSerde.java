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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
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
import org.testng.annotations.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.execution.buffer.BenchmarkDataGenerator.createValues;
import static io.trino.execution.buffer.PagesSerdeUtil.readPages;
import static io.trino.execution.buffer.PagesSerdeUtil.writePages;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.tpch.TpchTables.getTablePages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.Varchars.truncateToLength;
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

    @Benchmark
    public Object serializeLongDecimal(LongDecimalBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLongDecimal(LongDecimalBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeInt96(LongTimestampBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeInt96(LongTimestampBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeLong(BigintBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLong(BigintBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeInteger(IntegerBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeInteger(IntegerBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeShort(SmallintBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeShort(SmallintBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeByte(TinyintBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeByte(TinyintBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeSliceDirect(VarcharDirectBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeSliceDirect(VarcharDirectBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeLineitem(LineitemBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeLineitem(LineitemBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    @Benchmark
    public Object serializeRow(RowTypeBenchmarkData data)
    {
        return serializePages(data);
    }

    @Benchmark
    public Object deserializeRow(RowTypeBenchmarkData data)
    {
        return ImmutableList.copyOf(readPages(data.getDeserializer(), new BasicSliceInput(data.getDataSource())));
    }

    private static List<Slice> serializePages(BenchmarkData data)
    {
        return data.getPages().stream()
                .map(page -> data.getSerializer().serialize(page))
                .collect(toImmutableList());
    }

    public abstract static class TypeBenchmarkData
            extends BenchmarkData
    {
        @Param({"0", ".01", ".10", ".50", ".90", ".99"})
        private double nullChance;

        public void setup(Type type, Function<Random, ?> valueGenerator)
        {
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new TestingBlockEncodingSerde(), false);
            PageSerializer serializer = serdeFactory.createSerializer(Optional.empty());
            PageDeserializer deserializer = serdeFactory.createDeserializer(Optional.empty());
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builder();

            Iterator<?> values = createValues(ROWS, valueGenerator, nullChance);
            while (values.hasNext()) {
                writeValue(type, values.next(), blockBuilder);
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

            List<Page> pages = pagesBuilder.build();
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0);
            writePages(serializer, new OutputStreamSliceOutput(sliceOutput), pages.iterator());

            setup(sliceOutput.slice(), serializer, deserializer, pages);
        }

        private void writeValue(Type type, Object value, BlockBuilder blockBuilder)
        {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else if (BIGINT.equals(type)) {
                BIGINT.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (type instanceof DecimalType decimalType && !decimalType.isShort()) {
                type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).toBigDecimal().unscaledValue()));
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
            else if (type instanceof RowType) {
                BlockBuilder row = blockBuilder.beginBlockEntry();
                List<?> values = (List<?>) value;
                if (values.size() != type.getTypeParameters().size()) {
                    throw new IllegalArgumentException("Size of types and values must have the same size");
                }
                List<SimpleEntry<Type, Object>> pairs = new ArrayList<>();
                for (int i = 0; i < type.getTypeParameters().size(); i++) {
                    pairs.add(new SimpleEntry<>(type.getTypeParameters().get(i), ((List<?>) value).get(i)));
                }
                pairs.forEach(p -> writeValue(p.getKey(), p.getValue(), row));
                blockBuilder.closeEntry();
            }
            else {
                throw new IllegalArgumentException("Unsupported type " + type);
            }
        }
    }

    public abstract static class BenchmarkData
    {
        private Slice dataSource;
        private PageSerializer serializer;
        private PageDeserializer deserializer;
        private List<Page> pages;

        public void setup(Slice dataSource, PageSerializer serializer, PageDeserializer deserializer, List<Page> pages)
        {
            this.dataSource = dataSource;
            this.serializer = serializer;
            this.deserializer = deserializer;
            this.pages = pages;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public PageSerializer getSerializer()
        {
            return serializer;
        }

        public PageDeserializer getDeserializer()
        {
            return deserializer;
        }

        public Slice getDataSource()
        {
            return dataSource;
        }
    }

    @State(Thread)
    public static class LongDecimalBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            super.setup(LONG_DECIMAL_TYPE, BenchmarkDataGenerator::randomLongDecimal);
        }
    }

    @State(Thread)
    public static class LongTimestampBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            super.setup(TIMESTAMP_PICOS, BenchmarkDataGenerator::randomTimestamp);
        }
    }

    @State(Thread)
    public static class BigintBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            super.setup(BIGINT, Random::nextLong);
        }
    }

    @State(Thread)
    public static class IntegerBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            super.setup(INTEGER, Random::nextInt);
        }
    }

    @State(Thread)
    public static class SmallintBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            super.setup(SMALLINT, BenchmarkDataGenerator::randomShort);
        }
    }

    @State(Thread)
    public static class TinyintBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            super.setup(TINYINT, BenchmarkDataGenerator::randomByte);
        }
    }

    @State(Thread)
    public static class VarcharDirectBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            super.setup(VARCHAR, BenchmarkDataGenerator::randomAsciiString);
        }
    }

    @State(Thread)
    public static class LineitemBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
        {
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new TestingBlockEncodingSerde(), false);
            PageSerializer serializer = serdeFactory.createSerializer(Optional.empty());
            PageDeserializer deserializer = serdeFactory.createDeserializer(Optional.empty());

            List<Page> pages = ImmutableList.copyOf(getTablePages("lineitem", 0.1, DecimalTypeMapping.DOUBLE));
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0);
            writePages(serializer, new OutputStreamSliceOutput(sliceOutput), pages.listIterator());
            setup(sliceOutput.slice(), serializer, deserializer, pages);
        }
    }

    @State(Thread)
    public static class RowTypeBenchmarkData
            extends TypeBenchmarkData
    {
        @Setup
        public void setup()
        {
            RowType type = RowType.anonymous(ImmutableList.of(BIGINT));
            super.setup(type, (random -> BenchmarkDataGenerator.randomRow(type.getTypeParameters(), random)));
        }
    }

    @Test
    public void test()
    {
        LineitemBenchmarkData data = new LineitemBenchmarkData();
        data.setup();
        deserializeLineitem(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkBlockSerde.class).run();
    }
}
