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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.parquet.writer.valuewriter.PrimitiveValueWriter;
import io.trino.parquet.writer.valuewriter.TrinoValuesWriterFactory;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.bloomfilter.AdaptiveBlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.schema.PrimitiveType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.parquet.writer.ParquetWriters.getValueWriter;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Fork(2)
public abstract class AbstractColumnWriterBenchmark
{
    @Param
    public BloomFilterType bloomFilterType;

    @Param({
            "1", "1048576", // 1MB is default page size
    })
    public int maxDictionaryPageSize;

    public enum BloomFilterType
    {
        NONE {
            @Override
            Optional<BloomFilter> getBloomFilter(ColumnDescriptor columnDescriptor)
            {
                return Optional.empty();
            }
        },
        DEFAULT_BLOOM_FILTER {
            @Override
            Optional<BloomFilter> getBloomFilter(ColumnDescriptor columnDescriptor)
            {
                return Optional.of(new AdaptiveBlockSplitBloomFilter(1048576, 5, 0.05, columnDescriptor));
            }
        },
        /**/;

        abstract Optional<BloomFilter> getBloomFilter(ColumnDescriptor columnDescriptor);
    }

    private static final DataSize MAX_PAGE_SIZE = DataSize.of(1, MEGABYTE);
    private static final int DATA_GENERATION_BATCH_SIZE = 8192;
    private static final int BLOCK_COUNT = 32;

    private List<Block> blocks;

    protected abstract Type getTrinoType();

    protected abstract PrimitiveType getParquetType();

    protected abstract Block generateBlock(int size);

    private PrimitiveValueWriter createValuesWriter()
    {
        TrinoValuesWriterFactory valuesWriterFactory = new TrinoValuesWriterFactory(
                ParquetWriterOptions.builder()
                        .setMaxPageSize(MAX_PAGE_SIZE)
                        .setUseDeltaLengthByteArrayEncoding(false)
                        .build(),
                maxDictionaryPageSize);
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"test"}, getParquetType(), 0, 0);
        return getValueWriter(valuesWriterFactory.newValuesWriter(columnDescriptor, bloomFilterType.getBloomFilter(columnDescriptor)), getTrinoType(), columnDescriptor.getPrimitiveType(), Optional.empty());
    }

    @Setup
    public void setup()
    {
        this.blocks = IntStream.range(0, BLOCK_COUNT).boxed()
                .map(_ -> generateBlock(DATA_GENERATION_BATCH_SIZE))
                .collect(toImmutableList());
    }

    @Benchmark
    public List<BytesInput> write()
    {
        ImmutableList.Builder<BytesInput> output = ImmutableList.builder();
        try (PrimitiveValueWriter writer = createValuesWriter()) {
            for (Block block : blocks) {
                writer.write(block);
                if (writer.getEstimatedBufferedSize() >= MAX_PAGE_SIZE.toBytes()) {
                    output.add(flushPage(writer));
                }
            }
            output.add(flushPage(writer));
            DictionaryPage dictionaryPage = writer.toDictPageAndClose();
            if (dictionaryPage != null) {
                output.add(dictionaryPage.getBytes());
            }
        }
        return output.build();
    }

    private static BytesInput flushPage(PrimitiveValueWriter writer)
    {
        BytesInput pageBytes = writer.getBytes();
        // getEncoding records whether the page used the dictionary and has to be called between getBytes and reset
        writer.getEncoding();
        writer.reset();
        return pageBytes;
    }

    protected static void run(Class<?> clazz)
            throws RunnerException
    {
        benchmark(clazz, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
