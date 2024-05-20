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

import io.trino.parquet.writer.valuewriter.PrimitiveValueWriter;
import io.trino.parquet.writer.valuewriter.TrinoValuesWriterFactory;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.parquet.writer.ParquetWriters.getValueWriter;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;

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
            "1", "1048576" // 1MB is default page size
    })
    public int dictionaryPageSize;

    public enum BloomFilterType
    {
        NONE {
            @Override
            Optional<BloomFilter> getBloomFilter()
            {
                return Optional.empty();
            }
        },
        DEFAULT_BLOOM_FILTER {
            @Override
            Optional<BloomFilter> getBloomFilter()
            {
                return Optional.of(new BlockSplitBloomFilter(1048576, 1048576));
            }
        },
        /**/;

        abstract Optional<BloomFilter> getBloomFilter();
    }

    // Parquet pages are usually about 1MB
    private static final int DATA_GENERATION_BATCH_SIZE = 16384;

    private PrimitiveValueWriter writer;
    private List<Block> blocks;

    protected abstract Type getTrinoType();

    protected abstract PrimitiveType getParquetType();

    protected abstract Block generateBlock(int size);

    private PrimitiveValueWriter createValuesWriter()
    {
        TrinoValuesWriterFactory valuesWriterFactory = new TrinoValuesWriterFactory(ParquetProperties.builder()
                .withWriterVersion(PARQUET_1_0)
                .withDictionaryPageSize(dictionaryPageSize)
                .build());
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"test"}, getParquetType(), 0, 0);
        return getValueWriter(valuesWriterFactory.newValuesWriter(columnDescriptor, bloomFilterType.getBloomFilter()), getTrinoType(), columnDescriptor.getPrimitiveType(), Optional.empty());
    }

    @Setup
    public void setup()
            throws IOException
    {
        this.blocks = IntStream.range(0, 2).boxed().map((i) -> generateBlock(DATA_GENERATION_BATCH_SIZE)).collect(Collectors.toList());
        this.writer = createValuesWriter();
    }

    @Benchmark
    public void write()
            throws IOException
    {
        for (Block block : blocks) {
            writer.write(block);
        }
        writer.reset();
    }

    protected static void run(Class<?> clazz)
            throws RunnerException
    {
        benchmark(clazz, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
