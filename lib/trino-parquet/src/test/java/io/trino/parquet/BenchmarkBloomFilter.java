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
package io.trino.parquet;

import io.airlift.slice.Slice;
import io.trino.parquet.writer.valuewriter.PlainValuesWriter;
import io.trino.parquet.writer.valuewriter.ValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.parquet.predicate.TupleDomainParquetPredicate.checkInBloomFilter;
import static io.trino.parquet.writer.valuewriter.BloomFilterValuesWriter.createBloomFilterValuesWriter;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkBloomFilter
{
    private static final int VALUES = 1024;

    @Param({"16", "64", "1024"})
    public int length;

    private Slice[] values;
    private Slice[] missingValues;
    private BloomFilter bloomFilter;
    private ValuesWriter writer;

    @Setup
    public void setup()
    {
        Random random = new Random(0);
        values = new Slice[VALUES];
        missingValues = new Slice[VALUES];
        bloomFilter = new BlockSplitBloomFilter(16 * 1024);
        writer = createBloomFilterValuesWriter(new PlainValuesWriter(VALUES * (length + Integer.BYTES)), Optional.of(new BlockSplitBloomFilter(16 * 1024)));
        for (int i = 0; i < VALUES; i++) {
            values[i] = randomSlice(random);
            missingValues[i] = randomSlice(random);
            bloomFilter.insertHash(bloomFilter.hash(Binary.fromConstantByteArray(values[i].getBytes())));
        }
    }

    private Slice randomSlice(Random random)
    {
        byte[] bytes = new byte[length + 11];
        random.nextBytes(bytes);
        return wrappedBuffer(bytes, 7, length);
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public long write()
    {
        writer.reset();
        for (Slice value : values) {
            writer.writeBytes(value);
        }
        return writer.getBufferedSize();
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public int lookupHit()
    {
        return lookup(values);
    }

    @Benchmark
    @OperationsPerInvocation(VALUES)
    public int lookupMiss()
    {
        return lookup(missingValues);
    }

    private int lookup(Slice[] inputs)
    {
        int matches = 0;
        for (Slice value : inputs) {
            if (checkInBloomFilter(bloomFilter, value, VARBINARY)) {
                matches++;
            }
        }
        return matches;
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkBloomFilter.class).run();
    }
}
