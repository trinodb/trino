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
package io.trino.parquet.reader.decoders;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.parquet.reader.TestData.generateMixedData;
import static io.trino.parquet.reader.TestData.randomUnsignedInt;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Fork(1)
public class BenchmarkRleBitPackingDecoder
{
    private static final int MAX_VALUES = 5_000_000;
    private static final int READ_BATCH_SIZE = 4096;

    private Slice inputSlice;
    private int[] output;

    @Param
    public DataSet dataSet;

    @Param({
            // This encoding is not meant to store big numbers so 2^20 is enough
            "1", "2", "3", "4", "5", "6", "7", "8",
            "9", "10", "13", "16", "17", "18", "20",
    })
    public int bitWidth;

    public enum DataSet
    {
        RANDOM {
            @Override
            int[] getData(int size, int bitWidth)
            {
                Random random = new Random((long) size * bitWidth);
                int[] values = new int[size];
                for (int i = 0; i < size; i++) {
                    values[i] = randomUnsignedInt(random, bitWidth);
                }
                return values;
            }
        },
        MIXED_AND_GROUPS_SMALL {
            @Override
            int[] getData(int size, int bitWidth)
            {
                Random random = new Random((long) size * bitWidth);
                return generateMixedData(random, size, 23, bitWidth);
            }
        },
        MIXED_AND_GROUPS_LARGE {
            @Override
            int[] getData(int size, int bitWidth)
            {
                Random random = new Random((long) size * bitWidth);
                return generateMixedData(random, size, 127, bitWidth);
            }
        },
        MIXED_AND_GROUPS_HUGE {
            @Override
            int[] getData(int size, int bitWidth)
            {
                Random random = new Random((long) size * bitWidth);
                return generateMixedData(random, size, 2111, bitWidth);
            }
        },
        /**/;

        abstract int[] getData(int size, int bitWidth);
    }

    @Setup
    public void setup()
            throws IOException
    {
        ValuesWriter writer = createValuesWriter(1_000_000);
        for (int value : generateDataBatch(MAX_VALUES)) {
            writer.writeInteger(value);
        }
        inputSlice = getInputSlice(writer.getBytes().toByteArray());
        output = new int[READ_BATCH_SIZE];
    }

    @Benchmark
    public void rleBitPackingHybridDecoder()
    {
        ValueDecoder<int[]> decoder = new RleBitPackingHybridDecoder(bitWidth);
        decoder.init(new SimpleSliceInputStream(inputSlice));
        for (int i = 0; i < MAX_VALUES; i += READ_BATCH_SIZE) {
            decoder.read(output, 0, min(READ_BATCH_SIZE, MAX_VALUES - i));
        }
    }

    @Benchmark
    public void apacheRunLengthBitPackingHybridDecoder()
    {
        RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, ByteBufferInputStream.wrap(inputSlice.toByteBuffer()));
        for (int i = 0; i < MAX_VALUES; i += READ_BATCH_SIZE) {
            try {
                for (int batchIndex = 0; batchIndex < min(READ_BATCH_SIZE, MAX_VALUES - i); batchIndex++) {
                    output[batchIndex] = decoder.readInt();
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private ValuesWriter createValuesWriter(int bufferSize)
    {
        return new RunLengthBitPackingHybridValuesWriter(bitWidth, bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
    }

    private int[] generateDataBatch(int size)
    {
        return dataSet.getData(size, bitWidth);
    }

    private Slice getInputSlice(byte[] data)
    {
        // Skip size encoded as first 4 bytes by Apache writer
        return Slices.wrappedBuffer(data, Integer.BYTES, data.length - Integer.BYTES);
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkRleBitPackingDecoder.class, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
