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
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slices;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.parquet.reader.TestData.generateMixedData;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
public class BenchmarkFlatDefinitionLevelDecoder
{
    public static final int BATCH_SIZE = 1024;

    @Param({
            "1000",
            "10000",
            "100000",
    })
    private int size;

    @Param({
            "RANDOM",
            "ONLY_NULLS",
            "ONLY_NON_NULLS",
            "MIXED_RANDOM_AND_SMALL_GROUPS",
            "MIXED_RANDOM_AND_BIG_GROUPS",
    })
    private DataGenerator dataGenerator;

    private byte[] data;
    // Dummy output array
    private boolean[] output;

    public BenchmarkFlatDefinitionLevelDecoder()
    {
    }

    public BenchmarkFlatDefinitionLevelDecoder(int size, DataGenerator dataGenerator)
    {
        this.size = size;
        this.dataGenerator = dataGenerator;
    }

    @Setup
    public void setup()
            throws IOException
    {
        data = dataGenerator.getData(size);
        output = new boolean[size];
    }

    @Benchmark
    public int read()
            throws IOException
    {
        NullsDecoder decoder = new NullsDecoder(Slices.wrappedBuffer(data));
        int nonNullCount = 0;
        for (int i = 0; i < size; i += BATCH_SIZE) {
            nonNullCount += decoder.readNext(output, i, Math.min(BATCH_SIZE, size - i));
        }
        return nonNullCount;
    }

    public enum DataGenerator
    {
        RANDOM {
            @Override
            public void generate(RunLengthBitPackingHybridEncoder encoder, Random random, int size)
                    throws IOException
            {
                for (int i = 0; i < size; i++) {
                    encoder.writeInt(random.nextBoolean() ? 1 : 0);
                }
            }
        },
        ONLY_NULLS {
            @Override
            public void generate(RunLengthBitPackingHybridEncoder encoder, Random random, int size)
                    throws IOException
            {
                for (int i = 0; i < size; i++) {
                    encoder.writeInt(0);
                }
            }
        },
        ONLY_NON_NULLS {
            @Override
            public void generate(RunLengthBitPackingHybridEncoder encoder, Random random, int size)
                    throws IOException
            {
                for (int i = 0; i < size; i++) {
                    encoder.writeInt(1);
                }
            }
        },
        MIXED_RANDOM_AND_SMALL_GROUPS {
            @Override
            public void generate(RunLengthBitPackingHybridEncoder encoder, Random random, int size)
                    throws IOException
            {
                boolean[] data = generateMixedData(random, size, 23);
                for (int i = 0; i < size; i++) {
                    encoder.writeInt(data[i] ? 1 : 0);
                }
            }
        },
        MIXED_RANDOM_AND_BIG_GROUPS {
            @Override
            public void generate(RunLengthBitPackingHybridEncoder encoder, Random random, int size)
                    throws IOException
            {
                boolean[] data = generateMixedData(random, size, 127);
                for (int i = 0; i < size; i++) {
                    encoder.writeInt(data[i] ? 1 : 0);
                }
            }
        },
        /**/;

        public abstract void generate(RunLengthBitPackingHybridEncoder encoder, Random random, int size)
                throws IOException;

        public byte[] getData(int size)
                throws IOException
        {
            Random random = new Random(1);
            RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(1, size, size, HeapByteBufferAllocator.getInstance());
            generate(encoder, random, size);
            return encoder.toBytes().toByteArray();
        }

        DataGenerator()
        {
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkFlatDefinitionLevelDecoder.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
