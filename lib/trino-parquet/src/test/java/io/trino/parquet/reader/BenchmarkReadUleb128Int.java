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
package io.trino.parquet.reader;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetReaderUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.bytes.BytesUtils.writeUnsignedVarInt;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Fork(3)
public class BenchmarkReadUleb128Int
{
    @Param({
            "100",
            "1000"
    })
    private int size;

    @Param({
            "20000",
            "4000000"
    })
    private int maxValue;

    private Slice[] inputValues;

    public BenchmarkReadUleb128Int()
    {
        maxValue = Integer.MAX_VALUE;
    }

    public BenchmarkReadUleb128Int(int size, int maxValue)
    {
        this.size = size;
        this.maxValue = maxValue;
    }

    @Setup
    public void setUp()
            throws IOException
    {
        inputValues = new Slice[size];
        Random random = new Random(1);
        for (int i = 0; i < size; i++) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            writeUnsignedVarInt(random.nextInt(maxValue), bos);
            inputValues[i] = Slices.wrappedBuffer(bos.toByteArray());
        }
    }

    @Benchmark
    public void readUleb128Int()
    {
        for (Slice input : inputValues) {
            sink(ParquetReaderUtils.readUleb128Int(new SimpleSliceInputStream(input)));
        }
    }

    @Benchmark
    public void readUleb128IntLoop()
    {
        for (Slice input : inputValues) {
            sink(readUleb128IntLoop(new SimpleSliceInputStream(input)));
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public static void sink(int value)
    {
        // IT IS VERY IMPORTANT TO MATCH THE SIGNATURE TO AVOID AUTOBOXING.
        // The method intentionally does nothing.
    }

    private static int readUleb128IntLoop(SimpleSliceInputStream input)
    {
        int value = 0;
        int i = 0;
        int b = input.readByte();
        while ((b & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            b = input.readByte();
        }
        return value | (b << i);
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkReadUleb128Int.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
