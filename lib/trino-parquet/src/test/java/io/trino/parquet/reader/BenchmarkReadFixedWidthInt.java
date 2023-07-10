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
import static org.apache.parquet.bytes.BytesUtils.writeIntLittleEndianPaddedOnBitWidth;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
public class BenchmarkReadFixedWidthInt
{
    private Slice[] inputValues;

    @Param("1000")
    public int size;

    @Param({"1", "2", "3", "4"})
    public int byteWidth;

    @Setup
    public void setUp()
            throws IOException
    {
        inputValues = new Slice[size];
        Random random = new Random(1);
        for (int i = 0; i < size; i++) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            writeIntLittleEndianPaddedOnBitWidth(bos, random.nextInt(), byteWidth * Byte.SIZE);
            inputValues[i] = Slices.wrappedBuffer(bos.toByteArray());
        }
    }

    @Benchmark
    public void readFixedWidthInt()
    {
        for (Slice input : inputValues) {
            sink(ParquetReaderUtils.readFixedWidthInt(new SimpleSliceInputStream(input), byteWidth));
        }
    }

    @Benchmark
    public void readFixedWidthIntLoop()
    {
        for (Slice input : inputValues) {
            sink(readFixedWidthIntLoop(new SimpleSliceInputStream(input), byteWidth));
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public static void sink(int value)
    {
        // IT IS VERY IMPORTANT TO MATCH THE SIGNATURE TO AVOID AUTOBOXING.
        // The method intentionally does nothing.
    }

    private static int readFixedWidthIntLoop(SimpleSliceInputStream input, int bytes)
    {
        int result = 0;
        for (int i = 0; i < bytes; i++) {
            result |= (input.readByte() & 0xFF) << (i * Byte.SIZE);
        }
        return result;
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkReadFixedWidthInt.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
