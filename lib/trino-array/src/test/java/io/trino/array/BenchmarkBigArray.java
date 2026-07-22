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
package io.trino.array;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(3)
@Warmup(iterations = 8)
@Measurement(iterations = 10)
public class BenchmarkBigArray
{
    @State(Scope.Thread)
    public static class Data
    {
        @Param({"1024", "1048576", "16777216"})
        private int size;

        private LongBigArray longArray;
        private IntBigArray intArray;

        @Setup
        public void setup()
        {
            longArray = new LongBigArray();
            longArray.ensureCapacity(size);
            intArray = new IntBigArray();
            intArray.ensureCapacity(size);
            for (int i = 0; i < size; i++) {
                long value = ThreadLocalRandom.current().nextLong();
                longArray.set(i, value);
                intArray.set(i, (int) value);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public long longSequentialGet(Data data)
    {
        long sum = 0;
        for (long index = 0; index < data.size; index++) {
            sum += data.longArray.get(index);
        }
        return sum;
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public long longForEachSegment(Data data)
    {
        long[] sum = {0};
        data.longArray.forEachSegment(0, data.size, (segment, offset, length) -> {
            long localSum = 0;
            for (int i = offset; i < offset + length; i++) {
                localSum += segment[i];
            }
            sum[0] += localSum;
        });
        return sum[0];
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public long intSequentialGet(Data data)
    {
        long sum = 0;
        for (long index = 0; index < data.size; index++) {
            sum += data.intArray.get(index);
        }
        return sum;
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public long intForEachSegment(Data data)
    {
        long[] sum = {0};
        data.intArray.forEachSegment(0, data.size, (segment, offset, length) -> {
            long localSum = 0;
            for (int i = offset; i < offset + length; i++) {
                localSum += segment[i];
            }
            sum[0] += localSum;
        });
        return sum[0];
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkBigArray.class, WarmupMode.BULK).run();
    }
}
