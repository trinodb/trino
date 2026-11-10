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

import io.trino.parquet.GroupField;
import io.trino.parquet.reader.ListColumnReader.BlockPositions;
import io.trino.spi.type.ArrayType;
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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Fork(2)
public class BenchmarkCollectionOffsets
{
    private static final int VALUES = 1_000_000;

    public enum Shape
    {
        FIXED,
        FIXED_WITH_NULL_ELEMENTS,
        FIXED_WITH_EMPTY_LISTS,
        VARIABLE,
    }

    @Param
    public Shape shape;

    @Param({"4", "128"})
    public int length;

    @Param({"false", "true"})
    public boolean required;

    private GroupField field;
    private int[] definitionLevels;
    private int[] repetitionLevels;

    @Setup
    public void setup()
    {
        // optional list<optional int>: null list 0, empty list 1, null element 2, element 3
        // required list<optional int>: empty list 0, null element 1, element 2
        int base = required ? 0 : 1;
        field = new GroupField(new ArrayType(INTEGER), 0, base, required, List.of(Optional.empty()));

        Random random = new Random(42);
        int[] definition = new int[VALUES + length * 2];
        int[] repetition = new int[definition.length];
        int count = 0;
        while (count < VALUES) {
            int size = length;
            if (shape == Shape.VARIABLE) {
                size = 1 + random.nextInt(length * 2);
            }
            if (shape == Shape.FIXED_WITH_EMPTY_LISTS && random.nextInt(1000) == 0) {
                definition[count] = base;
                repetition[count] = 0;
                count++;
                continue;
            }
            for (int i = 0; i < size; i++) {
                boolean nullElement = shape == Shape.FIXED_WITH_NULL_ELEMENTS && random.nextInt(10) == 0;
                definition[count] = base + (nullElement ? 1 : 2);
                repetition[count] = i == 0 ? 0 : 1;
                count++;
            }
        }
        definitionLevels = Arrays.copyOf(definition, count);
        repetitionLevels = Arrays.copyOf(repetition, count);
    }

    @Benchmark
    public BlockPositions offsets()
    {
        return ListColumnReader.calculateCollectionOffsets(field, definitionLevels, repetitionLevels);
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkCollectionOffsets.class, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
