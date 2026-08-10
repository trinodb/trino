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

import io.trino.parquet.writer.repdef.DefLevelWriterProvider.DefinitionLevelWriter;
import io.trino.parquet.writer.repdef.DefLevelWriterProvider.ValuesCount;
import io.trino.parquet.writer.repdef.DefLevelWriterProviders;
import io.trino.parquet.writer.valuewriter.ColumnDescriptorValuesWriter;
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Bitmap;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Optional;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class BenchmarkBooleanDefinitionLevelWriter
{
    private static final int POSITION_COUNT = 16_384;

    @Param({"10", "50"})
    public int nullsPercentage;

    private BitArrayBlock block;
    private ColumnDescriptorValuesWriter valuesWriter;
    private DefinitionLevelWriter definitionLevelWriter;

    @Setup
    public void setup()
    {
        Random random = new Random(872364);
        long[] values = Bitmap.allocateWords(POSITION_COUNT, false);
        long[] validity = Bitmap.allocateWords(POSITION_COUNT, false);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (random.nextBoolean()) {
                Bitmap.set(values, 0, position);
            }
            if (random.nextInt(100) >= nullsPercentage) {
                Bitmap.set(validity, 0, position);
            }
        }
        block = new BitArrayBlock(POSITION_COUNT, Optional.of(validity), values);
        ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"test"}, Types.optional(BOOLEAN).named("test"), 0, 1);
        valuesWriter = ColumnDescriptorValuesWriter.newDefinitionLevelWriter(descriptor, 1024 * 1024);
    }

    @Setup(Invocation)
    public void reset()
    {
        valuesWriter.reset();
        definitionLevelWriter = DefLevelWriterProviders.of(block, 1).getDefinitionLevelWriter(Optional.empty(), valuesWriter);
    }

    @Benchmark
    public ValuesCount write()
    {
        return definitionLevelWriter.writeDefinitionLevels();
    }

    @Test
    public void verify()
    {
        nullsPercentage = 10;
        setup();
        reset();
        write();
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkBooleanDefinitionLevelWriter.class).run();
    }
}
