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
package io.trino.orc.stream;

import io.trino.orc.OrcOutputBuffer;
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Bitmap;
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
import static io.trino.orc.metadata.CompressionKind.NONE;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class BenchmarkBooleanOutputStream
{
    private static final int POSITION_COUNT = 8192;

    @Param({"0", "10", "50"})
    public int nullsPercentage;

    private BitArrayBlock block;
    private BooleanOutputStream packedOutput;
    private BooleanOutputStream scalarOutput;
    private PresentOutputStream packedPresentOutput;
    private PresentOutputStream scalarPresentOutput;

    @Setup
    public void setup()
    {
        Random random = new Random(812347);
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
        block = new BitArrayBlock(POSITION_COUNT, nullsPercentage == 0 ? Optional.empty() : Optional.of(validity), values);
        packedOutput = new BooleanOutputStream(new OrcOutputBuffer(NONE, 1024));
        scalarOutput = new BooleanOutputStream(new OrcOutputBuffer(NONE, 1024));
        packedPresentOutput = new PresentOutputStream(NONE, 1024);
        scalarPresentOutput = new PresentOutputStream(NONE, 1024);
    }

    @Setup(Invocation)
    public void reset()
    {
        packedOutput.reset();
        scalarOutput.reset();
        packedPresentOutput.reset();
        scalarPresentOutput.reset();
    }

    @Benchmark
    public long packedValues()
    {
        for (int position = 0; position < POSITION_COUNT; position += Long.SIZE) {
            packedOutput.writeBits(block.getRawValues()[position / Long.SIZE], Math.min(Long.SIZE, POSITION_COUNT - position));
        }
        return packedOutput.getBufferedBytes();
    }

    @Benchmark
    public long scalarValues()
    {
        for (int position = 0; position < POSITION_COUNT; position++) {
            scalarOutput.writeBoolean(block.getBoolean(position));
        }
        return scalarOutput.getBufferedBytes();
    }

    @Benchmark
    public long packedPresent()
    {
        packedPresentOutput.writeBlock(block);
        return packedPresentOutput.getBufferedBytes();
    }

    @Benchmark
    public long scalarPresent()
    {
        for (int position = 0; position < POSITION_COUNT; position++) {
            scalarPresentOutput.writeBoolean(!block.isNull(position));
        }
        return scalarPresentOutput.getBufferedBytes();
    }

    @Test
    public void verify()
    {
        setup();
        reset();
        packedValues();
        scalarValues();
        packedPresent();
        scalarPresent();
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkBooleanOutputStream.class).run();
    }
}
