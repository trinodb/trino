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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.parquet.reader.flat.BitColumnAdapter.BIT_ADAPTER;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.countTransitions;
import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(3)
public class BenchmarkBitColumnAdapter
{
    private static final int POSITION_COUNT = 4096;

    @Param
    public ValidityPattern validityPattern;

    private BitBuffer source;
    private BitBuffer destination;
    private long[] validity;
    private int nonNullCount;

    @Setup
    public void setup()
    {
        Random random = new Random(9182734L);
        validity = new long[(POSITION_COUNT + Long.SIZE - 1) / Long.SIZE];
        nonNullCount = 0;
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (validityPattern.isValid(random, position)) {
                set(validity, 0, position);
                nonNullCount++;
            }
        }
        source = new BitBuffer(nonNullCount);
        for (int position = 0; position < nonNullCount; position++) {
            if (random.nextBoolean()) {
                set(source.getValues(), 0, position);
            }
        }
        destination = new BitBuffer(POSITION_COUNT);
    }

    @Benchmark
    public BitBuffer expand()
    {
        BIT_ADAPTER.unpackNullValues(source, destination, validity, 0, nonNullCount, POSITION_COUNT);
        return destination;
    }

    @Benchmark
    public BitBuffer scalar()
    {
        int sourcePosition = 0;
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (isSet(validity, 0, position) && isSet(source.getValues(), 0, sourcePosition++)) {
                set(destination.getValues(), 0, position);
            }
            else {
                clear(destination.getValues(), 0, position);
            }
        }
        return destination;
    }

    @Benchmark
    public BitBuffer generic()
    {
        int sourceOffset = 0;
        int destinationOffset = 0;
        while (sourceOffset < nonNullCount && destinationOffset < POSITION_COUNT) {
            int bitsInWord = Math.min(Long.SIZE, POSITION_COUNT - destinationOffset);
            long validBits = getBits(validity, 0, destinationOffset, bitsInWord);
            if (validBits == 0) {
                destinationOffset += bitsInWord;
                continue;
            }
            if (Long.bitCount(validBits) == bitsInWord) {
                BIT_ADAPTER.copyValues(source, sourceOffset, destination, destinationOffset, bitsInWord);
                sourceOffset += bitsInWord;
                destinationOffset += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= 12) {
                int endOffsetInWord = destinationOffset + bitsInWord;
                while (destinationOffset < endOffsetInWord) {
                    BIT_ADAPTER.copyValue(source, sourceOffset, destination, destinationOffset);
                    sourceOffset += (int) (validBits & 1);
                    destinationOffset++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = Math.min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                destinationOffset += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = Math.min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                if (validCount == 1) {
                    BIT_ADAPTER.copyValue(source, sourceOffset, destination, destinationOffset);
                }
                else {
                    BIT_ADAPTER.copyValues(source, sourceOffset, destination, destinationOffset, validCount);
                }
                sourceOffset += validCount;
                destinationOffset += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
        return destination;
    }

    public enum ValidityPattern
    {
        ALL_NULL {
            @Override
            boolean isValid(Random random, int position)
            {
                return false;
            }
        },
        ALL_VALID {
            @Override
            boolean isValid(Random random, int position)
            {
                return true;
            }
        },
        ONE_PERCENT {
            @Override
            boolean isValid(Random random, int position)
            {
                return random.nextDouble() < 0.01;
            }
        },
        TEN_PERCENT {
            @Override
            boolean isValid(Random random, int position)
            {
                return random.nextDouble() < 0.10;
            }
        },
        FIFTY_PERCENT {
            @Override
            boolean isValid(Random random, int position)
            {
                return random.nextBoolean();
            }
        },
        NINETY_PERCENT {
            @Override
            boolean isValid(Random random, int position)
            {
                return random.nextDouble() < 0.90;
            }
        },
        SHORT_RUNS {
            @Override
            boolean isValid(Random random, int position)
            {
                return (position / 7 & 1) == 0;
            }
        },
        LONG_RUNS {
            @Override
            boolean isValid(Random random, int position)
            {
                return (position / 127 & 1) == 0;
            }
        };

        abstract boolean isValid(Random random, int position);
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkBitColumnAdapter.class).run();
    }
}
