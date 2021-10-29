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
package io.trino.operator.aggregation.state;

import com.google.common.collect.ImmutableSet;
import io.trino.jmh.Benchmarks;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory.GroupedLongDecimalWithOverflowAndLongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
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
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.stream.Collectors.toList;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongDecimalWithOverflowAndLongStateSerializer
{
    @Benchmark
    public Object serialize(BenchmarkData data)
    {
        return data.serialize(new LongDecimalWithOverflowAndLongStateSerializer());
    }

    @Benchmark
    public Object deserialize(BenchmarkData data)
    {
        return data.deserialize(new LongDecimalWithOverflowAndLongStateSerializer());
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final Random RANDOM = new Random(45624387);
        private static final int GROUP_COUNT = 1024;

        @Param({"0", "0.2", "0.5", "0.8"})
        private float nullRate;

        @Param({"0", "0.2", "0.5", "0.8"})
        private float overflowRate;

        private GroupedLongDecimalWithOverflowAndLongState inState;
        private GroupedLongDecimalWithOverflowAndLongState outState;
        private Block block;

        @Setup
        public void setup()
        {
            inState = new GroupedLongDecimalWithOverflowAndLongState();
            inState.ensureCapacity(GROUP_COUNT);
            Set<Integer> nullPositions = chooseRandomUnique(GROUP_COUNT, nullRate, ImmutableSet.of());
            Set<Integer> overflowPositions = chooseRandomUnique(GROUP_COUNT, overflowRate, nullPositions);

            for (int i = 0; i < GROUP_COUNT; i++) {
                inState.setGroupId(i);
                if (nullPositions.contains(i)) {
                    continue;
                }

                inState.setNotNull();
                inState.setLong(RANDOM.nextLong());
                long[] decimalArray = inState.getDecimalArray();
                decimalArray[0] = RANDOM.nextLong();
                decimalArray[1] = RANDOM.nextLong();
                if (overflowPositions.contains(i)) {
                    inState.setOverflow(RANDOM.nextLong());
                }
            }

            block = serialize(new LongDecimalWithOverflowAndLongStateSerializer()).build();
            outState = new GroupedLongDecimalWithOverflowAndLongState();
            outState.ensureCapacity(GROUP_COUNT);
        }

        private static Set<Integer> chooseRandomUnique(int bound, float rate, Set<Integer> taken)
        {
            int count = (int) (bound * rate);
            if (count == 0) {
                verify(rate == 0, "bound %s too small to have at least one  result with rate %s", (Object) count, rate);
                return ImmutableSet.of();
            }
            verify(bound >= count + taken.size(), "Too many numbers already taken bound %s, count %s, taken %s", bound, count, taken.size());

            List<Integer> availableNumbers = IntStream.range(0, bound).boxed()
                    .filter(i -> !taken.contains(i))
                    .collect(toList());
            Collections.shuffle(availableNumbers, RANDOM);
            return availableNumbers.stream()
                    .limit(count)
                    .collect(toImmutableSet());
        }

        public BlockBuilder serialize(AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState> serializer)
        {
            BlockBuilder out = serializer.getSerializedType().createBlockBuilder(null, GROUP_COUNT);
            GroupedLongDecimalWithOverflowAndLongState state = this.inState;
            for (int i = 0; i < GROUP_COUNT; i++) {
                state.setGroupId(i);
                serializer.serialize(state, out);
            }
            return out;
        }

        private GroupedLongDecimalWithOverflowAndLongState deserialize(AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState> serializer)
        {
            for (int i = 0; i < BenchmarkData.GROUP_COUNT; i++) {
                outState.setGroupId(i);
                serializer.deserialize(block, i, outState);
            }
            return outState;
        }

        public GroupedLongDecimalWithOverflowAndLongState getOutState()
        {
            return outState;
        }
    }

    @Test
    public void verifyBenchmark()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        new BenchmarkLongDecimalWithOverflowAndLongStateSerializer().serialize(data);
        new BenchmarkLongDecimalWithOverflowAndLongStateSerializer().deserialize(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // ensure the benchmarks are valid before running
        new BenchmarkLongDecimalWithOverflowAndLongStateSerializer().verifyBenchmark();

        Benchmarks.benchmark(BenchmarkLongDecimalWithOverflowAndLongStateSerializer.class).run();
    }
}
