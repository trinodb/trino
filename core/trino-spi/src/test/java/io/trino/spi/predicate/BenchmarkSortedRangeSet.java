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
package io.trino.spi.predicate;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.LongStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSortedRangeSet
{
    @Benchmark
    public SortedRangeSet benchmarkBuilder(Data data)
    {
        return SortedRangeSet.buildFromUnsortedRanges(BIGINT, data.ranges);
    }

    @Benchmark
    public List<SortedRangeSet> ofSingleRange(Data data)
    {
        List<SortedRangeSet> result = new ArrayList<>(data.ranges.size());
        for (Range range : data.ranges) {
            // intentionally going through public interface to cover any overhead or code path redirection this could incur
            ValueSet valueSet = ValueSet.ofRanges(range);
            result.add((SortedRangeSet) valueSet);
        }
        return result;
    }

    @Benchmark
    public List<Boolean> equalsSmall(Data data)
    {
        return benchmarkEquals(data.smallRanges);
    }

    @Benchmark
    public List<Boolean> equalsLarge(Data data)
    {
        return benchmarkEquals(data.largeRanges);
    }

    private List<Boolean> benchmarkEquals(List<SortedRangeSet> dataRanges)
    {
        List<Boolean> result = new ArrayList<>(dataRanges.size() - 1);
        for (int index = 0; index < dataRanges.size() - 1; index++) {
            result.add(dataRanges.get(index).equals(dataRanges.get(index + 1)));
        }
        return result;
    }

    @Benchmark
    public List<SortedRangeSet> unionSmall(Data data)
    {
        return benchmarkUnion(data.smallRanges);
    }

    @Benchmark
    public List<SortedRangeSet> unionLarge(Data data)
    {
        return benchmarkUnion(data.largeRanges);
    }

    private List<SortedRangeSet> benchmarkUnion(List<SortedRangeSet> dataRanges)
    {
        List<SortedRangeSet> result = new ArrayList<>(dataRanges.size() - 1);
        for (int index = 0; index < dataRanges.size() - 1; index++) {
            result.add(dataRanges.get(index).union(dataRanges.get(index + 1)));
        }
        return result;
    }

    @Benchmark
    public List<Boolean> overlapsSmall(Data data)
    {
        return benchmarkOverlaps(data.smallRanges, data.smallRanges);
    }

    @Benchmark
    public List<Boolean> overlapsLarge(Data data)
    {
        return benchmarkOverlaps(data.largeRanges, data.largeRanges);
    }

    @Benchmark
    public List<Boolean> overlapsSmallOnVeryLarge(Data data)
    {
        return benchmarkOverlaps(data.veryLargeRanges, data.smallRanges);
    }

    private List<Boolean> benchmarkOverlaps(List<SortedRangeSet> dataRanges, List<SortedRangeSet> otherDataRanges)
    {
        List<Boolean> result = new ArrayList<>(dataRanges.size() - 1);
        for (int index = 0; index < dataRanges.size() - 1; index++) {
            result.add(dataRanges.get(index).overlaps(otherDataRanges.get(index + 1 % otherDataRanges.size())));
        }
        return result;
    }

    @Benchmark
    public List<ValueSet> linearIntersectSmall(Data data)
    {
        return benchmarkIntersect(data.smallRanges, data.smallRanges, SortedRangeSet::linearSearchIntersect);
    }

    @Benchmark
    public List<ValueSet> binaryIntersectSmall(Data data)
    {
        return benchmarkIntersect(data.smallRanges, data.smallRanges, SortedRangeSet::binarySearchIntersect);
    }

    @Benchmark
    public List<ValueSet> linearIntersectLarge(Data data)
    {
        return benchmarkIntersect(data.largeRanges, data.largeRanges, SortedRangeSet::linearSearchIntersect);
    }

    @Benchmark
    public List<ValueSet> binaryIntersectLarge(Data data)
    {
        return benchmarkIntersect(data.largeRanges, data.largeRanges, SortedRangeSet::binarySearchIntersect);
    }

    @Benchmark
    public List<ValueSet> linearIntersectSmallOnVeryLarge(Data data)
    {
        return benchmarkIntersect(data.largeRanges, data.smallRanges, SortedRangeSet::linearSearchIntersect);
    }

    @Benchmark
    public List<ValueSet> binaryIntersectSmallOnVeryLarge(Data data)
    {
        return benchmarkIntersect(data.largeRanges, data.smallRanges, SortedRangeSet::binarySearchIntersect);
    }

    @Benchmark
    public List<ValueSet> linearIntersectDiscreteOnLarge(Data data)
    {
        return benchmarkIntersectionSingle(data.largeDiscreteSortedRangeSet, SortedRangeSet::linearSearchIntersect);
    }

    @Benchmark
    public List<ValueSet> linearDiscreteIntersectDiscreteOnLarge(Data data)
    {
        return benchmarkIntersectionSingle(data.largeDiscreteSortedRangeSet, SortedRangeSet::linearDiscreteSetIntersect);
    }

    @Benchmark
    public List<ValueSet> linearUnionDiscreteOnLarge(Data data)
    {
        return benchmarkIntersectionSingle(data.largeDiscreteSortedRangeSet, SortedRangeSet::linearDiscreteSetUnion);
    }

    @Benchmark
    public List<ValueSet> binaryIntersectRangeOnLarge(Data data)
    {
        return benchmarkIntersectionSingle(data.largeRangeSortedRangeSet, SortedRangeSet::binarySearchIntersect);
    }

    @Benchmark
    public List<ValueSet> linearIntersectRangeOnLarge(Data data)
    {
        return benchmarkIntersectionSingle(data.largeRangeSortedRangeSet, SortedRangeSet::linearSearchIntersect);
    }

    @Benchmark
    public List<ValueSet> binaryIntersectDiscreteOnLarge(Data data)
    {
        return benchmarkIntersectionSingle(data.largeDiscreteSortedRangeSet, SortedRangeSet::binarySearchIntersect);
    }

    public List<ValueSet> benchmarkIntersectionSingle(SortedRangeSet target, BiFunction<SortedRangeSet, SortedRangeSet, SortedRangeSet> intersection)
    {
        List<ValueSet> result = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            SortedRangeSet test = SortedRangeSet.of(range(BIGINT, ThreadLocalRandom.current().nextLong(0, 100), i % 2 == 0, ThreadLocalRandom.current().nextLong(99950, 100_000), i % 2 == 1));
            result.add(intersection.apply(target, test));
        }
        return result;
    }

    private List<ValueSet> benchmarkIntersect(List<SortedRangeSet> dataRanges, List<SortedRangeSet> testRanges, BiFunction<SortedRangeSet, SortedRangeSet, SortedRangeSet> intersection)
    {
        List<ValueSet> result = new ArrayList<>(dataRanges.size() - 1);
        for (int index = 0; index < dataRanges.size() - 1; index++) {
            result.add(intersection.apply(dataRanges.get(index), testRanges.get(index + 1 % testRanges.size())));
        }
        return result;
    }

    @Benchmark
    public long containsValueSmall(Data data)
    {
        return benchmarkContainsValue(data.smallRanges);
    }

    @Benchmark
    public long containsValueLarge(Data data)
    {
        return benchmarkContainsValue(data.largeRanges);
    }

    private long benchmarkContainsValue(List<SortedRangeSet> dataRanges)
    {
        int totalChecks = 5_000_000;
        long testedValuesTo = 10_000;

        long checksPerSet = totalChecks / dataRanges.size();
        long step = testedValuesTo / checksPerSet;
        long found = 0;
        for (SortedRangeSet dataRange : dataRanges) {
            for (long i = 0; i < testedValuesTo; i += step) {
                boolean contained = dataRange.containsValue(i);
                if (contained) {
                    found++;
                }
            }
        }
        return found;
    }

    @Benchmark
    public List<SortedRangeSet> complementSmall(Data data)
    {
        return benchmarkComplement(data.smallRanges);
    }

    @Benchmark
    public List<SortedRangeSet> complementLarge(Data data)
    {
        return benchmarkComplement(data.largeRanges);
    }

    private List<SortedRangeSet> benchmarkComplement(List<SortedRangeSet> dataRanges)
    {
        List<SortedRangeSet> result = new ArrayList<>(dataRanges.size());
        for (SortedRangeSet dataRange : dataRanges) {
            result.add(dataRange.complement());
        }
        return result;
    }

    @Benchmark
    public List<Integer> getOrderedRangesSmall(Data data)
    {
        return benchmarkGetOrderedRanges(data.smallRanges);
    }

    @Benchmark
    public List<Integer> getOrderedRangesLarge(Data data)
    {
        return benchmarkGetOrderedRanges(data.largeRanges);
    }

    private List<Integer> benchmarkGetOrderedRanges(List<SortedRangeSet> dataRanges)
    {
        List<Integer> result = new ArrayList<>(dataRanges.size());
        for (int index = 0; index < dataRanges.size(); index++) {
            int hash = 0;
            for (Range orderedRange : dataRanges.get(index).getRanges().getOrderedRanges()) {
                if (!orderedRange.isLowUnbounded()) {
                    hash = hash * 31 + orderedRange.getLowBoundedValue().hashCode();
                }
                if (!orderedRange.isHighUnbounded()) {
                    hash = hash * 31 + orderedRange.getHighBoundedValue().hashCode();
                }
            }
            result.add(hash);
        }
        return result;
    }

    @State(Scope.Thread)
    public static class Data
    {
        public List<Range> ranges;
        public List<SortedRangeSet> smallRanges;
        public List<SortedRangeSet> largeRanges;
        private List<SortedRangeSet> veryLargeRanges;
        public SortedRangeSet largeDiscreteSortedRangeSet = SortedRangeSet.of(BIGINT, 0L, LongStream.range(1, 100_000).boxed().toList().toArray());
        public SortedRangeSet largeRangeSortedRangeSet = SortedRangeSet.of(Range.range(BIGINT, 0L, true, 9L, true),
                LongStream.rangeClosed(1L, 100_000L).mapToObj(l -> Range.range(BIGINT, l * 10, l % 2 == 1, (l + 1) * 10 - 1, l % 2 == 0)).toList().toArray(Range[]::new));

        @Setup(Level.Iteration)
        public void init()
        {
            ranges = new ArrayList<>();

            long factor = 0;
            for (int i = 0; i < 10_000; i++) {
                long from = ThreadLocalRandom.current().nextLong(100) + factor * 100;
                long to = ThreadLocalRandom.current().nextLong(100) + (factor + 1) * 100;
                factor++;

                ranges.add(range(BIGINT, from, false, to, false));
            }

            smallRanges = generateRangeSets(50_000, 2);
            largeRanges = generateRangeSets(5_000, 300);
            veryLargeRanges = generateRangeSets(5_000, 30_000);
        }

        private List<SortedRangeSet> generateRangeSets(int count, int size)
        {
            List<SortedRangeSet> sortedRangeSets = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                sortedRangeSets.add(generateRangeSet(size));
            }
            return sortedRangeSets;
        }

        private SortedRangeSet generateRangeSet(int size)
        {
            List<Range> selectedRanges = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                selectedRanges.add(ranges.get(ThreadLocalRandom.current().nextInt(ranges.size())));
            }
            return SortedRangeSet.copyOf(BIGINT, selectedRanges);
        }
    }

    @Test
    public void test()
    {
        Data data = new Data();
        data.init();

        benchmarkBuilder(data);

        ofSingleRange(data);

        equalsSmall(data);
        equalsLarge(data);

        unionSmall(data);
        unionLarge(data);
        linearUnionDiscreteOnLarge(data);

        overlapsSmall(data);
        overlapsLarge(data);
        overlapsSmallOnVeryLarge(data);

        linearIntersectSmall(data);
        linearIntersectLarge(data);
        linearIntersectLarge(data);
        linearIntersectSmallOnVeryLarge(data);
        linearIntersectRangeOnLarge(data);
        linearIntersectDiscreteOnLarge(data);
        binaryIntersectSmall(data);
        binaryIntersectLarge(data);
        binaryIntersectLarge(data);
        binaryIntersectSmallOnVeryLarge(data);
        binaryIntersectRangeOnLarge(data);
        binaryIntersectDiscreteOnLarge(data);

        containsValueSmall(data);
        containsValueLarge(data);

        complementSmall(data);
        complementLarge(data);

        getOrderedRangesSmall(data);
        getOrderedRangesLarge(data);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkSortedRangeSet.class).run();
    }
}
