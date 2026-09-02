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
package io.trino.operator.scalar;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.type.JoniRegexp;
import io.trino.type.Re2JRegexp;
import io.trino.type.SafeReRegexp;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 2, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = SECONDS)
public class BenchmarkRegexpFunctions
{
    @Benchmark
    public boolean benchmarkLikeJoni(DotStarAroundData data)
    {
        return JoniRegexpFunctions.regexpLike(data.getSource(), data.getJoniPattern());
    }

    @Benchmark
    public boolean benchmarkLikeRe2J(DotStarAroundData data)
    {
        return Re2JRegexpFunctions.regexpLike(data.getSource(), data.getRe2JPattern());
    }

    @Benchmark
    public boolean benchmarkLikeSafeRe(DotStarAroundData data)
    {
        return SafeReRegexpFunctions.regexpLike(data.getSource(), data.getSafeRePattern());
    }

    @Benchmark
    public Slice benchmarkReplaceJoni(DotStarAroundData data)
    {
        return JoniRegexpFunctions.regexpReplace(data.getSource(), data.getJoniPattern(), Slices.EMPTY_SLICE);
    }

    @Benchmark
    public Slice benchmarkReplaceSafeRe(DotStarAroundData data)
    {
        return SafeReRegexpFunctions.regexpReplace(data.getSource(), data.getSafeRePattern(), Slices.EMPTY_SLICE);
    }

    @Benchmark
    public long benchmarkCountJoni(DotStarAroundData data)
    {
        return JoniRegexpFunctions.regexpCount(data.getSource(), data.getJoniPattern());
    }

    @Benchmark
    public long benchmarkCountSafeRe(DotStarAroundData data)
    {
        return SafeReRegexpFunctions.regexpCount(data.getSource(), data.getSafeRePattern());
    }

    @Benchmark
    public Slice benchmarkExtractJoni(DotStarAroundData data)
    {
        return JoniRegexpFunctions.regexpExtract(data.getSource(), data.getJoniPattern());
    }

    @Benchmark
    public Slice benchmarkExtractSafeRe(DotStarAroundData data)
    {
        return SafeReRegexpFunctions.regexpExtract(data.getSource(), data.getSafeRePattern());
    }

    @State(Thread)
    public static class DotStarAroundData
    {
        @Param({".*x.*", ".*(x|y).*", "longdotstar", "phone", "literal"})
        private String patternString;

        @Param({"1024", "32768"})
        private int sourceLength;

        private JoniRegexp joniPattern;
        private Re2JRegexp re2JPattern;
        private SafeReRegexp safeRePattern;
        private Slice source;

        @Setup
        public void setup()
        {
            SliceOutput sliceOutput = new DynamicSliceOutput(sourceLength);
            // Fixed seed so that first-match-position-sensitive benchmarks (e.g. regexp_like over
            // the "phone" input) measure the same text across engines, forks, and runs.
            Random random = new Random(42);
            Slice pattern;
            switch (patternString) {
                case ".*x.*" -> {
                    pattern = Slices.utf8Slice(".*x.*");
                    IntStream.generate(() -> 97).limit(sourceLength).forEach(sliceOutput::appendByte);
                }
                case ".*(x|y).*" -> {
                    pattern = Slices.utf8Slice(".*(x|y).*");
                    IntStream.generate(() -> 97).limit(sourceLength).forEach(sliceOutput::appendByte);
                }
                case "longdotstar" -> {
                    pattern = Slices.utf8Slice(".*coolfunctionname.*");
                    random.ints(97, 123).limit(sourceLength).forEach(sliceOutput::appendByte);
                }
                case "phone" -> {
                    pattern = Slices.utf8Slice("\\d{3}/\\d{3}/\\d{4}");
                    // 47: '/', 48-57: '0'-'9'
                    random.ints(47, 58).limit(sourceLength).forEach(sliceOutput::appendByte);
                }
                case "literal" -> {
                    pattern = Slices.utf8Slice("literal");
                    // 97-122: 'a'-'z'
                    random.ints(97, 123).limit(sourceLength).forEach(sliceOutput::appendByte);
                }
                default -> throw new IllegalArgumentException("pattern: " + patternString + " not supported");
            }

            joniPattern = joniRegexp(pattern);
            re2JPattern = re2JRegexp(pattern);
            safeRePattern = new SafeReRegexp(pattern);
            source = sliceOutput.slice();
            checkState(source.length() == sourceLength, "source.length=%s, sourceLength=%s", source.length(), sourceLength);
        }

        public Slice getSource()
        {
            return source;
        }

        public JoniRegexp getJoniPattern()
        {
            return joniPattern;
        }

        public Re2JRegexp getRe2JPattern()
        {
            return re2JPattern;
        }

        public SafeReRegexp getSafeRePattern()
        {
            return safeRePattern;
        }
    }

    private static Re2JRegexp re2JRegexp(Slice pattern)
    {
        return new Re2JRegexp(Integer.MAX_VALUE, 5, pattern);
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkRegexpFunctions.class).run();
    }
}
