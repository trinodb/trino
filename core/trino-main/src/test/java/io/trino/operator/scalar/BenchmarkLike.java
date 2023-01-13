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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.likematcher.DfaLikeMatcher;
import io.trino.likematcher.LikeMatcher;
import io.trino.likematcher.RegexLikeMatcher;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 2, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = MILLISECONDS)
public class BenchmarkLike
{
    @State(Thread)
    public static class Data
    {
        @Param({
                "%",
                "_%",
                "%_",
                "abc%",
                "%abc",
                "_____",
                "abc%def%ghi",
                "%abc%def%",
        })
        private String pattern;

        private Slice data;
        private byte[] bytes;
        private RegexLikeMatcher joniRegexMatcher;
        private DfaLikeMatcher dfaMatcher;

        @Setup
        public void setup()
        {
            data = Slices.utf8Slice(
                    switch (pattern) {
                        case "%" -> "qeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
                        case "_%", "%_" -> "qeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
                        case "abc%" -> "abcqeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
                        case "%abc" -> "qeroighqeorhgqerhb2eriuyerqiubgierubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhetabc";
                        case "_____" -> "abcde";
                        case "abc%def%ghi" -> "abc qeroighqeorhgqerhb2eriuyerqiubgier def ubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet ghi";
                        case "%abc%def%" -> "fdnbqerbfklerqbgqjerbgkr abc qeroighqeorhgqerhb2eriuyerqiubgier def ubgleuqrbgilquebriuqebryqebrhqerhqsnajkbcowuhet";
                        default -> throw new IllegalArgumentException("Unknown pattern: " + pattern);
                    });

            dfaMatcher = DfaLikeMatcher.compile(pattern, Optional.empty());
            joniRegexMatcher = RegexLikeMatcher.compile(pattern, Optional.empty());

            bytes = data.getBytes();
        }
    }

    @Benchmark
    public LikeMatcher benchmarkJoniRegexCompile(Data data)
    {
        return RegexLikeMatcher.compile(data.pattern, Optional.empty());
    }

    @Benchmark
    public LikeMatcher benchmarkDfaCompile(Data data)
    {
        return DfaLikeMatcher.compile(data.pattern, Optional.empty());
    }

    @Benchmark
    public boolean benchmarkJoniRegexMatch(Data data)
    {
        return data.joniRegexMatcher.match(data.bytes, 0, data.bytes.length);
    }

    @Benchmark
    public boolean benchmarkDFAMatch(Data data)
    {
        return data.dfaMatcher.match(data.bytes, 0, data.bytes.length);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkLike.class.getSimpleName() + ".*")
                .resultFormat(ResultFormatType.JSON)
                .build();

        new Runner(options).run();
    }
}
