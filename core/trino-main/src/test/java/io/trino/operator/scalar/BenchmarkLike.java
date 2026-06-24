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
import io.trino.likematcher.LikeMatcher;
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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.util.Failures.checkCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 30, time = 500, timeUnit = MILLISECONDS)
public class BenchmarkLike
{
    private static final String LONG_STRING = "a".repeat(100) +
            "b".repeat(100) +
            "a".repeat(100) +
            "b".repeat(100) +
            "the quick brown fox jumps over the lazy dog";

    public enum BenchmarkCase
    {
        ANY("%", LONG_STRING),
        WILDCARD_PREFIX("_%", LONG_STRING),
        WILDCARD_SUFFIX("%_", LONG_STRING),
        PREFIX("the%", "the quick brown fox jumps over the lazy dog"),
        SUFFIX("%dog", "the quick brown fox jumps over the lazy dog"),
        FIXED_WILDCARD("_____", "abcdef"),
        SHORT_TOKENS_1("%a%b%a%b%", LONG_STRING),
        SHORT_TOKENS_2("%the%quick%brown%fox%jumps%over%the%lazy%dog%", LONG_STRING),
        SHORT_TOKEN("%the%", LONG_STRING),
        LONG_TOKENS_1("%aaaaaaaaab%bbbbbbbbba%aaaaaaaaab%bbbbbbbbbt%", LONG_STRING),
        LONG_TOKENS_2("%aaaaaaaaaaaaaaaaaaaaaaaaaa%aaaaaaaaaaaaaaaaaaaaaaaaaathe%", LONG_STRING),
        LONG_TOKEN_1("%bbbbbbbbbbbbbbbthe%", LONG_STRING),
        LONG_TOKEN_2("%the quick brown fox%", LONG_STRING),
        LONG_TOKEN_3("%aaaaaaaxaaaaaa%", LONG_STRING),
        SHORT_TOKENS_WITH_LONG_SKIP("%the%dog%", LONG_STRING);

        private final String pattern;
        private final String text;

        BenchmarkCase(String pattern, String text)
        {
            this.pattern = pattern;
            this.text = text;
        }

        public String pattern()
        {
            return pattern;
        }

        public String text()
        {
            return text;
        }
    }

    @State(Thread)
    public static class Data
    {
        @Param
        private BenchmarkCase benchmarkCase;

        private Slice data;
        private byte[] bytes;
        private SafeReRegexp regexPattern;
        private LikeMatcher optimizedMatcher;
        private LikeMatcher nonOptimizedMatcher;

        @Setup
        public void setup()
        {
            optimizedMatcher = LikeMatcher.compile(benchmarkCase.pattern(), Optional.empty(), true);
            nonOptimizedMatcher = LikeMatcher.compile(benchmarkCase.pattern(), Optional.empty(), false);
            regexPattern = compileRegex(benchmarkCase.pattern(), '0', false);

            bytes = benchmarkCase.text().getBytes(UTF_8);
            data = Slices.wrappedBuffer(bytes);
        }
    }

    @Benchmark
    public boolean matchRegex(Data data)
    {
        return SafeReRegexpFunctions.regexpLike(data.data, data.regexPattern);
    }

    @Benchmark
    public boolean matchOptimized(Data data)
    {
        return data.optimizedMatcher.match(data.bytes, 0, data.bytes.length);
    }

    @Benchmark
    public boolean matchNonOptimized(Data data)
    {
        return data.nonOptimizedMatcher.match(data.bytes, 0, data.bytes.length);
    }

    @Benchmark
    public SafeReRegexp compileRegex(Data data)
    {
        return compileRegex(data.benchmarkCase.pattern(), (char) 0, false);
    }

    @Benchmark
    public LikeMatcher compileOptimized(Data data)
    {
        return LikeMatcher.compile(data.benchmarkCase.pattern(), Optional.empty(), true);
    }

    @Benchmark
    public LikeMatcher compileNonOptimized(Data data)
    {
        return LikeMatcher.compile(data.benchmarkCase.pattern(), Optional.empty(), false);
    }

    @Benchmark
    public boolean dynamicRegex(Data data)
    {
        return SafeReRegexpFunctions.regexpLike(data.data, compileRegex(Slices.utf8Slice(data.benchmarkCase.pattern()).toStringUtf8(), '0', false));
    }

    @Benchmark
    public boolean dynamicOptimized(Data data)
    {
        return LikeMatcher.compile(data.benchmarkCase.pattern(), Optional.empty(), true)
                .match(data.bytes, 0, data.bytes.length);
    }

    @Benchmark
    public boolean dynamicNonOptimized(Data data)
    {
        return LikeMatcher.compile(data.benchmarkCase.pattern(), Optional.empty(), false)
                .match(data.bytes, 0, data.bytes.length);
    }

    private static SafeReRegexp compileRegex(String patternString, char escapeChar, boolean shouldEscape)
    {
        return new SafeReRegexp(Slices.utf8Slice(likeToRegex(patternString, escapeChar, shouldEscape)));
    }

    private static String likeToRegex(String patternString, char escapeChar, boolean shouldEscape)
    {
        StringBuilder regex = new StringBuilder(patternString.length() * 2);

        regex.append('^');
        boolean escaped = false;
        for (char currentChar : patternString.toCharArray()) {
            checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar);
            if (shouldEscape && !escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else {
                switch (currentChar) {
                    case '%' -> {
                        regex.append(escaped ? "%" : ".*");
                        escaped = false;
                    }
                    case '_' -> {
                        regex.append(escaped ? "_" : ".");
                        escaped = false;
                    }
                    default -> {
                        // escape special regex characters
                        switch (currentChar) {
                            case '\\', '^', '$', '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|' -> regex.append('\\');
                        }
                        regex.append(currentChar);
                        escaped = false;
                    }
                }
            }
        }
        checkEscape(!escaped);
        regex.append('$');
        return regex.toString();
    }

    private static void checkEscape(boolean condition)
    {
        checkCondition(condition, INVALID_FUNCTION_ARGUMENT, "Escape character must be followed by '%%', '_' or the escape character itself");
    }

    static void main()
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
