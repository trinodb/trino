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
package io.trino.json.regex;

import io.airlift.slice.Slice;
import io.trino.operator.scalar.JoniRegexpFunctions;
import io.trino.type.JoniRegexp;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * The purpose of this benchmark is to compare the performance disparity between Joni
 * and {@link XQuerySqlRegex}. This comparison is, in some scenarios, unfair because the
 * semantics of the exact same pattern may differ between these two implementations. The
 * main difference arises from how each defines a line boundary. Specifically,
 * {@link XQuerySqlRegex} includes the two-character sequence CRLF as a line boundary
 * and treats it as a single character. This may, in some cases, result in significant
 * performance overhead.
 */
@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 30, time = 500, timeUnit = MILLISECONDS)
public class BenchmarkXQuerySqlRegex
{
    public enum BenchmarkCase
    {
        SINGLE_LINE("1".repeat(100), "^\\d+$", ""),
        MULTI_LINE("%s\n%s\n%s\n%s".formatted("a".repeat(100), "b".repeat(100), "1".repeat(100), "c".repeat(100)), "^\\d+$", "m"),
        ANY_CHAR("a%sc".formatted("b".repeat(100)), "a.*c", ""),
        DOT_ALL("a%sc".formatted("b".repeat(100)), "a.*c", "s"),
        WHITESPACE_ESCAPE("\n\r".repeat(10), "[\\s]+", "");

        private final String input;
        private final String pattern;
        private final String flags;

        BenchmarkCase(String input, String pattern, String flags)
        {
            this.input = input;
            this.pattern = pattern;
            this.flags = flags;
        }

        public String pattern()
        {
            return pattern;
        }

        public String input()
        {
            return input;
        }

        public String flags()
        {
            return flags;
        }
    }

    @State(Thread)
    public static class Data
    {
        @Param
        private BenchmarkCase benchmarkCase;

        private Slice joniPattern;
        private JoniRegexp joniRegex;
        private XQuerySqlRegex xQuerySqlRegex;
        private Slice input;

        @Setup
        public void setup()
        {
            if (!benchmarkCase.flags().isEmpty()) {
                joniPattern = utf8Slice("(?%s)%s".formatted(benchmarkCase.flags(), benchmarkCase.pattern()));
            }
            else {
                joniPattern = utf8Slice(benchmarkCase.pattern());
            }

            joniRegex = joniRegexp(joniPattern);
            xQuerySqlRegex = XQuerySqlRegex.compile(benchmarkCase.pattern(), Optional.of(benchmarkCase.flags()));
            input = utf8Slice(benchmarkCase.input());
        }
    }

    @Benchmark
    public boolean matchJoni(Data data)
    {
        return JoniRegexpFunctions.regexpLike(data.input, data.joniRegex);
    }

    @Benchmark
    public boolean matchXQuerySql(Data data)
    {
        return data.xQuerySqlRegex.match(data.input);
    }

    @Benchmark
    public JoniRegexp compileJoni(Data data)
    {
        return joniRegexp(data.joniPattern);
    }

    @Benchmark
    public XQuerySqlRegex compileXQuerySql(Data data)
    {
        return XQuerySqlRegex.compile(data.benchmarkCase.pattern(), Optional.of(data.benchmarkCase.flags()));
    }

    @Test
    void testCorrectness()
    {
        Data data = new Data();
        for (BenchmarkCase benchmarkCase : BenchmarkCase.values()) {
            data.benchmarkCase = benchmarkCase;
            data.setup();
            assertThat(matchJoni(data) == matchXQuerySql(data))
                    .isTrue();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkXQuerySqlRegex.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
