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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.operator.scalar.JsonOperators;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
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
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static java.lang.Integer.parseInt;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkJsonOperators
{
    private static final String UNBOUNDED_LENGTH = "2147483647";

    @Benchmark
    public Slice benchmarkCastToVarchar(BenchmarkData data)
    {
        return JsonOperators.castToVarchar(data.varcharLength, data.jsonSlice);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param
        private JsonType jsonType;

        @Param({"10000", UNBOUNDED_LENGTH})
        private long varcharLength = -1 /* invalid value */;

        private Slice jsonSlice;

        @Setup
        public void setup()
        {
            String jsonString = switch (jsonType) {
                case STRING_SHORT -> "\"hello world\"";
                case STRING_MEDIUM -> "\"" + "The quick brown fox jumps over the lazy dog. ".repeat(5) + "\"";
                case STRING_LONG -> "\"" + "abcdefghijklmnopqrstuvwxyz0123456789".repeat(50) + "\"";
                case STRING_WITH_UNICODE -> "\"Hello \\u4e16\\u754c \\ud83d\\ude00 \\u03b1\\u03b2\\u03b3\"";
                case NUMBER_INTEGER -> "123456789";
                case NUMBER_DECIMAL -> "123456.789012";
                case NUMBER_SCIENTIFIC -> "1.23456789E8";
                case BOOLEAN_TRUE -> "true";
                case BOOLEAN_FALSE -> "false";
                case NULL -> "null";
            };
            jsonSlice = utf8Slice(jsonString);
        }
    }

    public enum JsonType
    {
        STRING_SHORT,
        STRING_MEDIUM,
        STRING_LONG,
        STRING_WITH_UNICODE,
        NUMBER_INTEGER,
        NUMBER_DECIMAL,
        NUMBER_SCIENTIFIC,
        BOOLEAN_TRUE,
        BOOLEAN_FALSE,
        NULL
    }

    @Test
    public void verify()
    {
        assertThat(parseInt(UNBOUNDED_LENGTH)).isEqualTo(VarcharType.UNBOUNDED_LENGTH);

        BenchmarkData data = new BenchmarkData();
        for (JsonType type : JsonType.values()) {
            data.jsonType = type;
            data.varcharLength = 10000;
            data.setup();
            new BenchmarkJsonOperators().benchmarkCastToVarchar(data);
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkJsonOperators.class).run();
    }
}
