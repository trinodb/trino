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
package io.trino.util;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkParseDate
{
    @Benchmark
    public void parseDate(BenchmarkData data, Blackhole blackhole)
    {
        for (String dt : data.dates) {
            blackhole.consume(DateTimeUtils.parseDate(dt));
        }
    }

    @State(Thread)
    public static class BenchmarkData
    {
        String[] dates;

        @Setup
        public void setup()
        {
            dates = new String[100];
            // use the 100 consecutive dates start from 2023-01-01
            String startDate = "2023-01-01";
            int days = DateTimeUtils.parseDate(startDate);
            for (int i = 0; i < dates.length; i++) {
                dates[i] = DateTimeUtils.printDate(days + i);
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkParseDate.class).run();
    }
}
