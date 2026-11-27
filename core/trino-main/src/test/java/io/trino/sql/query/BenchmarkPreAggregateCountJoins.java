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
package io.trino.sql.query;

import io.trino.Session;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.PRE_AGGREGATE_COUNT_JOINS;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * End-to-end benchmark, including parsing/planning, with exact VALUES statistics.
 */
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkPreAggregateCountJoins
{
    @Param({"false", "true"})
    public boolean enabled;

    @Param({"expanding", "unique"})
    public String shape;

    private StandaloneQueryRunner runner;
    private Session session;
    private String query;

    @Setup
    public void setup()
    {
        session = testSessionBuilder()
                .setSystemProperty(PRE_AGGREGATE_COUNT_JOINS, Boolean.toString(enabled))
                .build();
        runner = new StandaloneQueryRunner(session);
        String rows = IntStream.range(0, 8000)
                .mapToObj(i -> "(%s, %s)".formatted(shape.equals("expanding") ? i % 4 : i, i % 3))
                .collect(joining(","));
        query = "SELECT l.a, r.b, count(*) FROM (VALUES " + rows + ") l(k,a) JOIN (VALUES " + rows + ") r(k,b) ON l.k=r.k GROUP BY l.a,r.b";
        var result = runner.executeWithPlan(session, query);
        checkState(TestPreAggregateCountJoinsExecution.hasCountJoin(result.queryPlan().orElseThrow().getRoot()) == (enabled && shape.equals("expanding")), "Unexpected benchmark plan");
    }

    @TearDown
    public void close()
    {
        runner.close();
    }

    @Benchmark
    public MaterializedResult execute()
    {
        return runner.execute(session, query);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        if (args.length == 1 && args[0].equals("plans")) {
            for (String shape : new String[] {"expanding", "unique"}) {
                for (boolean enabled : new boolean[] {false, true}) {
                    BenchmarkPreAggregateCountJoins benchmark = new BenchmarkPreAggregateCountJoins();
                    benchmark.shape = shape;
                    benchmark.enabled = enabled;
                    benchmark.setup();
                    try {
                        System.out.println("shape=" + shape + " enabled=" + enabled);
                        System.out.println(benchmark.runner.execute(benchmark.session, "EXPLAIN ANALYZE " + benchmark.query).getOnlyValue());
                    }
                    finally {
                        benchmark.close();
                    }
                }
            }
            return;
        }
        benchmark(BenchmarkPreAggregateCountJoins.class).run();
    }
}
