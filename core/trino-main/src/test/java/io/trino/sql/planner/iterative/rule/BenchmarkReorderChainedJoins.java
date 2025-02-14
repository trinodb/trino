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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.Plan;
import io.trino.testing.PlanTester;
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

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkReorderChainedJoins
{
    @Benchmark
    public Plan benchmarkReorderJoins(BenchmarkInfo benchmarkInfo)
    {
        PlanTester planTester = benchmarkInfo.getPlanTester();
        return planTester.inTransaction(transactionSession -> planTester.createPlan(
                transactionSession,
                """
                SELECT *
                FROM nation n1
                JOIN nation n2 ON n1.nationkey = n2.nationkey
                JOIN nation n3 ON n2.comment = n3.comment
                JOIN nation n4 ON n3.name = n4.name
                JOIN region r1 ON n4.regionkey = r1.regionkey
                JOIN region r2 ON r1.name = r2.name
                JOIN region r3 ON r3.comment = r2.comment
                JOIN region r4 ON r4.regionkey = r3.regionkey
                """));
    }

    @State(Thread)
    public static class BenchmarkInfo
    {
        @Param({"ELIMINATE_CROSS_JOINS", "AUTOMATIC"})
        private String joinReorderingStrategy;

        private PlanTester planTester;

        @Setup
        public void setup()
        {
            Session session = testSessionBuilder()
                    .setSystemProperty("join_reordering_strategy", joinReorderingStrategy)
                    .setSystemProperty("join_distribution_type", "AUTOMATIC")
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            planTester = PlanTester.create(session);
            planTester.installPlugin(new TpchPlugin());
            planTester.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        }

        public PlanTester getPlanTester()
        {
            return planTester;
        }

        @TearDown
        public void tearDown()
        {
            planTester.close();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkReorderChainedJoins.class).run();
    }
}
