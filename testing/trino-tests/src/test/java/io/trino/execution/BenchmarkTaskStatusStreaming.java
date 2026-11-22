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
package io.trino.execution;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.jmh.Benchmarks.benchmark;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(16)
@Warmup(iterations = 1, time = 25)
@Measurement(iterations = 2, time = 12)
@Fork(2)
public class BenchmarkTaskStatusStreaming
{
    @Param({"false", "true"})
    public boolean streamingEnabled;

    private DistributedQueryRunner queryRunner;
    private Session session;

    @Setup
    public void setup()
            throws Exception
    {
        queryRunner = TpchQueryRunner.builder()
                .setWorkerCount(1)
                .addCoordinatorProperty("experimental.task-status-streaming-enabled", Boolean.toString(streamingEnabled))
                .build();
        session = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .build();
    }

    @Benchmark
    public Object oneTask()
    {
        return queryRunner.execute(session, "SELECT 1").getOnlyValue();
    }

    @Benchmark
    public Object manyTasks()
    {
        return queryRunner.execute(session,
                """
                SELECT sum(a.nationkey + b.nationkey + c.nationkey + d.nationkey)
                FROM nation a
                JOIN nation b ON a.nationkey = b.nationkey
                JOIN nation c ON b.nationkey = c.nationkey
                JOIN nation d ON c.nationkey = d.nationkey
                """).getOnlyValue();
    }

    @TearDown
    public void tearDown()
    {
        queryRunner.close();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkTaskStatusStreaming.class).run();
    }
}
