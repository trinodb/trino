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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSplit;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.execution.TestingBlockedSplitSource.BLOCKED_QUERY;
import static io.trino.execution.TestingBlockedSplitSource.installBlockedSplitSource;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 4, time = 3)
@Fork(2)
public class BenchmarkBlockedQueryScheduling
{
    @Param("100")
    public int splitWaitMillis;

    private DistributedQueryRunner queryRunner;

    @Setup
    public void setup()
            throws Exception
    {
        queryRunner = TpchQueryRunner.builder()
                .setWorkerCount(1)
                .addCoordinatorProperty("query.executor-pool-size", "1C")
                .build();
        installBlockedSplitSource(queryRunner, () -> new CompletableFuture<List<ConnectorSplit>>()
                .completeOnTimeout(ImmutableList.of(), splitWaitMillis, MILLISECONDS));
    }

    @Benchmark
    public Object waitForSplits()
    {
        return queryRunner.execute(BLOCKED_QUERY).getOnlyValue();
    }

    @TearDown
    public void tearDown()
    {
        queryRunner.close();
    }
}
