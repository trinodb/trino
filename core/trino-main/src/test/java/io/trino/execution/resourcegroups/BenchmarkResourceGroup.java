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
package io.trino.execution.resourcegroups;

import io.airlift.units.DataSize;
import io.trino.execution.MockManagedQueryExecution;
import io.trino.execution.MockManagedQueryExecution.MockManagedQueryExecutionBuilder;
import io.trino.jmh.Benchmarks;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkResourceGroup
{
    @Benchmark
    public Object benchmark(BenchmarkData data)
    {
        data.getRoot().updateGroupsAndProcessQueuedQueries();
        return data.getRoot();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1000", "10000", "100000"})
        private int children = 1000;

        @Param({"100", "1000", "10000"})
        private int queries = 100;

        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private InternalResourceGroup root;

        @Setup
        public void setup()
        {
            root = new InternalResourceGroup("root", (group, export) -> {}, executor);
            root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
            root.setMaxQueuedQueries(queries);
            root.setHardConcurrencyLimit(queries);
            InternalResourceGroup group = root;
            for (int i = 0; i < children; i++) {
                group = root.getOrCreateSubGroup(String.valueOf(i));
                group.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
                group.setMaxQueuedQueries(queries);
                group.setHardConcurrencyLimit(queries);
            }
            for (int i = 0; i < queries; i++) {
                MockManagedQueryExecution query = new MockManagedQueryExecutionBuilder()
                        .withInitialMemoryUsage(DataSize.ofBytes(10))
                        .build();
                group.run(query);
            }
        }

        @TearDown
        public void tearDown()
        {
            executor.shutdownNow();
        }

        public InternalResourceGroup getRoot()
        {
            return root;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkResourceGroup.class).run();
    }
}
