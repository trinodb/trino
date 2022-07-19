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
package io.trino.hdfs;

import io.trino.jmh.Benchmarks;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 5000, timeUnit = MILLISECONDS)
@Fork(2)
@Measurement(iterations = 5, time = 5000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGetFileSystem
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"10", "100", "1000"})
        private int numUsers;

        @Param({"1", "16"})
        private int numThreads;

        @Param("1000")
        private int numGetCallsPerInvocation;

        public List<Callable<Void>> callableTasks;
        public ExecutorService executor;
        public Blackhole blackhole;

        @Setup(Level.Invocation)
        public void setUp(Blackhole blackhole)
        {
            this.blackhole = blackhole;

            this.callableTasks = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                this.callableTasks.add(new TestFileSystemCache.CreateFileSystemAndConsume(
                        new SplittableRandom(i), numUsers, numGetCallsPerInvocation, fs -> {}));
            }

            this.executor = Executors.newFixedThreadPool(numThreads);
        }

        @TearDown(Level.Invocation)
        public void tearDown()
                throws IOException
        {
            TrinoFileSystemCache.INSTANCE.closeAll();
            executor.shutdown();
        }
    }

    @Benchmark
    public void benchmark(BenchmarkData data)
            throws InterruptedException, ExecutionException
    {
        List<Future<Void>> futures = data.executor.invokeAll(data.callableTasks);
        for (Future<Void> fut : futures) {
            data.blackhole.consume(fut.get());
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkGetFileSystem.class).run();
    }
}
