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
package io.trino.testing;

import io.trino.jvm.Threads;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.stream.Collectors.joining;

public final class ThreadAssertions
{
    private ThreadAssertions() {}

    private static final AtomicInteger sequence = new AtomicInteger();

    public static void assertNoThreadLeakedInPlanTester(ThrowingSupplier<PlanTester> resourceCreator, Consumer<? super PlanTester> exerciseResource)
            throws Exception
    {
        assertNoThreadLeaked(resourceCreator, exerciseResource);
    }

    public static <T extends QueryRunner> void assertNoThreadLeakedInQueryRunner(ThrowingSupplier<T> resourceCreator, Consumer<? super T> exerciseResource)
            throws Exception
    {
        assertNoThreadLeaked(resourceCreator, exerciseResource);
    }

    private static <T extends AutoCloseable> void assertNoThreadLeaked(
            ThrowingSupplier<T> resourceCreator,
            Consumer<? super T> exerciseResource)
            throws Exception
    {
        // warm up all statically initialized threads
        try (T resource = resourceCreator.get()) {
            exerciseResource.accept(resource);
        }

        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        ThreadGroup threadGroup = new ThreadGroup("test-group-" + sequence.incrementAndGet());
        Thread.ofPlatform()
                .name("ThreadAssertions.assertNoThreadLeaked")
                .group(threadGroup)
                .start(() -> {
                    testFuture.completeAsync(() -> {
                        try (T resource = resourceCreator.get()) {
                            exerciseResource.accept(resource);
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        reportLeakedThreads(threadGroup);
                        return null;
                    }, directExecutor());
                });
        getFutureValue(testFuture);
    }

    public static void reportLeakedThreads(ThreadGroup threadGroup)
    {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        Thread[] threads = new Thread[256];
        // TODO detect leaked virtual threads -- enumerate does not return them and they live in their own thread group
        int count = threadGroup.enumerate(threads, true);
        String stackTraces = Arrays.stream(threads, 0, count)
                .filter(thread -> thread != Thread.currentThread())
                // ATQF
                .filter(thread -> !thread.getName().startsWith("AbstractTestQueryFramework-"))
                // io.trino.tpch.TextPool.EXECUTOR
                .filter(thread -> !thread.getName().startsWith("io.trino.tpch.TextPool-"))
                // io.trino.hdfs.FileSystemFinalizerService.finalizerThread
                .filter(thread -> !thread.getName().equals("FileSystemFinalizerService"))
                // org.apache.hadoop.fs.FileSystem.Statistics.STATS_DATA_CLEANER
                .filter(thread -> !thread.getName().equals("org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner"))
                // Common ForkJoinPool threads are statically managed, not considered a leak
                .filter(thread -> !thread.getName().startsWith("ForkJoinPool.commonPool-worker-"))
                // java.util.concurrent.CompletableFuture.Delayer#delayer
                .filter(thread -> !thread.getName().equals("CompletableFutureDelayScheduler"))
                // okhttp3.internal.concurrent.TaskRunner
                .filter(thread -> !thread.getName().equals("OkHttp TaskRunner"))
                // dev.failsafe.internal.util.DelegatingScheduler#DELAYER
                .filter(thread -> !thread.getName().equals("FailsafeDelayScheduler"))
                // RyukResourceReaper
                .filter(thread -> !thread.getName().equals("testcontainers-ryuk"))
                // org.testcontainers.containers.wait.strategy.AbstractWaitStrategy.EXECUTOR
                .filter(thread -> !thread.getName().startsWith("testcontainers-wait-"))
                // org.testcontainers.images.TimeLimitedLoggedPullImageResultCallback.PROGRESS_WATCHDOG_EXECUTOR
                .filter(thread -> !thread.getName().startsWith("testcontainers-pull-watchdog-"))
                // org.testcontainers.lifecycle.Startables#EXECUTOR
                .filter(thread -> !thread.getName().startsWith("testcontainers-lifecycle-"))
                // org.rnorth.ducttape.timeouts.Timeouts.EXECUTOR_SERVICE
                .filter(thread -> !thread.getName().startsWith("ducttape-"))
                // Potentially leaked by org.testcontainers.shaded.com.github.dockerjava.core.DefaultInvocationBuilder#executeAndStream -- test-only problem
                .filter(thread -> !thread.getName().startsWith("docker-java-stream-"))
                // com.amazonaws.http.IdleConnectionReaper
                .filter(thread -> !thread.getName().equals("java-sdk-http-connection-reaper"))
                // software.amazon.awssdk.http.apache.internal.conn.IdleConnectionReaper
                .filter(thread -> !thread.getName().equals("idle-connection-reaper"))
                // io.netty.util.concurrent.GlobalEventExecutor.INSTANCE
                .filter(thread -> !thread.getName().startsWith("globalEventExecutor-"))
                // io.grpc.netty.Utils.NIO_WORKER_EVENT_LOOP_GROUP
                .filter(thread -> !thread.getName().startsWith("grpc-nio-worker-ELG-"))
                // io.grpc.netty.Utils#DEFAULT_WORKER_EVENT_LOOP_GROUP
                .filter(thread -> !thread.getName().startsWith("grpc-default-worker-ELG-"))
                // io.grpc.internal.ManagedChannelImplBuilder#DEFAULT_EXECUTOR_POOL // TODO not sure if this is a leak or not
                .filter(thread -> !thread.getName().startsWith("grpc-default-executor-"))
                // io.grpc.internal.SharedResourceHolder#holder
                .filter(thread -> !thread.getName().startsWith("grpc-shared-destroyer-"))
                // com.mysql.cj.jdbc.AbandonedConnectionCleanupThread.cleanupThreadExecutorService
                .filter(thread -> !thread.getName().equals("mysql-cj-abandoned-connection-cleanup"))
                // org.postgresql.util.LazyCleaner
                .filter(thread -> !thread.getName().equals("PostgreSQL-JDBC-Cleaner"))
                // Oracle JDBC driver and UPC
                .filter(thread -> switch (thread.getName()) {
                    case "UCP Clock", "oracle.jdbc.diagnostics.Diagnostic.CLOCK", "oracle.jdbc.driver.BlockSource.ThreadedCachingBlockSource.BlockReleaser", "InterruptTimer",
                         "oracle.ucp.actors.InterruptableActor-control" -> false;
                    default -> !thread.getName().startsWith("UCP-ScheduledThreadPoolExecutor-") && !thread.getName().startsWith("UCP-worker-thread-");
                })
                // com.clickhouse.data.ClickHouseDataStreamFactory.DefaultExecutors
                .filter(thread -> !thread.getName().startsWith("ClickHouseWorker-") && !thread.getName().startsWith("ClickHouseScheduler-"))
                // These are likely to be statically managed, not considered a leak. We probably wouldn't see them if we had a dry run
                // But that's not feasible in ATQF's case
                .filter(thread -> !thread.getName().equals("JNA Cleaner"))
                .filter(thread -> !thread.getName().equals("Okio Watchdog"))
                // False positive? Bound by DiscoveryModule; io.airlift.http.client.jetty.JettyHttpClient.close should close it. TODO JettyHttpClient.close could abstain from swallowing exceptions
                .filter(thread -> !thread.getName().startsWith("http-client-discovery-scheduler-"))
                .map(thread -> threadMXBean.getThreadInfo(thread.threadId(), Integer.MAX_VALUE))
                .filter(Objects::nonNull) // could be virtual, or exit concurrently
                .map(Threads::fullToString)
                .collect(joining("\n"));

        if (!stackTraces.isEmpty()) {
            throw new AssertionError("Threads leaked:\n" + stackTraces);
        }
    }

    public interface ThrowingSupplier<T>
    {
        T get()
                throws Exception;
    }
}
