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

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        ThreadGroup threadGroup = new ThreadGroup("test-group-" + sequence.incrementAndGet());
        Thread.ofPlatform().group(threadGroup).start(() -> {
            testFuture.completeAsync(() -> {
                try (T resource = resourceCreator.get()) {
                    exerciseResource.accept(resource);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                Thread[] threads = new Thread[256];
                // TODO detect leaked virtual threads -- enumerate does not return them and they live in their own thread group
                int count = threadGroup.enumerate(threads, true);
                String stackTraces = Arrays.stream(threads, 0, count)
                        .filter(thread -> thread != Thread.currentThread())
                        // Common ForkJoinPool threads are statically managed, not considered a leak
                        .filter(thread -> !thread.getName().startsWith("ForkJoinPool.commonPool-worker-"))
                        // OkHttp TaskRunner is statically managed (okhttp3.internal.concurrent.TaskRunner), not considered a leak
                        .filter(thread -> !thread.getName().equals("OkHttp TaskRunner"))
                        .map(thread -> threadMXBean.getThreadInfo(thread.threadId(), Integer.MAX_VALUE))
                        .filter(Objects::nonNull) // could be virtual, or exit concurrently
                        .map(Threads::fullToString)
                        .collect(joining("\n"));

                if (!stackTraces.isEmpty()) {
                    throw new AssertionError("Threads leaked:\n" + stackTraces);
                }

                return null;
            }, directExecutor());
        });
        getFutureValue(testFuture);
    }

    public interface ThrowingSupplier<T>
    {
        T get()
                throws Exception;
    }
}
