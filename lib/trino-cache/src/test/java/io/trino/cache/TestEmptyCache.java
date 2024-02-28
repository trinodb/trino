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
package io.trino.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEmptyCache
{
    private static final int TEST_TIMEOUT_SECONDS = 10;

    @Test
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testLoadFailure()
            throws Exception
    {
        Cache<Integer, String> cache = new EmptyCache<>(
                CacheLoader.from(() -> {
                    throw new UnsupportedOperationException();
                }),
                false);
        int key = 10;

        ExecutorService executor = newFixedThreadPool(2);
        try {
            Exchanger<Thread> exchanger = new Exchanger<>();
            CountDownLatch secondUnblocked = new CountDownLatch(1);

            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                boolean first = i == 0;
                futures.add(executor.submit(() -> {
                    if (!first) {
                        // Wait for the first one to start the call
                        exchanger.exchange(Thread.currentThread(), 10, SECONDS);
                        // Prove that we are back in RUNNABLE state.
                        secondUnblocked.countDown();
                    }
                    return cache.get(key, () -> {
                        if (first) {
                            Thread secondThread = exchanger.exchange(null, 10, SECONDS);
                            assertThat(secondUnblocked.await(10, SECONDS)).isTrue();
                            // Wait for the second one to hang inside the cache.get call.
                            assertEventually(() -> assertThat(secondThread.getState()).isNotEqualTo(Thread.State.RUNNABLE));
                            throw new RuntimeException("first attempt is poised to fail");
                        }
                        return "success";
                    });
                }));
            }

            List<String> results = new ArrayList<>();
            for (Future<String> future : futures) {
                try {
                    results.add(future.get());
                }
                catch (ExecutionException e) {
                    results.add(e.getCause().toString());
                }
            }

            assertThat(results).containsExactlyInAnyOrder(
                    "success",
                    "com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException: first attempt is poised to fail");
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }
}
