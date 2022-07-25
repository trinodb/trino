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
package io.trino.collect.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEmptyCache
{
    private static final int TEST_TIMEOUT_MILLIS = 10_000;

    @Test(timeOut = TEST_TIMEOUT_MILLIS)
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
            AtomicBoolean first = new AtomicBoolean(true);
            CyclicBarrier barrier = new CyclicBarrier(2);

            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                futures.add(executor.submit(() -> {
                    barrier.await(10, SECONDS);
                    return cache.get(key, () -> {
                        if (first.compareAndSet(true, false)) {
                            // first
                            Thread.sleep(1); // increase chances that second thread calls cache.get before we return
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
            executor.awaitTermination(10, SECONDS);
        }
    }
}
