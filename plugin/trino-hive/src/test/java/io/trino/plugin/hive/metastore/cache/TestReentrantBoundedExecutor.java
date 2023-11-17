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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReentrantBoundedExecutor
{
    @Test
    public void testReentrantBoundedExecutor()
            throws ExecutionException, InterruptedException
    {
        AtomicInteger callCounter = new AtomicInteger();
        SettableFuture<Object> future = SettableFuture.create();
        ExecutorService executor = newCachedThreadPool();
        try {
            Executor reentrantExecutor = new ReentrantBoundedExecutor(executor, 1);
            reentrantExecutor.execute(() -> {
                callCounter.incrementAndGet();
                reentrantExecutor.execute(() -> {
                    callCounter.incrementAndGet();
                    future.set(null);
                });
                try {
                    future.get();
                }
                catch (Exception ignored) {
                }
            });
            future.get();

            SettableFuture<Object> secondFuture = SettableFuture.create();
            reentrantExecutor.execute(() -> secondFuture.set(null));
            secondFuture.get();

            assertThat(callCounter.get()).isEqualTo(2);
        }
        finally {
            executor.shutdownNow();
        }
    }
}
