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
package io.prestosql.plugin.hive.metastore.cache;

import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestReentrantBoundedExecutor
{
    @Test
    public void testReentrantBoundedExecutor()
            throws ExecutionException, InterruptedException
    {
        AtomicInteger callCounter = new AtomicInteger();
        SettableFuture<Object> future = SettableFuture.create();
        Executor reentrantExecutor = new ReentrantBoundedExecutor(newCachedThreadPool(), 1);
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

        assertEquals(callCounter.get(), 2);
    }
}
