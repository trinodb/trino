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
package io.trino.execution.executor.scheduler;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFairSchedulerNesting
{
    @Test
    @Timeout(30)
    public void testTasksUnderNestedGroupsComplete()
            throws ExecutionException, InterruptedException
    {
        try (FairScheduler scheduler = FairScheduler.newInstance(4)) {
            Group tenant = scheduler.createGroup("tenant");
            Group queryA = scheduler.createGroup(tenant, "queryA");
            Group queryB = scheduler.createGroup(tenant, "queryB");

            AtomicInteger ran = new AtomicInteger();
            List<ListenableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                futures.add(scheduler.submit(queryA, i, _ -> ran.incrementAndGet()));
                futures.add(scheduler.submit(queryB, i, _ -> ran.incrementAndGet()));
            }

            Futures.allAsList(futures).get();

            assertThat(ran.get()).isEqualTo(200);
            assertThat(scheduler.getConcurrencyControlAvailableSlots()).isEqualTo(4);
        }
    }

    @Test
    @Timeout(30)
    public void testRemovingAParentCancelsTasksInNestedGroups()
            throws ExecutionException, InterruptedException
    {
        try (FairScheduler scheduler = FairScheduler.newInstance(4)) {
            Group tenant = scheduler.createGroup("tenant");
            Group query = scheduler.createGroup(tenant, "query");

            CountDownLatch blocked = new CountDownLatch(3);
            SettableFuture<Void> never = SettableFuture.create();
            List<ListenableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                futures.add(scheduler.submit(query, i, context -> {
                    blocked.countDown();
                    context.block(never); // parks until cancelled
                }));
            }
            blocked.await();

            // Removing the tenant must tear down the nested query group and cancel its tasks.
            scheduler.removeGroup(tenant);

            Futures.allAsList(futures).get();
            assertThat(scheduler.getConcurrencyControlAvailableSlots()).isEqualTo(4);
            assertThat(scheduler.getTasks(query)).isEmpty();
        }
    }
}
