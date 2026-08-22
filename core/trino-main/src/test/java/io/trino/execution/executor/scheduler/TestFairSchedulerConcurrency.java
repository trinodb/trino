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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/// Stresses the direct-handoff admission path: every slot-freeing and work-creating edge must call
/// `schedule()`, and concurrent callers must not lose a wakeup or leak a slot.
public class TestFairSchedulerConcurrency
{
    private static final int SLOTS = 4;

    @Test
    @Timeout(30)
    public void testManyShortTasksHandOffCleanly()
            throws ExecutionException, InterruptedException
    {
        int groups = 10;
        int tasksPerGroup = 200;
        try (FairScheduler scheduler = FairScheduler.newInstance(SLOTS)) {
            AtomicInteger ran = new AtomicInteger();
            List<ListenableFuture<Void>> futures = new ArrayList<>();

            for (int g = 0; g < groups; g++) {
                Group group = scheduler.createGroup("G" + g);
                for (int i = 0; i < tasksPerGroup; i++) {
                    futures.add(scheduler.submit(group, i, _ -> ran.incrementAndGet()));
                }
            }

            Futures.allAsList(futures).get();

            assertThat(ran.get()).isEqualTo(groups * tasksPerGroup);
            // No slot leaked: everything drained back to full availability.
            assertThat(scheduler.getConcurrencyControlAvailableSlots()).isEqualTo(SLOTS);
        }
    }

    @Test
    @Timeout(30)
    public void testBlockUnblockChurnDoesNotLoseWakeups()
            throws ExecutionException, InterruptedException
    {
        int groups = 6;
        int tasksPerGroup = 60;
        int blocksPerTask = 4;
        try (FairScheduler scheduler = FairScheduler.newInstance(SLOTS)) {
            ConcurrentLinkedQueue<SettableFuture<Void>> pending = new ConcurrentLinkedQueue<>();
            AtomicInteger completed = new AtomicInteger();
            List<ListenableFuture<Void>> futures = new ArrayList<>();

            // Background completer: repeatedly unblocks whatever tasks are currently parked,
            // maximizing slot free/refill churn through schedule().
            AtomicInteger running = new AtomicInteger(1);
            Thread completer = new Thread(() -> {
                while (running.get() > 0) {
                    SettableFuture<Void> future = pending.poll();
                    if (future != null) {
                        future.set(null);
                    }
                }
                // Drain anything left behind after the flag flips.
                SettableFuture<Void> future;
                while ((future = pending.poll()) != null) {
                    future.set(null);
                }
            });
            completer.setDaemon(true);
            completer.start();

            for (int g = 0; g < groups; g++) {
                Group group = scheduler.createGroup("G" + g);
                for (int i = 0; i < tasksPerGroup; i++) {
                    futures.add(scheduler.submit(group, i, context -> {
                        for (int b = 0; b < blocksPerTask; b++) {
                            SettableFuture<Void> future = SettableFuture.create();
                            pending.add(future);
                            if (!context.block(future)) {
                                return;
                            }
                        }
                        completed.incrementAndGet();
                    }));
                }
            }

            Futures.allAsList(futures).get();
            running.set(0);
            completer.join();

            assertThat(completed.get()).isEqualTo(groups * tasksPerGroup);
            assertThat(scheduler.getConcurrencyControlAvailableSlots()).isEqualTo(SLOTS);
        }
    }
}
