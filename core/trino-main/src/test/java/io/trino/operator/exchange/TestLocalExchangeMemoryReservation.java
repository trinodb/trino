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
package io.trino.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.TaskContext;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLocalExchangeMemoryReservation
{
    private final ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterAll
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMemoryManagersReportInSteps()
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        LocalExchangeMemoryReservation reservation = new LocalExchangeMemoryReservation(memoryContext.newLocalMemoryContext("test"));
        LocalExchangeMemoryManager first = new LocalExchangeMemoryManager(1000, reservation, 50);
        LocalExchangeMemoryManager second = new LocalExchangeMemoryManager(1000, reservation, 50);

        first.updateMemoryUsage(10);
        assertThat(memoryContext.getBytes()).isEqualTo(60);

        first.updateMemoryUsage(40);
        assertThat(memoryContext.getBytes()).isEqualTo(60);

        first.updateMemoryUsage(11);
        assertThat(memoryContext.getBytes()).isEqualTo(111);

        second.updateMemoryUsage(150);
        assertThat(memoryContext.getBytes()).isEqualTo(311);

        // dropping within two steps of the reported bytes is not reported
        second.updateMemoryUsage(-50);
        assertThat(memoryContext.getBytes()).isEqualTo(311);

        second.updateMemoryUsage(-50);
        assertThat(memoryContext.getBytes()).isEqualTo(211);
    }

    @Test
    public void testCloseReleasesReservation()
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        LocalExchangeMemoryReservation reservation = new LocalExchangeMemoryReservation(memoryContext.newLocalMemoryContext("test"));

        reservation.updateMemoryUsage(50);
        assertThat(memoryContext.getBytes()).isEqualTo(50);

        reservation.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        reservation.updateMemoryUsage(500);
        reservation.updateMemoryUsage(-550);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testMemoryFutureWaitsForMemoryPool()
    {
        TaskContext taskContext = TestingTaskContext.builder(directExecutor(), scheduledExecutor, testSessionBuilder().build())
                .setMemoryPoolSize(DataSize.ofBytes(1000))
                .build();
        LocalMemoryContext otherMemoryContext = taskContext.aggregateUserMemoryContext().newLocalMemoryContext("other");
        LocalExchangeMemoryReservation reservation = new LocalExchangeMemoryReservation(taskContext.aggregateUserMemoryContext().newLocalMemoryContext("test"));

        otherMemoryContext.setBytes(900);
        assertThat(reservation.getMemoryFuture().isDone()).isTrue();

        reservation.updateMemoryUsage(250);
        ListenableFuture<Void> memoryFuture = reservation.getMemoryFuture();
        assertThat(memoryFuture.isDone()).isFalse();

        // shrinking the reservation keeps the pending future while the pool is still full
        reservation.updateMemoryUsage(-100);
        assertThat(taskContext.getQueryContext().getMemoryPool().getReservedBytes()).isEqualTo(1050);
        assertThat(reservation.getMemoryFuture().isDone()).isFalse();

        otherMemoryContext.setBytes(0);
        assertThat(memoryFuture.isDone()).isTrue();
        assertThat(reservation.getMemoryFuture().isDone()).isTrue();

        reservation.close();
        assertThat(taskContext.getQueryContext().getMemoryPool().getReservedBytes()).isEqualTo(0);
    }
}
