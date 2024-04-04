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
package io.trino.memory.context;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMemoryContexts
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();
    private static final long GUARANTEED_MEMORY = DataSize.of(1, MEGABYTE).toBytes();

    @Test
    public void testLocalMemoryContextClose()
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext aggregateContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");
        localContext.setBytes(100);

        assertThat(localContext.getBytes()).isEqualTo(100);
        assertThat(aggregateContext.getBytes()).isEqualTo(100);
        assertThat(reservationHandler.getReservation()).isEqualTo(100);

        localContext.close();
        assertThat(localContext.getBytes()).isEqualTo(0);
        assertThat(aggregateContext.getBytes()).isEqualTo(0);
        assertThat(reservationHandler.getReservation()).isEqualTo(0);
    }

    @Test
    public void testMemoryContexts()
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext aggregateContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");

        assertThat(localContext.setBytes(10)).isEqualTo(NOT_BLOCKED);
        assertThat(localContext.getBytes()).isEqualTo(10);
        assertThat(aggregateContext.getBytes()).isEqualTo(10);
        assertThat(reservationHandler.getReservation()).isEqualTo(aggregateContext.getBytes());

        LocalMemoryContext secondLocalContext = aggregateContext.newLocalMemoryContext("test");
        assertThat(secondLocalContext.setBytes(20)).isEqualTo(NOT_BLOCKED);
        assertThat(secondLocalContext.getBytes()).isEqualTo(20);
        assertThat(aggregateContext.getBytes()).isEqualTo(30);
        assertThat(localContext.getBytes()).isEqualTo(10);
        assertThat(reservationHandler.getReservation()).isEqualTo(aggregateContext.getBytes());

        aggregateContext.close();

        assertThat(aggregateContext.getBytes()).isEqualTo(0);
        assertThat(reservationHandler.getReservation()).isEqualTo(0);
    }

    @Test
    public void testTryReserve()
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext parentContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        AggregatedMemoryContext aggregateContext1 = parentContext.newAggregatedMemoryContext();
        AggregatedMemoryContext aggregateContext2 = parentContext.newAggregatedMemoryContext();
        LocalMemoryContext childContext1 = aggregateContext1.newLocalMemoryContext("test");

        assertThat(childContext1.trySetBytes(500)).isTrue();
        assertThat(childContext1.trySetBytes(1_000)).isTrue();
        assertThat(childContext1.trySetBytes(1_001)).isFalse();
        assertThat(reservationHandler.getReservation()).isEqualTo(parentContext.getBytes());

        aggregateContext1.close();
        aggregateContext2.close();
        parentContext.close();

        assertThat(aggregateContext1.getBytes()).isEqualTo(0);
        assertThat(aggregateContext2.getBytes()).isEqualTo(0);
        assertThat(parentContext.getBytes()).isEqualTo(0);
        assertThat(reservationHandler.getReservation()).isEqualTo(0);
    }

    @Test
    public void testHierarchicalMemoryContexts()
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext parentContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        AggregatedMemoryContext aggregateContext1 = parentContext.newAggregatedMemoryContext();
        AggregatedMemoryContext aggregateContext2 = parentContext.newAggregatedMemoryContext();
        LocalMemoryContext childContext1 = aggregateContext1.newLocalMemoryContext("test");
        LocalMemoryContext childContext2 = aggregateContext2.newLocalMemoryContext("test");

        assertThat(childContext1.setBytes(1)).isEqualTo(NOT_BLOCKED);
        assertThat(childContext2.setBytes(1)).isEqualTo(NOT_BLOCKED);

        assertThat(aggregateContext1.getBytes()).isEqualTo(1);
        assertThat(aggregateContext2.getBytes()).isEqualTo(1);
        assertThat(parentContext.getBytes()).isEqualTo(aggregateContext1.getBytes() + aggregateContext2.getBytes());
        assertThat(reservationHandler.getReservation()).isEqualTo(parentContext.getBytes());
    }

    @Test
    public void testGuaranteedMemoryDoesntBlock()
    {
        long maxMemory = 2 * GUARANTEED_MEMORY;
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(maxMemory);
        AggregatedMemoryContext parentContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        LocalMemoryContext childContext = parentContext.newLocalMemoryContext("test");

        // exhaust the max memory available
        reservationHandler.exhaustMemory();
        assertThat(reservationHandler.getReservation()).isEqualTo(maxMemory);

        // even if the pool is exhausted we never block queries using a trivial amount of memory
        assertThat(childContext.setBytes(1_000)).isEqualTo(NOT_BLOCKED);
        assertThat(childContext.setBytes(GUARANTEED_MEMORY + 1))
                .isNotEqualTo(NOT_BLOCKED);

        // at this point the memory contexts have reserved GUARANTEED_MEMORY + 1 bytes
        childContext.close();
        parentContext.close();

        assertThat(childContext.getBytes()).isEqualTo(0);
        assertThat(parentContext.getBytes()).isEqualTo(0);

        // since we have exhausted the memory above after the memory contexts are closed
        // the pool must still be exhausted
        assertThat(reservationHandler.getReservation()).isEqualTo(maxMemory);
    }

    @Test
    public void testClosedLocalMemoryContext()
    {
        assertThatThrownBy(() -> {
            AggregatedMemoryContext aggregateContext = newSimpleAggregatedMemoryContext();
            LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");
            localContext.close();
            localContext.setBytes(100);
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SimpleLocalMemoryContext is already closed");
    }

    @Test
    public void testClosedAggregateMemoryContext()
    {
        assertThatThrownBy(() -> {
            AggregatedMemoryContext aggregateContext = newSimpleAggregatedMemoryContext();
            LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");
            aggregateContext.close();
            localContext.setBytes(100);
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SimpleAggregatedMemoryContext is already closed");
    }

    private static class TestMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private long reservation;
        private final long maxMemory;
        private SettableFuture<Void> future;

        public TestMemoryReservationHandler(long maxMemory)
        {
            this.maxMemory = maxMemory;
        }

        public long getReservation()
        {
            return reservation;
        }

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            reservation += delta;
            if (delta >= 0) {
                if (reservation >= maxMemory) {
                    future = SettableFuture.create();
                    return future;
                }
            }
            else {
                if (reservation < maxMemory) {
                    if (future != null) {
                        future.set(null);
                    }
                }
            }
            return NOT_BLOCKED;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            if (reservation + delta > maxMemory) {
                return false;
            }
            reservation += delta;
            return true;
        }

        public void exhaustMemory()
        {
            reservation = maxMemory;
        }
    }
}
