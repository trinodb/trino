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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.connector.MemoryContext;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

final class MemoryContextReservationHandler
        implements MemoryReservationHandler
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();
    private final MemoryContext memoryContext;

    @GuardedBy("this")
    private long memoryUsage;

    public MemoryContextReservationHandler(MemoryContext memoryContext)
    {
        this.memoryContext = memoryContext;
    }

    @Override
    public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
    {
        if (delta != 0) {
            synchronized (this) {
                memoryUsage += delta;
                memoryContext.setBytes(memoryUsage);
            }
        }
        return NOT_BLOCKED;
    }

    @Override
    public boolean tryReserveMemory(String allocationTag, long delta)
    {
        ListenableFuture<Void> _ = reserveMemory(allocationTag, delta);
        return true;
    }
}
