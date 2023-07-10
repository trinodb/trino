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

import com.google.common.io.Closer;
import com.google.errorprone.annotations.ThreadSafe;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Objects.requireNonNull;

/**
 * This class is used to track memory usage at all levels (operator, driver, pipeline, etc.).
 * <p>
 * At every level we have three aggregate and three local memory contexts. The local memory contexts
 * track the allocations in the current level while the aggregate memory contexts aggregate the memory
 * allocated by the leaf levels and the current level.
 * <p>
 * The reason we have local memory contexts at every level is that not all the
 * allocations are done by the leaf levels (e.g., at the pipeline level exchange clients
 * can do system allocations directly, see the ExchangeOperator, another example is the buffers
 * doing system allocations at the task context level, etc.).
 * <p>
 * As another example, at the pipeline level there will be system allocations initiated by the operator context
 * and there will be system allocations initiated by the exchange clients (local allocations). All these system
 * allocations will be visible in the systemAggregateMemoryContext.
 * <p>
 * To perform local allocations clients should use localUserMemoryContext()
 * and get a reference to the local memory contexts. Clients can also use updateUserMemory()/tryReserveUserMemory()
 * to allocate memory (non-local allocations), which will be reflected to all ancestors of this context in the hierarchy.
 */
@ThreadSafe
public final class MemoryTrackingContext
{
    private final AggregatedMemoryContext userAggregateMemoryContext;
    private final AggregatedMemoryContext revocableAggregateMemoryContext;

    private LocalMemoryContext userLocalMemoryContext;
    private LocalMemoryContext revocableLocalMemoryContext;

    public MemoryTrackingContext(
            AggregatedMemoryContext userAggregateMemoryContext,
            AggregatedMemoryContext revocableAggregateMemoryContext)
    {
        this.userAggregateMemoryContext = requireNonNull(userAggregateMemoryContext, "userAggregateMemoryContext is null");
        this.revocableAggregateMemoryContext = requireNonNull(revocableAggregateMemoryContext, "revocableAggregateMemoryContext is null");
    }

    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(userAggregateMemoryContext::close);
            closer.register(revocableAggregateMemoryContext::close);
            closer.register(userLocalMemoryContext::close);
            closer.register(revocableLocalMemoryContext::close);
        }
        catch (IOException e) {
            throw new RuntimeException("Exception closing memory tracking context", e);
        }
    }

    public LocalMemoryContext localUserMemoryContext()
    {
        verifyNotNull(userLocalMemoryContext, "local memory contexts are not initialized");
        return userLocalMemoryContext;
    }

    public LocalMemoryContext localRevocableMemoryContext()
    {
        verifyNotNull(revocableLocalMemoryContext, "local memory contexts are not initialized");
        return revocableLocalMemoryContext;
    }

    public LocalMemoryContext newUserMemoryContext(String allocationTag)
    {
        return userAggregateMemoryContext.newLocalMemoryContext(allocationTag);
    }

    public AggregatedMemoryContext aggregateUserMemoryContext()
    {
        return userAggregateMemoryContext;
    }

    public AggregatedMemoryContext aggregateRevocableMemoryContext()
    {
        return revocableAggregateMemoryContext;
    }

    public AggregatedMemoryContext newAggregateUserMemoryContext()
    {
        return userAggregateMemoryContext.newAggregatedMemoryContext();
    }

    public AggregatedMemoryContext newAggregateRevocableMemoryContext()
    {
        return revocableAggregateMemoryContext.newAggregatedMemoryContext();
    }

    public long getUserMemory()
    {
        return userAggregateMemoryContext.getBytes();
    }

    public long getRevocableMemory()
    {
        return revocableAggregateMemoryContext.getBytes();
    }

    public MemoryTrackingContext newMemoryTrackingContext()
    {
        return new MemoryTrackingContext(
                userAggregateMemoryContext.newAggregatedMemoryContext(),
                revocableAggregateMemoryContext.newAggregatedMemoryContext());
    }

    /**
     * This method has to be called to initialize the local memory contexts. Otherwise, calls to methods
     * localUserMemoryContext(), etc. will fail.
     */
    public void initializeLocalMemoryContexts(String allocationTag)
    {
        this.userLocalMemoryContext = userAggregateMemoryContext.newLocalMemoryContext(allocationTag);
        this.revocableLocalMemoryContext = revocableAggregateMemoryContext.newLocalMemoryContext(allocationTag);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("userAggregateMemoryContext", userAggregateMemoryContext)
                .add("revocableAggregateMemoryContext", revocableAggregateMemoryContext)
                .add("userLocalMemoryContext", userLocalMemoryContext)
                .add("revocableLocalMemoryContext", revocableLocalMemoryContext)
                .toString();
    }
}
