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

import com.google.errorprone.annotations.ThreadSafe;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/// Hierarchical memory tracker
@ThreadSafe
public final class MemoryTrackingContext
{
    private final AggregatedMemoryContext userAggregateMemoryContext;
    private final AggregatedMemoryContext revocableAggregateMemoryContext;

    public MemoryTrackingContext(
            AggregatedMemoryContext userAggregateMemoryContext,
            AggregatedMemoryContext revocableAggregateMemoryContext)
    {
        this.userAggregateMemoryContext = requireNonNull(userAggregateMemoryContext, "userAggregateMemoryContext is null");
        this.revocableAggregateMemoryContext = requireNonNull(revocableAggregateMemoryContext, "revocableAggregateMemoryContext is null");
    }

    public AggregatedMemoryContext aggregateUserMemoryContext()
    {
        return userAggregateMemoryContext;
    }

    public AggregatedMemoryContext aggregateRevocableMemoryContext()
    {
        return revocableAggregateMemoryContext;
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("userAggregateMemoryContext", userAggregateMemoryContext)
                .add("revocableAggregateMemoryContext", revocableAggregateMemoryContext)
                .toString();
    }
}
