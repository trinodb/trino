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
package io.trino.operator;

import io.airlift.units.DataSize;
import io.trino.memory.context.MemoryAllocationValidator;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.ExceededMemoryLimitException.exceededTaskMemoryLimit;
import static java.lang.String.format;
import static java.util.Map.Entry.comparingByValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
// Keeps track of per-node memory usage of given task. Single instance is shared by multiple ValidatingLocalMemoryContext instances
// originating from single ValidatingAggregateContext.
public class TaskAllocationValidator
        implements MemoryAllocationValidator
{
    private final long limitBytes;
    @GuardedBy("this")
    private long usedBytes;
    @GuardedBy("this")
    private final Map<String, Long> taggedAllocations = new HashMap<>();

    public TaskAllocationValidator(DataSize memoryLimit)
    {
        this.limitBytes = requireNonNull(memoryLimit, "memoryLimit is null").toBytes();
    }

    @Override
    public synchronized void reserveMemory(String allocationTag, long delta)
    {
        if (usedBytes + delta > limitBytes) {
            verify(delta > 0, "exceeded limit with negative delta (%s); usedBytes=%s, limitBytes=%s", delta, usedBytes, limitBytes);
            raiseLimitExceededFailure(allocationTag, delta);
        }
        usedBytes += delta;
        taggedAllocations.merge(allocationTag, delta, Long::sum);
    }

    private synchronized void raiseLimitExceededFailure(String currentAllocationTag, long currentAllocationDelta)
    {
        Map<String, Long> tmpTaggedAllocations = new HashMap<>(taggedAllocations);
        // include current allocation in the output of top-consumers
        tmpTaggedAllocations.merge(currentAllocationTag, currentAllocationDelta, Long::sum);
        String topConsumers = tmpTaggedAllocations.entrySet().stream()
                .sorted(comparingByValue(Comparator.reverseOrder()))
                .limit(3)
                .filter(e -> e.getValue() >= 0)
                .collect(toImmutableMap(Map.Entry::getKey, e -> succinctBytes(e.getValue())))
                .toString();

        String additionalInfo = format("Allocated: %s, Delta: %s, Top Consumers: %s", succinctBytes(usedBytes), succinctBytes(currentAllocationDelta), topConsumers);
        throw exceededTaskMemoryLimit(DataSize.succinctBytes(limitBytes), additionalInfo);
    }

    @Override
    public synchronized boolean tryReserveMemory(String allocationTag, long delta)
    {
        if (usedBytes + delta > limitBytes) {
            verify(delta > 0, "exceeded limit with negative delta (%s); usedBytes=%s, limitBytes=%s", delta, usedBytes, limitBytes);
            return false;
        }
        usedBytes += delta;
        return true;
    }
}
