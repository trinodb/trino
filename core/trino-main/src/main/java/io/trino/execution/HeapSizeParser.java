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
package io.trino.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import io.airlift.units.DataSize;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.parseDouble;
import static java.util.Objects.requireNonNull;

public class HeapSizeParser
{
    private static final String RELATIVE_SUFFIX = "%";

    // Memoize the max heap memory to avoid calling Runtime.getRuntime().maxMemory() multiple times
    // and to ensure that the value will not change over JVM lifetime.
    private static final Supplier<Long> AVAILABLE_HEAP_MEMORY = Suppliers.memoize(Runtime.getRuntime()::maxMemory);

    public static final HeapSizeParser DEFAULT = new HeapSizeParser(AVAILABLE_HEAP_MEMORY);

    private final Supplier<Long> maxHeapMemory;

    @VisibleForTesting
    HeapSizeParser(Supplier<Long> maxHeapMemory)
    {
        this.maxHeapMemory = requireNonNull(maxHeapMemory, "maxHeapMemory is null");
    }

    public DataSize parse(String value)
    {
        long maxHeapMemory = this.maxHeapMemory.get();
        checkState(maxHeapMemory > 0, "maxHeapMemory must be positive");

        if (value.endsWith(RELATIVE_SUFFIX)) {
            double multiplier = parseDouble(value.substring(0, value.length() - RELATIVE_SUFFIX.length()).trim()) / 100.0;
            return checkHeapSizeMemory(DataSize.ofBytes(Math.round(maxHeapMemory * multiplier)).succinct(), maxHeapMemory);
        }

        return checkHeapSizeMemory(DataSize.valueOf(value), maxHeapMemory);
    }

    private static DataSize checkHeapSizeMemory(DataSize heapSize, long maxHeapMemory)
    {
        checkArgument(heapSize.toBytes() <= maxHeapMemory, "Heap size cannot be greater than maximum heap size");
        checkArgument(heapSize.toBytes() > 0, "Heap size cannot be less than or equal to 0");
        return heapSize;
    }
}
