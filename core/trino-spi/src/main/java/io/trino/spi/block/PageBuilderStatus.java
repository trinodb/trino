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
package io.trino.spi.block;

import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.clamp;
import static java.lang.Math.min;

public class PageBuilderStatus
{
    private static final int INITIAL_BATCH_SIZE = 2;
    private static final int BATCH_SIZE_GROWTH_FACTOR = 2;

    public static final int INSTANCE_SIZE = instanceSize(PageBuilderStatus.class);

    public static final int DEFAULT_MAX_PAGE_SIZE_IN_BYTES = 1024 * 1024;

    private final int maxPageSizeInBytes;
    private final Supplier<Long> currentSizeSupplier;

    private long lastRecordedSize;
    private int positionsAtLastRecordedSize;
    private int batchSize = INITIAL_BATCH_SIZE;

    public PageBuilderStatus(int maxPageSizeInBytes, Supplier<Long> currentSizeSupplier, OptionalInt maxRowCountHint)
    {
        this.maxPageSizeInBytes = maxPageSizeInBytes;
        this.currentSizeSupplier = currentSizeSupplier;
        // Use row count of previous page to start initial batch size from a higher value
        maxRowCountHint.ifPresent(value -> batchSize = value / 4);
    }

    public int getMaxPageSizeInBytes()
    {
        return maxPageSizeInBytes;
    }

    public boolean isFull(int declaredPositions)
    {
        if (declaredPositions - positionsAtLastRecordedSize >= batchSize) {
            positionsAtLastRecordedSize = declaredPositions;
            lastRecordedSize = currentSizeSupplier.get();
            if (lastRecordedSize >= maxPageSizeInBytes) {
                batchSize = 0;
                return true;
            }
            double sizePerPosition = (double) lastRecordedSize / declaredPositions;
            int maxRemainingPositions = clamp((long) (maxPageSizeInBytes / sizePerPosition) - declaredPositions, 1, Integer.MAX_VALUE);
            batchSize = min(batchSize * BATCH_SIZE_GROWTH_FACTOR, min(maxRemainingPositions, 8192));
        }
        return false;
    }

    public long getSizeInBytes()
    {
        return lastRecordedSize;
    }

    public int getMaxRowCountHint()
    {
        return positionsAtLastRecordedSize;
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder("PageBuilderStatus{");
        buffer.append("maxPageSizeInBytes=").append(maxPageSizeInBytes);
        buffer.append(", currentSize=").append(lastRecordedSize);
        buffer.append('}');
        return buffer.toString();
    }
}
