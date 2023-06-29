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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.spi.Page;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Scale up local writers based on throughput and data processed by writers.
 * Input pages are distributed across different writers in a round-robin fashion.
 */
public class ScaleWriterExchanger
        implements LocalExchanger
{
    private static final Logger log = Logger.get(ScaleWriterExchanger.class);

    private final List<Consumer<Page>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final long maxBufferedBytes;
    private final AtomicLong dataProcessed;
    private final long writerScalingMinDataProcessed;

    // Start with single writer and increase the writer count based on
    // data processed by writers and buffer utilization.
    private int writerCount = 1;
    private int nextWriterIndex = -1;

    public ScaleWriterExchanger(
            List<Consumer<Page>> buffers,
            LocalExchangeMemoryManager memoryManager,
            long maxBufferedBytes,
            AtomicLong dataProcessed,
            DataSize writerScalingMinDataProcessed)
    {
        this.buffers = requireNonNull(buffers, "buffers is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.maxBufferedBytes = maxBufferedBytes;
        this.dataProcessed = requireNonNull(dataProcessed, "dataProcessed is null");
        this.writerScalingMinDataProcessed = writerScalingMinDataProcessed.toBytes();
    }

    @Override
    public void accept(Page page)
    {
        dataProcessed.addAndGet(page.getSizeInBytes());
        Consumer<Page> buffer = buffers.get(getNextWriterIndex());
        memoryManager.updateMemoryUsage(page.getRetainedSizeInBytes());
        buffer.accept(page);
    }

    private int getNextWriterIndex()
    {
        // Scale up writers when current buffer memory utilization is more than 50% of the
        // maximum and data processed  is greater than current writer count * writerScalingMinOutputSize.
        // This also mean that we won't scale local writers if the writing speed can cope up
        // with incoming data. In another word, buffer utilization is below 50%.
        if (writerCount < buffers.size() && memoryManager.getBufferedBytes() >= maxBufferedBytes / 2) {
            if (dataProcessed.get() >= writerCount * writerScalingMinDataProcessed) {
                writerCount++;
                log.debug("Increased task writer count: %d", writerCount);
            }
        }

        nextWriterIndex = (nextWriterIndex + 1) % writerCount;
        return nextWriterIndex;
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
