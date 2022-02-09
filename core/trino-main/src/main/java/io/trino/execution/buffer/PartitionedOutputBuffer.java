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
package io.trino.execution.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.execution.buffer.SerializedPageReference.PagesReleasedListener;
import io.trino.memory.context.LocalMemoryContext;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.PagesSerde.getSerializedPagePositionCount;
import static io.trino.execution.buffer.SerializedPageReference.dereferencePages;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputBuffer
        implements OutputBuffer
{
    private final OutputBufferStateMachine stateMachine;
    private final OutputBuffers outputBuffers;
    private final OutputBufferMemoryManager memoryManager;
    private final PagesReleasedListener onPagesReleased;

    private final List<ClientBuffer> partitions;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public PartitionedOutputBuffer(
            String taskInstanceId,
            OutputBufferStateMachine stateMachine,
            OutputBuffers outputBuffers,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> memoryContextSupplier,
            Executor notificationExecutor)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");

        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers.getType() == PARTITIONED, "Expected a PARTITIONED output buffer descriptor");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "Expected a final output buffer descriptor");
        this.outputBuffers = outputBuffers;
        this.memoryManager = new OutputBufferMemoryManager(
                requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes(),
                requireNonNull(memoryContextSupplier, "memoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.onPagesReleased = PagesReleasedListener.forOutputBufferMemoryManager(memoryManager);

        ImmutableList.Builder<ClientBuffer> partitions = ImmutableList.builder();
        for (OutputBufferId bufferId : outputBuffers.getBuffers().keySet()) {
            ClientBuffer partition = new ClientBuffer(taskInstanceId, bufferId, onPagesReleased);
            partitions.add(partition);
        }
        this.partitions = partitions.build();

        stateMachine.noMoreBuffers();
        checkFlushComplete();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public boolean isOverutilized()
    {
        return memoryManager.isOverutilized();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = stateMachine.getState();

        int totalBufferedPages = 0;
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builderWithExpectedSize(partitions.size());
        for (ClientBuffer partition : partitions) {
            BufferInfo bufferInfo = partition.getInfo();
            infos.add(bufferInfo);
            totalBufferedPages += bufferInfo.getPageBufferInfo().getBufferedPages();
        }

        return new OutputBufferInfo(
                "PARTITIONED",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                infos.build());
    }

    @Override
    public BufferState getState()
    {
        return stateMachine.getState();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (stateMachine.getState().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<Void> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    @Override
    public void enqueue(List<Slice> pages)
    {
        checkState(partitions.size() == 1, "Expected exactly one partition");
        enqueue(0, pages);
    }

    @Override
    public void enqueue(int partitionNumber, List<Slice> pages)
    {
        requireNonNull(pages, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!stateMachine.getState().canAddPages()) {
            return;
        }

        ImmutableList.Builder<SerializedPageReference> references = ImmutableList.builderWithExpectedSize(pages.size());
        long bytesAdded = 0;
        long rowCount = 0;
        for (Slice page : pages) {
            bytesAdded += page.getRetainedSize();
            int positionCount = getSerializedPagePositionCount(page);
            rowCount += positionCount;
            // create page reference counts with an initial single reference
            references.add(new SerializedPageReference(page, positionCount, 1));
        }
        List<SerializedPageReference> serializedPageReferences = references.build();

        // update stats
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(serializedPageReferences.size());

        // reserve memory
        memoryManager.updateMemoryUsage(bytesAdded);

        // add pages to the buffer (this will increase the reference count by one)
        partitions.get(partitionNumber).enqueuePages(serializedPageReferences);

        // drop the initial reference
        dereferencePages(serializedPageReferences, onPagesReleased);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return partitions.get(outputBufferId.getId()).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId outputBufferId, long sequenceId)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");

        partitions.get(outputBufferId.getId()).acknowledgePages(sequenceId);
    }

    @Override
    public void destroy(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        partitions.get(bufferId.getId()).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        stateMachine.noMorePages();
        memoryManager.setNoBlockOnFull();

        partitions.forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (stateMachine.finish()) {
            partitions.forEach(ClientBuffer::destroy);
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
        }
    }

    @Override
    public void abort()
    {
        // ignore abort if the buffer already in a terminal state.
        if (stateMachine.abort()) {
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
            // DO NOT destroy buffers or set no more pages.  The coordinator manages the teardown of failed queries.
        }
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return memoryManager.getPeakMemoryUsage();
    }

    @Override
    public Optional<Throwable> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    @VisibleForTesting
    void forceFreeMemory()
    {
        memoryManager.close();
    }

    private void checkFlushComplete()
    {
        BufferState state = stateMachine.getState();
        if (state != FLUSHING && state != NO_MORE_BUFFERS) {
            return;
        }

        if (partitions.stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }
}
