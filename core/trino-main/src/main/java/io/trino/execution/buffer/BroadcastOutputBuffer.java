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
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.execution.buffer.SerializedPageReference.PagesReleasedListener;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.base.metrics.TDigestHistogram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.buffer.BufferState.ABORTED;
import static io.trino.execution.buffer.BufferState.FAILED;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.BROADCAST;
import static io.trino.execution.buffer.SerializedPageReference.dereferencePages;
import static java.util.Objects.requireNonNull;

public class BroadcastOutputBuffer
        implements OutputBuffer
{
    private final String taskInstanceId;
    private final OutputBufferStateMachine stateMachine;
    private final OutputBufferMemoryManager memoryManager;
    private final PagesReleasedListener onPagesReleased;

    @GuardedBy("this")
    private volatile PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(BROADCAST);

    @GuardedBy("this")
    private final Map<OutputBufferId, ClientBuffer> buffers = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final List<SerializedPageReference> initialPagesForNewBuffers = new ArrayList<>();

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();
    private final AtomicLong totalBufferedPages = new AtomicLong();

    private final AtomicBoolean hasBlockedBefore = new AtomicBoolean();
    private final Runnable notifyStatusChanged;

    public BroadcastOutputBuffer(
            String taskInstanceId,
            OutputBufferStateMachine stateMachine,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> memoryContextSupplier,
            Executor notificationExecutor,
            Runnable notifyStatusChanged)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.memoryManager = new OutputBufferMemoryManager(
                maxBufferSize.toBytes(),
                requireNonNull(memoryContextSupplier, "memoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.onPagesReleased = (releasedPageCount, releasedMemorySizeInBytes) -> {
            checkState(totalBufferedPages.addAndGet(-releasedPageCount) >= 0);
            memoryManager.updateMemoryUsage(-releasedMemorySizeInBytes);
        };
        this.notifyStatusChanged = requireNonNull(notifyStatusChanged, "notifyStatusChanged is null");
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
    public OutputBufferStatus getStatus()
    {
        // do not grab lock to acquire outputBuffers to avoid delaying TaskStatus response
        return OutputBufferStatus.builder(outputBuffers.getVersion())
                .setOverutilized(getUtilization() > 0.5 && stateMachine.getState().canAddPages())
                .build();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = stateMachine.getState();

        // buffer it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> buffers = this.buffers.values();

        return new OutputBufferInfo(
                "BROADCAST",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                Optional.of(buffers.stream()
                        .map(ClientBuffer::getInfo)
                        .collect(toImmutableList())),
                Optional.of(new TDigestHistogram(memoryManager.getUtilizationHistogram())),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public BufferState getState()
    {
        return stateMachine.getState();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        checkState(!Thread.holdsLock(this), "Cannot set output buffers while holding a lock on this");
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");
        checkArgument(newOutputBuffers instanceof PipelinedOutputBuffers, "newOutputBuffers is expected to be an instance of PipelinedOutputBuffers");

        synchronized (this) {
            // ignore buffers added after query finishes, which can happen when a query is canceled
            // also ignore old versions, which is normal
            BufferState state = stateMachine.getState();
            if (state.isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
                return;
            }

            // verify this is valid state change
            outputBuffers.checkValidTransition(newOutputBuffers);
            outputBuffers = (PipelinedOutputBuffers) newOutputBuffers;

            // add the new buffers
            for (Entry<OutputBufferId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
                if (!buffers.containsKey(entry.getKey())) {
                    ClientBuffer buffer = getBuffer(entry.getKey());
                    if (!state.canAddPages()) {
                        buffer.setNoMorePages();
                    }
                }
            }

            // update state if no more buffers is set
            if (outputBuffers.isNoMoreBufferIds()) {
                stateMachine.noMoreBuffers();
            }
        }

        if (!stateMachine.getState().canAddBuffers()) {
            noMoreBuffers();
        }

        checkFlushComplete();
    }

    @Override
    public ListenableFuture<Void> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    @Override
    public void enqueue(List<Slice> pages)
    {
        checkState(!Thread.holdsLock(this), "Cannot enqueue pages while holding a lock on this");
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
        totalBufferedPages.addAndGet(serializedPageReferences.size());

        // reserve memory
        memoryManager.updateMemoryUsage(bytesAdded);

        // if we can still add buffers, remember the pages for the future buffers
        Collection<ClientBuffer> buffers;
        synchronized (this) {
            if (stateMachine.getState().canAddBuffers()) {
                serializedPageReferences.forEach(SerializedPageReference::addReference);
                initialPagesForNewBuffers.addAll(serializedPageReferences);
            }

            // make a copy while holding the lock to avoid race with initialPagesForNewBuffers.addAll above
            buffers = safeGetBuffersSnapshot();
        }

        // add pages to all existing buffers (each buffer will increment the reference count)
        buffers.forEach(partition -> partition.enqueuePages(serializedPageReferences));

        // drop the initial reference
        dereferencePages(serializedPageReferences, onPagesReleased);

        // if the buffer is full for first time and more clients are expected, update the task status
        // notifying a status change will lead to the SourcePartitionedScheduler sending 'no-more-buffers' to unblock
        if (!hasBlockedBefore.get()
                && stateMachine.getState().canAddBuffers()
                && !isFull().isDone()
                && hasBlockedBefore.compareAndSet(false, true)) {
            notifyStatusChanged.run();
        }
    }

    @Override
    public void enqueue(int partitionNumber, List<Slice> pages)
    {
        checkState(partitionNumber == 0, "Expected partition number to be zero");
        enqueue(pages);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Cannot get pages while holding a lock on this");
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(outputBufferId).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long sequenceId)
    {
        checkState(!Thread.holdsLock(this), "Cannot acknowledge pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).acknowledgePages(sequenceId);
    }

    @Override
    public void destroy(OutputBufferId bufferId)
    {
        checkState(!Thread.holdsLock(this), "Cannot destroy while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        checkState(!Thread.holdsLock(this), "Cannot set no more pages while holding a lock on this");
        stateMachine.noMorePages();
        memoryManager.setNoBlockOnFull();

        safeGetBuffersSnapshot().forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        checkState(!Thread.holdsLock(this), "Cannot destroy while holding a lock on this");

        // ignore destroy if the buffer already in a terminal state.
        if (stateMachine.finish()) {
            noMoreBuffers();

            safeGetBuffersSnapshot().forEach(ClientBuffer::destroy);

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

    private synchronized ClientBuffer getBuffer(OutputBufferId id)
    {
        ClientBuffer buffer = buffers.get(id);
        if (buffer != null) {
            return buffer;
        }

        // NOTE: buffers are allowed to be created in the FINISHED state because destroy() can move to the finished state
        // without a clean "no-more-buffers" message from the scheduler.  This happens with limit queries and is ok because
        // the buffer will be immediately destroyed.
        BufferState state = stateMachine.getState();
        // buffer may become aborted while the final output buffers are being set
        checkState(state == ABORTED || state.canAddBuffers() || !outputBuffers.isNoMoreBufferIds(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(taskInstanceId, id, onPagesReleased);

        // do not setup the new buffer if we are already aborted
        if (state != ABORTED) {
            verify(state != FAILED, "broadcast output buffer is not expected to fail internally");

            // add initial pages
            buffer.enqueuePages(initialPagesForNewBuffers);

            // update state
            if (!state.canAddPages()) {
                // BE CAREFUL: set no more pages only if not FAILED, because this allows clients to FINISH
                buffer.setNoMorePages();
            }

            // buffer may have finished immediately before calling this method
            if (state == FINISHED) {
                buffer.destroy();
            }
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized Collection<ClientBuffer> safeGetBuffersSnapshot()
    {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private void noMoreBuffers()
    {
        checkState(!Thread.holdsLock(this), "Cannot set no more buffers while holding a lock on this");
        List<SerializedPageReference> pages;
        synchronized (this) {
            pages = ImmutableList.copyOf(initialPagesForNewBuffers);
            initialPagesForNewBuffers.clear();

            if (outputBuffers.isNoMoreBufferIds()) {
                // verify all created buffers have been declared
                SetView<OutputBufferId> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
                checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
            }
        }

        // dereference outside of synchronized to avoid making a callback while holding a lock
        dereferencePages(pages, onPagesReleased);
    }

    private void checkFlushComplete()
    {
        BufferState state = stateMachine.getState();
        if (state != FLUSHING && state != NO_MORE_BUFFERS) {
            return;
        }

        if (safeGetBuffersSnapshot().stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }
}
