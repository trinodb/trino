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
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.ClientBuffer.PagesSupplier;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.execution.buffer.SerializedPageReference.PagesReleasedListener;
import io.trino.memory.context.LocalMemoryContext;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.trino.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.execution.buffer.PagesSerde.getSerializedPagePositionCount;
import static io.trino.execution.buffer.SerializedPageReference.dereferencePages;
import static java.util.Objects.requireNonNull;

/**
 * A buffer that assigns pages to queues based on a first come, first served basis.
 */
public class ArbitraryOutputBuffer
        implements OutputBuffer
{
    private final OutputBufferMemoryManager memoryManager;
    private final PagesReleasedListener onPagesReleased;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);

    private final MasterBuffer masterBuffer;

    @GuardedBy("this")
    private final ConcurrentMap<OutputBufferId, ClientBuffer> buffers = new ConcurrentHashMap<>();

    //  The index of the first client buffer that should be polled
    private final AtomicInteger nextClientBufferIndex = new AtomicInteger(0);

    private final OutputBufferStateMachine stateMachine;
    private final String taskInstanceId;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public ArbitraryOutputBuffer(
            String taskInstanceId,
            OutputBufferStateMachine stateMachine,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> memoryContextSupplier,
            Executor notificationExecutor)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.memoryManager = new OutputBufferMemoryManager(
                maxBufferSize.toBytes(),
                requireNonNull(memoryContextSupplier, "memoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.onPagesReleased = PagesReleasedListener.forOutputBufferMemoryManager(memoryManager);
        this.masterBuffer = new MasterBuffer(onPagesReleased);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return stateMachine.getState() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public boolean isOverutilized()
    {
        return (memoryManager.getUtilization() >= 0.5) || !stateMachine.getState().canAddPages();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = stateMachine.getState();

        // buffers it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> buffers = this.buffers.values();

        int totalBufferedPages = masterBuffer.getBufferedPages();
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (ClientBuffer buffer : buffers) {
            BufferInfo bufferInfo = buffer.getInfo();
            infos.add(bufferInfo);

            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedPages += pageBufferInfo.getBufferedPages();
        }

        return new OutputBufferInfo(
                "ARBITRARY",
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
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        checkState(!Thread.holdsLock(this), "Cannot set output buffers while holding a lock on this");
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        synchronized (this) {
            // ignore buffers added after query finishes, which can happen when a query is canceled
            // also ignore old versions, which is normal
            BufferState state = stateMachine.getState();
            if (state.isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
                return;
            }

            // verify this is valid state change
            outputBuffers.checkValidTransition(newOutputBuffers);
            outputBuffers = newOutputBuffers;

            // add the new buffers
            for (OutputBufferId outputBufferId : outputBuffers.getBuffers().keySet()) {
                getBuffer(outputBufferId);
            }
            // Reset resume from position
            nextClientBufferIndex.set(0);

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

        // reserve memory
        memoryManager.updateMemoryUsage(bytesAdded);

        // add pages to the buffer (this will increase the reference count by one)
        masterBuffer.addPages(serializedPageReferences);

        // process any pending reads from the client buffers
        List<ClientBuffer> buffers = safeGetBuffersSnapshot();
        if (buffers.isEmpty()) {
            return;
        }
        // handle potential for racy update of next index and client buffers present
        int index = nextClientBufferIndex.get() % buffers.size();
        for (int i = 0; i < buffers.size(); i++) {
            buffers.get(index).loadPagesIfNecessary(masterBuffer);
            index = (index + 1) % buffers.size();
            if (masterBuffer.isEmpty()) {
                // Resume from the next client buffer on the next iteration
                nextClientBufferIndex.set(index);
                break;
            }
        }
    }

    @Override
    public void enqueue(int partition, List<Slice> pages)
    {
        checkState(partition == 0, "Expected partition number to be zero");
        enqueue(pages);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Cannot get pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(bufferId).getPages(startingSequenceId, maxSize, Optional.of(masterBuffer));
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long sequenceId)
    {
        checkState(!Thread.holdsLock(this), "Cannot acknowledge pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).acknowledgePages(sequenceId);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        checkState(!Thread.holdsLock(this), "Cannot abort while holding a lock on this");
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

        masterBuffer.setNoMorePages();

        // process any pending reads from the client buffers
        for (ClientBuffer clientBuffer : safeGetBuffersSnapshot()) {
            clientBuffer.loadPagesIfNecessary(masterBuffer);
        }

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        checkState(!Thread.holdsLock(this), "Cannot destroy while holding a lock on this");

        // ignore destroy if the buffer already in a terminal state.
        if (stateMachine.finish()) {
            noMoreBuffers();

            masterBuffer.destroy();

            safeGetBuffersSnapshot().forEach(ClientBuffer::destroy);

            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
        }
    }

    @Override
    public void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        if (stateMachine.fail()) {
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
        checkState(stateMachine.getState().canAddBuffers() || !outputBuffers.isNoMoreBufferIds(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(taskInstanceId, id, onPagesReleased);

        // buffer may have finished immediately before calling this method
        if (stateMachine.getState() == FINISHED) {
            buffer.destroy();
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized List<ClientBuffer> safeGetBuffersSnapshot()
    {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private synchronized void noMoreBuffers()
    {
        if (outputBuffers.isNoMoreBufferIds()) {
            // verify all created buffers have been declared
            SetView<OutputBufferId> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
            checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
        }
    }

    private void checkFlushComplete()
    {
        // This buffer type assigns each page to a single, arbitrary reader,
        // so we don't need to wait for no-more-buffers to finish the buffer.
        // Any readers added after finish will simply receive no data.
        BufferState state = stateMachine.getState();
        if ((state == FLUSHING) || ((state == NO_MORE_PAGES) && masterBuffer.isEmpty())) {
            if (safeGetBuffersSnapshot().stream().allMatch(ClientBuffer::isDestroyed)) {
                destroy();
            }
        }
    }

    @ThreadSafe
    private static class MasterBuffer
            implements PagesSupplier
    {
        private final PagesReleasedListener onPagesReleased;

        @GuardedBy("this")
        private final LinkedList<SerializedPageReference> masterBuffer = new LinkedList<>();

        @GuardedBy("this")
        private boolean noMorePages;

        private final AtomicInteger bufferedPages = new AtomicInteger();

        private MasterBuffer(PagesReleasedListener onPagesReleased)
        {
            this.onPagesReleased = requireNonNull(onPagesReleased, "onPagesReleased is null");
        }

        public synchronized void addPages(List<SerializedPageReference> pages)
        {
            masterBuffer.addAll(pages);
            bufferedPages.set(masterBuffer.size());
        }

        public synchronized boolean isEmpty()
        {
            return masterBuffer.isEmpty();
        }

        @Override
        public synchronized boolean mayHaveMorePages()
        {
            return !noMorePages || !masterBuffer.isEmpty();
        }

        public synchronized void setNoMorePages()
        {
            this.noMorePages = true;
        }

        @Override
        public synchronized List<SerializedPageReference> getPages(DataSize maxSize)
        {
            long maxBytes = maxSize.toBytes();
            List<SerializedPageReference> pages = new ArrayList<>();
            long bytesRemoved = 0;

            while (true) {
                SerializedPageReference page = masterBuffer.peek();
                if (page == null) {
                    break;
                }
                bytesRemoved += page.getRetainedSizeInBytes();
                // break (and don't add) if this page would exceed the limit
                if (!pages.isEmpty() && bytesRemoved > maxBytes) {
                    break;
                }
                // this should not happen since we have a lock
                checkState(masterBuffer.poll() == page, "Master buffer corrupted");
                pages.add(page);
            }

            bufferedPages.set(masterBuffer.size());

            return ImmutableList.copyOf(pages);
        }

        public void destroy()
        {
            checkState(!Thread.holdsLock(this), "Cannot destroy master buffer while holding a lock on this");
            List<SerializedPageReference> pages;
            synchronized (this) {
                pages = ImmutableList.copyOf(masterBuffer);
                masterBuffer.clear();
                bufferedPages.set(0);
            }

            // dereference outside of synchronized to avoid making a callback while holding a lock
            dereferencePages(pages, onPagesReleased);
        }

        public int getBufferedPages()
        {
            return bufferedPages.get();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bufferedPages", bufferedPages.get())
                    .toString();
        }
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }
}
