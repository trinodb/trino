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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.exchange.ExchangeSink;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SpoolingExchangeOutputBuffer
        implements OutputBuffer
{
    private static final Logger log = Logger.get(SpoolingExchangeOutputBuffer.class);

    private final OutputBufferStateMachine stateMachine;
    private volatile SpoolingOutputBuffers outputBuffers;
    // This field is not final to allow releasing the memory retained by the ExchangeSink instance.
    // It is modified (assigned to null) when the OutputBuffer is destroyed (either finished or aborted).
    // It doesn't have to be declared as volatile as the nullification of this variable doesn't have to be immediately visible to other threads.
    // However since the abort can be triggered at any moment of time this variable has to be accessed in a safe way (avoiding "check-then-use").
    private ExchangeSink exchangeSink;
    private final Supplier<LocalMemoryContext> memoryContextSupplier;

    private final AtomicLong peakMemoryUsage = new AtomicLong();
    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    private final SpoolingOutputStats outputStats;

    public SpoolingExchangeOutputBuffer(
            OutputBufferStateMachine stateMachine,
            SpoolingOutputBuffers outputBuffers,
            ExchangeSink exchangeSink,
            Supplier<LocalMemoryContext> memoryContextSupplier)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        // this assignment is expected to be followed by an assignment of a final field to ensure safe publication
        this.exchangeSink = requireNonNull(exchangeSink, "exchangeSink is null");
        this.memoryContextSupplier = requireNonNull(memoryContextSupplier, "memoryContextSupplier is null");

        stateMachine.noMoreBuffers();

        outputStats = new SpoolingOutputStats(outputBuffers.getOutputPartitionCount());
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        BufferState state = stateMachine.getState();
        LocalMemoryContext memoryContext = getSystemMemoryContextOrNull();
        return new OutputBufferInfo(
                "EXTERNAL",
                state,
                false,
                state.canAddPages(),
                memoryContext == null ? 0 : memoryContext.getBytes(),
                totalPagesAdded.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                Optional.empty(),
                Optional.empty(),
                outputStats.getFinalSnapshot());
    }

    @Override
    public BufferState getState()
    {
        return stateMachine.getState();
    }

    @Override
    public double getUtilization()
    {
        return 0;
    }

    @Override
    public OutputBufferStatus getStatus()
    {
        // do not grab lock to acquire outputBuffers to avoid delaying TaskStatus response
        OutputBufferStatus.Builder result = OutputBufferStatus.builder(outputBuffers.getVersion());
        ExchangeSink sink = exchangeSink;
        if (sink != null) {
            result.setExchangeSinkInstanceHandleUpdateRequired(sink.isHandleUpdateRequired());
        }
        return result.build();
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (stateMachine.getState().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);

        ExchangeSink sink = exchangeSink;
        if (sink != null) {
            sink.updateHandle(((SpoolingOutputBuffers) newOutputBuffers).getExchangeSinkInstanceHandle());
        }

        // assign output buffers only after updating the sink to avoid triggering an extra update
        outputBuffers = (SpoolingOutputBuffers) newOutputBuffers;
    }

    @Override
    public ListenableFuture<BufferResult> get(PipelinedOutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledge(PipelinedOutputBuffers.OutputBufferId bufferId, long token)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy(PipelinedOutputBuffers.OutputBufferId bufferId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> isFull()
    {
        ExchangeSink sink = exchangeSink;
        if (sink != null) {
            return toListenableFuture(sink.isBlocked());
        }
        return immediateVoidFuture();
    }

    @Override
    public void enqueue(List<Slice> pages)
    {
        enqueue(0, pages);
    }

    @Override
    public void enqueue(int partition, List<Slice> pages)
    {
        requireNonNull(pages, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!stateMachine.getState().canAddPages()) {
            return;
        }

        ExchangeSink sink = exchangeSink;
        checkState(sink != null, "exchangeSink is null");
        long dataSizeInBytes = 0;
        for (Slice page : pages) {
            dataSizeInBytes += page.length();
            sink.add(partition, page);
            totalRowsAdded.addAndGet(getSerializedPagePositionCount(page));
        }
        updateMemoryUsage(sink.getMemoryUsage());
        totalPagesAdded.addAndGet(pages.size());
        outputStats.update(partition, dataSizeInBytes);
    }

    @Override
    public void setNoMorePages()
    {
        if (!stateMachine.noMorePages()) {
            return;
        }

        outputStats.finish();

        ExchangeSink sink = exchangeSink;
        if (sink == null) {
            // abort might've released the sink in a meantime
            return;
        }
        sink.finish().whenComplete((value, failure) -> {
            if (failure != null) {
                stateMachine.fail(failure);
            }
            else {
                stateMachine.finish();
            }
            exchangeSink = null;
            forceFreeMemory();
        });
    }

    @Override
    public void destroy()
    {
        // Abort the buffer if it hasn't been finished. This is possible when a task is cancelled early by the coordinator.
        // Task cancellation is not supported (and not expected to be requested by the coordinator when the spooling exchange
        // is in use) as the task output is expected to be deterministic.
        // In a scenario when due to a bug in coordinator logic a cancellation is requested it is better to invalidate the sink
        // to avoid publishing incomplete data to the downstream stage that could potentially cause a correctness problem
        abort();
    }

    @Override
    public void abort()
    {
        if (!stateMachine.abort()) {
            return;
        }

        ExchangeSink sink = exchangeSink;
        if (sink == null) {
            return;
        }
        sink.abort().whenComplete((value, failure) -> {
            if (failure != null) {
                log.warn(failure, "Error aborting exchange sink");
            }
            exchangeSink = null;
            forceFreeMemory();
        });
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return peakMemoryUsage.get();
    }

    @Override
    public Optional<Throwable> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    private void updateMemoryUsage(long bytes)
    {
        LocalMemoryContext context = getSystemMemoryContextOrNull();
        if (context != null) {
            context.setBytes(bytes);
        }
        updatePeakMemoryUsage(bytes);
    }

    private void updatePeakMemoryUsage(long bytes)
    {
        while (true) {
            long currentValue = peakMemoryUsage.get();
            if (currentValue >= bytes) {
                return;
            }
            if (peakMemoryUsage.compareAndSet(currentValue, bytes)) {
                return;
            }
        }
    }

    private void forceFreeMemory()
    {
        LocalMemoryContext context = getSystemMemoryContextOrNull();
        if (context != null) {
            context.close();
        }
    }

    private LocalMemoryContext getSystemMemoryContextOrNull()
    {
        try {
            return memoryContextSupplier.get();
        }
        catch (RuntimeException ignored) {
            // This is possible with races, e.g., a task is created and then immediately aborted,
            // so that the task context hasn't been created yet (as a result there's no memory context available).
            return null;
        }
    }
}
