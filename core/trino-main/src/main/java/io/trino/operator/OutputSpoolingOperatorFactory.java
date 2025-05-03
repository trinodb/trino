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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.client.spooling.DataAttributes;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.OperationTimer.OperationTiming;
import io.trino.operator.SpoolingController.MetricSnapshot;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.SpooledMetadataBlock;
import io.trino.spi.Mergeable;
import io.trino.spi.Page;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
import io.trino.sql.planner.plan.PlanNodeId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.client.spooling.DataAttribute.EXPIRES_AT;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.FINISHED;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.HAS_LAST_OUTPUT;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.HAS_OUTPUT;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.NEEDS_INPUT;
import static io.trino.operator.SpoolingController.Mode.INLINE;
import static io.trino.operator.SpoolingController.Mode.SPOOL;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.getInitialSegmentSize;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.getMaxSegmentSize;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class OutputSpoolingOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final SpoolingManager spoolingManager;
    private final Supplier<QueryDataEncoder> queryDataEncoderSupplier;
    private final QueryDataEncoder queryDataEncoder;
    private final AtomicInteger encoderReferencesCount = new AtomicInteger(1);

    private boolean closed;

    public OutputSpoolingOperatorFactory(int operatorId, PlanNodeId planNodeId, Supplier<QueryDataEncoder> queryDataEncoderSupplier, SpoolingManager spoolingManager)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.queryDataEncoderSupplier = requireNonNull(queryDataEncoderSupplier, "queryDataEncoder is null");
        this.queryDataEncoder = queryDataEncoderSupplier.get();
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OutputSpoolingOperator.class.getSimpleName());
        encoderReferencesCount.incrementAndGet();
        QueryDataEncoder trackingQueryDataEncoder = new QueryDataEncoder()
        {
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public DataAttributes encodeTo(OutputStream output, List<Page> pages)
                    throws IOException
            {
                return queryDataEncoder.encodeTo(output, pages);
            }

            @Override
            public void close()
            {
                if (closed.getAndSet(true)) {
                    return;
                }
                closeEncoderIfNoMoreReferences();
            }

            @Override
            public String encoding()
            {
                return queryDataEncoder.encoding();
            }
        };
        return new OutputSpoolingOperator(operatorContext, trackingQueryDataEncoder, spoolingManager);
    }

    @Override
    public void noMoreOperators()
    {
        closed = true;
        closeEncoderIfNoMoreReferences();
    }

    private void closeEncoderIfNoMoreReferences()
    {
        if (encoderReferencesCount.decrementAndGet() == 0) {
            queryDataEncoder.close();
        }
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new OutputSpoolingOperatorFactory(operatorId, planNodeId, queryDataEncoderSupplier, spoolingManager);
    }

    static class OutputSpoolingOperator
            implements Operator
    {
        private final SpoolingController controller;
        private final ZoneId clientZoneId;

        enum State
        {
            NEEDS_INPUT, // output is not ready
            HAS_OUTPUT, // output page ready
            HAS_LAST_OUTPUT, // last output page ready
            FINISHED // no more pages will be ever produced
        }

        private OutputSpoolingOperator.State state = NEEDS_INPUT;
        private final OperatorContext operatorContext;
        private final AggregatedMemoryContext aggregatedMemoryContext;
        private final LocalMemoryContext localMemoryContext;
        private final QueryDataEncoder queryDataEncoder;
        private final SpoolingManager spoolingManager;
        private final PageBuffer buffer;
        private final OperationTiming spoolingTiming = new OperationTiming();
        private final AtomicLong spooledEncodedBytes = new AtomicLong();
        private final AtomicLong inlinedEncodedBytes = new AtomicLong();

        private Page outputPage;

        public OutputSpoolingOperator(OperatorContext operatorContext, QueryDataEncoder queryDataEncoder, SpoolingManager spoolingManager)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.clientZoneId = operatorContext.getSession().getTimeZoneKey().getZoneId();
            this.controller = new PipelineSpoolingController(
                    operatorContext.getDriverContext().getPipelineContext(),
                    new OperatorSpoolingController(
                        getInitialSegmentSize(operatorContext.getSession()).toBytes(),
                        getMaxSegmentSize(operatorContext.getSession()).toBytes()));
            this.aggregatedMemoryContext = operatorContext.newAggregateUserMemoryContext();
            this.queryDataEncoder = requireNonNull(queryDataEncoder, "queryDataEncoder is null");
            this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
            this.buffer = PageBuffer.create();
            this.localMemoryContext = aggregatedMemoryContext.newLocalMemoryContext(OutputSpoolingOperator.class.getSimpleName());

            operatorContext.setInfoSupplier(new OutputSpoolingInfoSupplier(spoolingTiming, controller, inlinedEncodedBytes, spooledEncodedBytes));
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public boolean needsInput()
        {
            return state == NEEDS_INPUT;
        }

        @Override
        public void addInput(Page page)
        {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");

            outputPage = switch (controller.nextMode(page)) {
                case SPOOL -> {
                    buffer.add(page);
                    yield outputBuffer();
                }
                case BUFFER -> {
                    buffer.add(page);
                    yield null;
                }
                case INLINE -> inline(page);
            };

            if (outputPage != null) {
                state = HAS_OUTPUT;
            }

            updateMemoryReservation();
        }

        @Override
        public Page getOutput()
        {
            if (state != HAS_OUTPUT && state != HAS_LAST_OUTPUT) {
                return null;
            }

            Page toReturn = outputPage;
            outputPage = null;
            state = state == HAS_LAST_OUTPUT ? FINISHED : NEEDS_INPUT;
            updateMemoryReservation();
            return toReturn;
        }

        @Override
        public void finish()
        {
            if (state == NEEDS_INPUT) {
                outputPage = outputBuffer();
                if (outputPage != null) {
                    state = HAS_LAST_OUTPUT;
                    controller.finish();
                }
                else {
                    state = FINISHED;
                }
            }
        }

        @Override
        public boolean isFinished()
        {
            return state == FINISHED;
        }

        private Page outputBuffer()
        {
            if (buffer.isEmpty()) {
                return null;
            }

            return spool(buffer.removeAll());
        }

        private Page spool(List<Page> pages)
        {
            long rows = reduce(pages, Page::getPositionCount);
            long size = reduce(pages, Page::getSizeInBytes);
            SpooledSegmentHandle segmentHandle = spoolingManager.create(new SpoolingContext(
                    queryDataEncoder.encoding(),
                    operatorContext.getDriverContext().getSession().getQueryId(),
                    rows,
                    size));

            OperationTimer overallTimer = new OperationTimer(false);
            try (OutputStream output = spoolingManager.createOutputStream(segmentHandle)) {
                DataAttributes attributes = queryDataEncoder.encodeTo(output, pages)
                        .toBuilder()
                        .set(ROWS_COUNT, rows)
                        .set(EXPIRES_AT, ZonedDateTime.ofInstant(segmentHandle.expirationTime(), clientZoneId).toLocalDateTime().toString())
                        .build();
                spooledEncodedBytes.addAndGet(attributes.get(SEGMENT_SIZE, Integer.class));
                // This page is small (hundreds of bytes) so there is no point in tracking its memory usage
                return SpooledMetadataBlock.forSpooledLocation(spoolingManager.location(segmentHandle), attributes).serialize();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                overallTimer.end(spoolingTiming);
            }
        }

        private Page inline(Page page)
        {
            OperationTimer overallTimer = new OperationTimer(false);
            try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                DataAttributes attributes = queryDataEncoder.encodeTo(output, List.of(page))
                        .toBuilder()
                        .set(ROWS_COUNT, (long) page.getPositionCount())
                        .build();
                inlinedEncodedBytes.addAndGet(attributes.get(SEGMENT_SIZE, Integer.class));
                return SpooledMetadataBlock.forInlineData(attributes, output.toByteArray()).serialize();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                overallTimer.end(spoolingTiming);
            }
        }

        private void updateMemoryReservation()
        {
            localMemoryContext.setBytes(buffer.getSize() + Optional.ofNullable(outputPage)
                    .map(Page::getRetainedSizeInBytes)
                    .orElse(0L));
        }

        static long reduce(List<Page> page, ToLongFunction<Page> reduce)
        {
            return page.stream()
                    .mapToLong(reduce)
                    .sum();
        }

        @Override
        public void close()
                throws Exception
        {
            aggregatedMemoryContext.close();
            queryDataEncoder.close();
        }
    }

    private static class PageBuffer
    {
        private final List<Page> buffer = new ArrayList<>();

        private PageBuffer()
        {
        }

        public static PageBuffer create()
        {
            return new PageBuffer();
        }

        public synchronized void add(Page page)
        {
            buffer.add(page);
        }

        public boolean isEmpty()
        {
            return buffer.isEmpty();
        }

        public synchronized List<Page> removeAll()
        {
            List<Page> pages = ImmutableList.copyOf(buffer);
            buffer.clear();
            return pages;
        }

        public synchronized long getSize()
        {
            return buffer.stream()
                    .mapToLong(Page::getSizeInBytes)
                    .sum();
        }
    }

    private record OutputSpoolingInfoSupplier(
            OperationTiming spoolingTiming,
            SpoolingController controller,
            AtomicLong inlinedEncodedBytes,
            AtomicLong spooledEncodedBytes)
            implements Supplier<OutputSpoolingInfo>
    {
        private OutputSpoolingInfoSupplier
        {
            requireNonNull(spoolingTiming, "spoolingTiming is null");
            requireNonNull(controller, "controller is null");
            requireNonNull(inlinedEncodedBytes, "inlinedEncodedBytes is null");
            requireNonNull(spooledEncodedBytes, "spooledEncodedBytes is null");
        }

        @Override
        public OutputSpoolingInfo get()
        {
            MetricSnapshot inlined = controller.getMetrics(INLINE);
            MetricSnapshot spooled = controller.getMetrics(SPOOL);

            return new OutputSpoolingInfo(
                    succinctDuration(spoolingTiming.getWallNanos(), NANOSECONDS),
                    succinctDuration(spoolingTiming.getCpuNanos(), NANOSECONDS),
                    inlined.pages(),
                    inlined.positions(),
                    inlined.size(),
                    inlinedEncodedBytes.get(),
                    spooled.pages(),
                    spooled.positions(),
                    spooled.size(),
                    spooledEncodedBytes.get());
        }
    }

    public record OutputSpoolingInfo(
            Duration spoolingWallTime,
            Duration spoolingCpuTime,
            long inlinedPages,
            long inlinedPositions,
            long inlinedRawBytes,
            long inlinedEncodedBytes,
            long spooledPages,
            long spooledPositions,
            long spooledRawBytes,
            long spooledEncodedBytes)
            implements Mergeable<OutputSpoolingInfo>, OperatorInfo
    {
        public OutputSpoolingInfo
        {
            requireNonNull(spoolingWallTime, "spoolingWallTime is null");
            requireNonNull(spoolingCpuTime, "spoolingCpuTime is null");
        }

        @Override
        public OutputSpoolingInfo mergeWith(OutputSpoolingInfo other)
        {
            return new OutputSpoolingInfo(
                    succinctDuration(spoolingWallTime.toMillis() + other.spoolingWallTime().toMillis(), MILLISECONDS),
                    succinctDuration(spoolingCpuTime.toMillis() + other.spoolingCpuTime().toMillis(), MILLISECONDS),
                    inlinedPages + other.inlinedPages(),
                    inlinedPositions + other.inlinedPositions,
                    inlinedRawBytes + other.inlinedRawBytes,
                    inlinedEncodedBytes + other.inlinedEncodedBytes,
                    spooledPages + other.spooledPages,
                    spooledPositions + other.spooledPositions,
                    spooledRawBytes + other.spooledRawBytes,
                    spooledEncodedBytes + other.spooledEncodedBytes);
        }

        @JsonProperty
        public double getEncodedToRawBytesRatio()
        {
            return 1.0 * (spooledEncodedBytes + inlinedEncodedBytes) / (spooledRawBytes + inlinedRawBytes);
        }

        @Override
        public boolean isFinal()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("spoolingWallTime", spoolingWallTime)
                    .add("spoolingCpuTime", spoolingCpuTime)
                    .add("inlinedPages", inlinedPages)
                    .add("inlinedPositions", inlinedPositions)
                    .add("inlinedRawBytes", inlinedRawBytes)
                    .add("inlinedEncodedBytes", inlinedEncodedBytes)
                    .add("spooledPages", spooledPages)
                    .add("spooledPositions", spooledPositions)
                    .add("spooledRawBytes", spooledRawBytes)
                    .add("spooledEncodedBytes", spooledEncodedBytes)
                    .add("encodedToRawBytesRatio", getEncodedToRawBytesRatio())
                    .toString();
        }
    }
}
