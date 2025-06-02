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

import java.util.concurrent.atomic.AtomicLong;

import static io.trino.operator.SpoolingController.Mode.INLINE;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.getInliningMaxRows;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.getInliningMaxSize;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.isInliningEnabled;
import static java.util.Objects.requireNonNull;

public class PipelineSpoolingController
        implements SpoolingController
{
    private final SpoolingController next;
    private final AtomicLong totalInlinedPositions;
    private final AtomicLong totalInlinedSize;
    private final boolean inliningEnabled;
    private final long inlinedPositions;
    private final long inlinedSize;

    private final ActionMetrics inlined = new ActionMetrics();

    public PipelineSpoolingController(PipelineContext context, SpoolingController next)
    {
        this(
                context.getInlinedPositions(),
                context.getInlinedSize(),
                isInliningEnabled(context.getSession()),
                getInliningMaxRows(context.getSession()),
                getInliningMaxSize(context.getSession()).toBytes(),
                next);
    }

    PipelineSpoolingController(AtomicLong totalInlinedPositions, AtomicLong totalInlinedSize, boolean inliningEnabled, long inlinedPositions, long inlinedSize, SpoolingController next)
    {
        this.inliningEnabled = inliningEnabled;
        this.inlinedPositions = inlinedPositions;
        this.inlinedSize = inlinedSize;
        this.totalInlinedPositions = requireNonNull(totalInlinedPositions, "totalInlinedPositions is null");
        this.totalInlinedSize = requireNonNull(totalInlinedSize, "totalInlinedSize is null");
        this.next = next;
    }

    @Override
    public Mode nextMode(int positions, long size)
    {
        if (!inliningEnabled) {
            return next.nextMode(positions, size);
        }

        if (totalInlinedPositions.addAndGet(positions) > inlinedPositions) {
            return next.nextMode(positions, size);
        }

        if (totalInlinedSize.addAndGet(size) > inlinedSize) {
            return next.nextMode(positions, size);
        }

        return execute(INLINE, positions, size);
    }

    @Override
    public <T extends SpoolingController> T unwrap(Class<T> clazz)
    {
        if (clazz.equals(PipelineSpoolingController.class)) {
            return (T) this;
        }
        return next.unwrap(clazz);
    }

    @Override
    public Mode execute(Mode mode, long positions, long size)
    {
        if (!inliningEnabled) {
            return next.execute(mode, positions, size);
        }

        switch (mode) {
            case BUFFER, SPOOL -> next.execute(mode, positions, size);
            case INLINE -> inlined.recordPage(positions, size);
        }
        return mode;
    }

    @Override
    public MetricSnapshot getMetrics(Mode mode)
    {
        return switch (mode) {
            case INLINE -> inlined.snapshot();
            case BUFFER, SPOOL -> next.getMetrics(mode);
        };
    }
}
