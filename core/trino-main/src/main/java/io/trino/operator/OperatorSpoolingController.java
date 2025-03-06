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

import static com.google.common.base.Verify.verify;
import static io.trino.operator.SpoolingController.Mode.BUFFER;
import static io.trino.operator.SpoolingController.Mode.INLINE;
import static io.trino.operator.SpoolingController.Mode.SPOOL;
import static java.lang.Math.min;

public class OperatorSpoolingController
        implements SpoolingController
{
    private long currentSpooledSegmentTarget;
    private final long maximumSpooledSegmentTarget;

    private final long maximumInlinedPositions;
    private final long maximumInlinedSize;

    private final ActionMetrics spooled = new ActionMetrics();
    private final ActionMetrics inlined = new ActionMetrics();
    private final ActionMetrics buffered = new ActionMetrics();

    private Mode currentMode;

    public OperatorSpoolingController(boolean inlineInitialRows, long maximumInlinedPositions, long maximumInlinedSize, long initialSpooledSegmentTarget, long maximumSpooledSegmentTarget)
    {
        this.currentSpooledSegmentTarget = initialSpooledSegmentTarget;
        this.maximumSpooledSegmentTarget = maximumSpooledSegmentTarget;
        this.maximumInlinedPositions = maximumInlinedPositions;
        this.maximumInlinedSize = maximumInlinedSize;

        currentMode = inlineInitialRows ? INLINE : SPOOL;
    }

    @Override
    public Mode nextMode(int positions, long size)
    {
        return switch (currentMode) {
            case INLINE -> {
                MetricSnapshot snapshot = inlined.snapshot();
                // If we still didn't inline maximum number of positions
                if (snapshot.positions() + positions >= maximumInlinedPositions) {
                    currentMode = SPOOL; // switch to spooling mode
                    yield nextMode(positions, size); // and now decide whether to buffer or spool this page
                }

                // If we still didn't inline maximum number of bytes
                if (snapshot.size() + size >= maximumInlinedSize) {
                    currentMode = SPOOL; // switch to spooling mode
                    yield nextMode(positions, size); // and now decide whether to buffer or spool this page
                }

                verify(buffered.isEmpty(), "There should be no buffered pages when streaming");
                yield execute(INLINE, positions, size); // we are ok to stream this page
            }
            case SPOOL, BUFFER -> {
                MetricSnapshot snapshot = buffered.snapshot();
                if (snapshot.size() + size >= currentSpooledSegmentTarget) {
                    yield execute(SPOOL, positions, size);
                }
                yield execute(BUFFER, positions, size);
            }
        };
    }

    @Override
    public Mode execute(Mode mode, long positions, long size)
    {
        currentMode = mode;
        switch (mode) {
            case BUFFER -> buffered.recordPage(positions, size);
            case INLINE -> inlined.recordPage(positions, size);
            case SPOOL -> {
                MetricSnapshot snapshot = buffered.snapshot();
                buffered.reset();
                spooled.recordPage(positions + snapshot.positions(), size + snapshot.size());
                currentSpooledSegmentTarget = min(currentSpooledSegmentTarget * 2, maximumSpooledSegmentTarget);
            }
        }
        return currentMode;
    }

    public MetricSnapshot spooledMetrics()
    {
        return spooled.snapshot();
    }

    public MetricSnapshot inlinedMetrics()
    {
        return inlined.snapshot();
    }

    public MetricSnapshot bufferedMetrics()
    {
        return buffered.snapshot();
    }

    public long getCurrentSpooledSegmentTarget()
    {
        return currentSpooledSegmentTarget;
    }

    private static class ActionMetrics
    {
        private long pages;
        private long size;
        private long positions;

        public void recordPage(long positions, long size)
        {
            verify(positions > 0, "Expected positions to be non-negative");
            verify(size > 0, "Expected size to be non-negative");
            this.pages++;
            this.positions += positions;
            this.size += size;
        }

        public void reset()
        {
            this.pages = 0;
            this.size = 0;
            this.positions = 0;
        }

        public boolean isEmpty()
        {
            return pages == 0 && size == 0 && positions == 0;
        }

        public MetricSnapshot snapshot()
        {
            return new MetricSnapshot(positions, size, pages);
        }
    }

    public record MetricSnapshot(long positions, long size, long pages)
    {
        public MetricSnapshot
        {
            verify(positions >= 0, "Positions are expected to be non-negative");
            verify(size >= 0, "Size is expected to be non-negative");
            verify(pages >= 0, "Pages are expected to be non-negative");
        }
    }
}
