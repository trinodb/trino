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

import static io.trino.operator.SpoolingController.Mode.BUFFER;
import static io.trino.operator.SpoolingController.Mode.SPOOL;
import static java.lang.Math.min;

public class OperatorSpoolingController
        implements SpoolingController
{
    private long currentSpooledSegmentTarget;
    private final long maximumSpooledSegmentTarget;

    private final ActionMetrics spooled = new ActionMetrics();
    private final ActionMetrics buffered = new ActionMetrics();

    private Mode currentMode = BUFFER;

    public OperatorSpoolingController(long initialSpooledSegmentTarget, long maximumSpooledSegmentTarget)
    {
        this.currentSpooledSegmentTarget = initialSpooledSegmentTarget;
        this.maximumSpooledSegmentTarget = maximumSpooledSegmentTarget;
    }

    @Override
    public Mode nextMode(int positions, long size)
    {
        return switch (currentMode) {
            case INLINE -> throw new IllegalStateException("OperatorSpoolingController does not handle INLINE mode");
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
            case SPOOL -> {
                MetricSnapshot snapshot = buffered.snapshot();
                buffered.reset();
                spooled.recordPage(positions + snapshot.positions(), size + snapshot.size());
                currentSpooledSegmentTarget = min(currentSpooledSegmentTarget * 2, maximumSpooledSegmentTarget);
            }
            case INLINE -> throw new IllegalStateException("OperatorSpoolingController does not handle INLINE mode");
        }
        return currentMode;
    }

    @Override
    public MetricSnapshot getMetrics(Mode mode)
    {
        return switch (mode) {
            case BUFFER -> buffered.snapshot();
            case SPOOL -> spooled.snapshot();
            case INLINE -> throw new IllegalStateException("OperatorSpoolingController does not handle INLINE mode");
        };
    }

    public long getCurrentSpooledSegmentTarget()
    {
        return currentSpooledSegmentTarget;
    }
}
