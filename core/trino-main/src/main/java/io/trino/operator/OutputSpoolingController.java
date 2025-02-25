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

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.Page;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;
import static java.lang.Math.clamp;
import static java.util.Objects.requireNonNull;

public class OutputSpoolingController
{
    public enum Mode
    {
        INLINE,
        BUFFER,
        SPOOL
    }

    private long currentSpooledSegmentTarget;
    private final long maximumSpooledSegmentTarget;

    private final long maximumInlinedPositions;
    private final long maximumInlinedSize;

    private long spooledPositions;
    private long spooledPages;
    private long spooledRawBytes;
    private long spooledEncodedBytes;

    private long inlinedPositions;
    private long inlinedPages;
    private long inlinedRawBytes;
    private long bufferedRawSize;
    private long bufferedPositions;

    private Mode currentMode;

    public OutputSpoolingController(
            boolean inlineInitialRows,
            long maximumInlinedPositions,
            long maximumInlinedSize,
            long initialSpooledSegmentTarget,
            long maximumSpooledSegmentTarget)
    {
        this.currentSpooledSegmentTarget = initialSpooledSegmentTarget;
        this.maximumSpooledSegmentTarget = maximumSpooledSegmentTarget;
        this.maximumInlinedPositions = maximumInlinedPositions;
        this.maximumInlinedSize = maximumInlinedSize;

        currentMode = inlineInitialRows ? Mode.INLINE : Mode.SPOOL;
    }

    public Mode getNextMode(PipelineContext pipelineContext, Page page)
    {
        requireNonNull(pipelineContext, "pipelineContext is null");
        return getNextMode(
                pipelineContext.getInlinedPositions(),
                pipelineContext.getInlinedRawSize(),
                page.getPositionCount(),
                page.getSizeInBytes());
    }

    @VisibleForTesting
    Mode getNextMode(AtomicLong totalInlinedPositions, AtomicLong totalInlinedBytes, int positionCount, long sizeInBytes)
    {
        return switch (currentMode) {
            case INLINE -> {
                // If we still didn't inline maximum number of positions
                if (!canInlinePositions(totalInlinedPositions.get(), positionCount)) {
                    currentMode = Mode.SPOOL; // switch to spooling mode
                    yield getNextMode(totalInlinedPositions, totalInlinedBytes, positionCount, sizeInBytes); // and now decide whether to buffer or spool this page
                }

                // If we still didn't inline maximum number of bytes
                if (!canInlineBytes(totalInlinedBytes.get(), sizeInBytes)) {
                    currentMode = Mode.SPOOL; // switch to spooling mode
                    yield getNextMode(totalInlinedPositions, totalInlinedBytes, positionCount, sizeInBytes); // and now decide whether to buffer or spool this page
                }

                verify(bufferedRawSize == 0, "There should be no buffered pages when streaming");
                recordInlined(totalInlinedPositions, totalInlinedBytes, positionCount, sizeInBytes);
                yield Mode.INLINE; // we are ok to stream this page
            }
            case SPOOL -> {
                if (bufferedRawSize + sizeInBytes >= currentSpooledSegmentTarget) {
                    recordSpooled(bufferedPositions + positionCount, bufferedRawSize + sizeInBytes);
                    yield Mode.SPOOL;
                }

                recordBuffered(positionCount, sizeInBytes);
                yield Mode.BUFFER;
            }

            case BUFFER -> throw new IllegalStateException("Current mode can be either STREAM or SPOOL");
        };
    }

    private boolean canInlinePositions(long totalInlinedPositions, long positionCount)
    {
        if (inlinedPositions + positionCount >= maximumInlinedPositions) {
            return false;
        }

        return totalInlinedPositions + positionCount < maximumInlinedPositions;
    }

    private boolean canInlineBytes(long totalInlinedBytes, long sizeInBytes)
    {
        if (inlinedRawBytes + sizeInBytes >= maximumInlinedSize) {
            return false;
        }

        return totalInlinedBytes + sizeInBytes < maximumInlinedSize;
    }

    public void recordSpooled(long rows, long size)
    {
        bufferedRawSize = 0;
        bufferedPositions = 0; // Buffer cleared when spooled

        spooledPositions += rows;
        spooledRawBytes += size;
        spooledPages++;

        // Double spool target size until we reach maximum
        currentSpooledSegmentTarget = clamp(currentSpooledSegmentTarget * 2, currentSpooledSegmentTarget, maximumSpooledSegmentTarget);
    }

    public void recordEncoded(long encodedSize)
    {
        spooledEncodedBytes += encodedSize;
    }

    public void recordInlined(AtomicLong totalInlinedPositions, AtomicLong totalInlinedBytes, int positionCount, long sizeInBytes)
    {
        inlinedPositions += positionCount;
        inlinedPages++;
        inlinedRawBytes += sizeInBytes;

        // Increase per-pipeline counters
        totalInlinedPositions.addAndGet(positionCount);
        totalInlinedBytes.addAndGet(sizeInBytes);
    }

    public void recordBuffered(int positionCount, long sizeInBytes)
    {
        bufferedPositions += positionCount;
        bufferedRawSize += sizeInBytes;
    }

    public long getSpooledPositions()
    {
        return spooledPositions;
    }

    public long getSpooledPages()
    {
        return spooledPages;
    }

    public long getSpooledRawBytes()
    {
        return spooledRawBytes;
    }

    public long getSpooledEncodedBytes()
    {
        return spooledEncodedBytes;
    }

    public long getInlinedPositions()
    {
        return inlinedPositions;
    }

    public long getInlinedPages()
    {
        return inlinedPages;
    }

    public long getInlinedRawBytes()
    {
        return inlinedRawBytes;
    }

    public long getBufferedRawSize()
    {
        return bufferedRawSize;
    }

    public long getBufferedPositions()
    {
        return bufferedPositions;
    }

    public long getCurrentSpooledSegmentTarget()
    {
        return currentSpooledSegmentTarget;
    }
}
