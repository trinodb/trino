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

import io.trino.spi.Page;

import static com.google.common.base.Verify.verify;
import static java.lang.Math.clamp;

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

    private Mode mode;

    public OutputSpoolingController(boolean inlineFirstRows, long maximumInlinedPositions, long maximumInlinedSize, long initialSpooledSegmentTarget, long maximumSpooledSegmentTarget)
    {
        this.currentSpooledSegmentTarget = initialSpooledSegmentTarget;
        this.maximumSpooledSegmentTarget = maximumSpooledSegmentTarget;
        this.maximumInlinedPositions = maximumInlinedPositions;
        this.maximumInlinedSize = maximumInlinedSize;

        mode = inlineFirstRows ? Mode.INLINE : Mode.SPOOL;
    }

    public Mode getNextMode(Page page)
    {
        return getNextMode(page.getPositionCount(), page.getSizeInBytes());
    }

    public Mode getNextMode(int positionCount, long sizeInBytes)
    {
        return switch (mode) {
            case INLINE -> {
                // If we still didn't inline maximum number of positions
                if (inlinedPositions + positionCount >= maximumInlinedPositions) {
                    mode = Mode.SPOOL; // switch to spooling mode
                    yield getNextMode(positionCount, sizeInBytes); // and now decide whether to buffer or spool this page
                }

                // We don't want to many inlined segments
                if (inlinedPages > 3) { // or better bound
                    mode = Mode.SPOOL; // switch to spooling mode
                    yield getNextMode(positionCount, sizeInBytes); // and now decide whether to buffer or spool this page
                }

                // If we still didn't inline maximum number of bytes
                if (inlinedRawBytes + sizeInBytes >= maximumInlinedSize) {
                    mode = Mode.SPOOL; // switch to spooling mode
                    yield getNextMode(positionCount, sizeInBytes); // and now decide whether to buffer or spool this page
                }

                verify(bufferedRawSize == 0, "There should be no buffered pages when streaming");
                recordInlined(positionCount, sizeInBytes);
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

    public void recordInlined(int positionCount, long sizeInBytes)
    {
        inlinedPositions += positionCount;
        inlinedPages++;
        inlinedRawBytes += sizeInBytes;
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
