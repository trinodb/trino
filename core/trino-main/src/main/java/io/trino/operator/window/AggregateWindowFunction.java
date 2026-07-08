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
package io.trino.operator.window;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowIndex;

import java.util.function.Supplier;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AggregateWindowFunction
        implements WindowFunction
{
    private final Supplier<WindowAccumulator> accumulatorFactory;
    private final boolean hasRemoveInput;

    private WindowIndex windowIndex;
    private WindowAccumulator accumulator;
    private int currentStart;
    private int currentEnd;

    // When a hole is applied to a removable accumulator (see processRow), the accumulator holds the
    // contiguous [currentStart, currentEnd] with these positions removed (the EXCLUDE hole), except for
    // holeKeptRow, which lies within the hole and is retained (EXCLUDE TIES). The hole is restorable:
    // adding the removed positions back yields the contiguous frame, so consecutive excluded frames
    // slide incrementally instead of rebuilding. holeStart > holeEnd means no hole is applied.
    private int holeStart;
    private int holeEnd = -1;
    private int holeKeptRow = -1;

    // set when the accumulator holds a non-contiguous frame that was rebuilt from scratch (a hole on an
    // accumulator that could not remove input, or an order-sensitive accumulator) and so cannot be
    // reused by the incremental sliding path; forces a fresh rebuild on the next row
    private boolean accumulatorHasExcludedFrame;

    public AggregateWindowFunction(Supplier<WindowAccumulator> accumulatorFactory, boolean hasRemoveInput)
    {
        this.accumulatorFactory = requireNonNull(accumulatorFactory, "accumulatorFactory is null");
        this.hasRemoveInput = hasRemoveInput;
    }

    @Override
    public void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        resetAccumulator();
    }

    @Override
    public void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd, int excludedStart, int excludedEnd, int keptRow)
    {
        boolean hasHole = excludedStart <= excludedEnd;

        if (hasHole && !hasRemoveInput) {
            // The accumulator cannot remove input (and may be order-sensitive, e.g. array_agg), so the
            // non-contiguous frame has to be rebuilt from scratch in frame order every row.
            buildExcludedFrame(frameStart, frameEnd, excludedStart, excludedEnd, keptRow);
            accumulator.output(output);
            return;
        }

        // Restore the accumulator to the contiguous [currentStart, currentEnd] by adding back any hole
        // applied for the previous row, so the incremental sliding logic below can treat it as contiguous.
        if (holeStart <= holeEnd) {
            addHole(holeStart, holeEnd, holeKeptRow);
            clearHole();
        }

        if (frameStart < 0) {
            // empty frame
            resetAccumulator();
        }
        else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
            // same or expanding frame
            accumulate(currentEnd + 1, frameEnd);
            currentEnd = frameEnd;
        }
        else {
            buildNewFrame(frameStart, frameEnd);
        }

        if (hasHole) {
            // Punch the hole back out of the now-contiguous frame. This keeps common exclusion queries
            // (e.g. EXCLUDE CURRENT ROW over a fixed frame) at the cost of one hole removal per row rather
            // than a full O(frame) rebuild. If the accumulator refuses the removal, fall back to a rebuild.
            if (removeHole(excludedStart, excludedEnd, keptRow)) {
                holeStart = excludedStart;
                holeEnd = excludedEnd;
                holeKeptRow = keptRow;
            }
            else {
                buildExcludedFrame(frameStart, frameEnd, excludedStart, excludedEnd, keptRow);
            }
        }

        accumulator.output(output);
    }

    private void buildExcludedFrame(int frameStart, int frameEnd, int excludedStart, int excludedEnd, int keptRow)
    {
        resetAccumulator();
        // Accumulate the surviving positions in frame order so that order-sensitive aggregates (e.g. array_agg)
        // see them in the right order: the positions before the excluded range, then the kept row (EXCLUDE TIES),
        // which sits within the excluded range, then the positions after the excluded range.
        if (frameStart <= excludedStart - 1) {
            accumulate(frameStart, excludedStart - 1);
        }
        if (keptRow >= 0) {
            accumulate(keptRow, keptRow);
        }
        if (excludedEnd + 1 <= frameEnd) {
            accumulate(excludedEnd + 1, frameEnd);
        }
        accumulatorHasExcludedFrame = true;
    }

    // Add back the positions of a hole [start, end] previously removed from the accumulator, skipping keptRow
    // (which was retained, so was never removed). keptRow, when >= 0, lies within [start, end].
    private void addHole(int start, int end, int keptRow)
    {
        if (keptRow >= start && keptRow <= end) {
            if (start <= keptRow - 1) {
                accumulate(start, keptRow - 1);
            }
            if (keptRow + 1 <= end) {
                accumulate(keptRow + 1, end);
            }
        }
        else {
            accumulate(start, end);
        }
    }

    // Remove the positions of a hole [start, end] from the accumulator, keeping keptRow (EXCLUDE TIES).
    // Returns false if the accumulator could not remove some position and must be rebuilt.
    private boolean removeHole(int start, int end, int keptRow)
    {
        if (keptRow >= start && keptRow <= end) {
            if (start <= keptRow - 1 && !remove(start, keptRow - 1)) {
                return false;
            }
            return keptRow + 1 > end || remove(keptRow + 1, end);
        }
        return remove(start, end);
    }

    private void clearHole()
    {
        holeStart = 0;
        holeEnd = -1;
        holeKeptRow = -1;
    }

    private void buildNewFrame(int frameStart, int frameEnd)
    {
        if (hasRemoveInput) {
            // Note that all the start/end intervals are inclusive on both ends!
            if (currentStart < 0) {
                currentStart = 0;
                currentEnd = -1;
            }
            int overlapStart = max(frameStart, currentStart);
            int overlapEnd = min(frameEnd, currentEnd);
            int prefixRemoveLength = overlapStart - currentStart;
            int suffixRemoveLength = currentEnd - overlapEnd;

            if ((overlapEnd - overlapStart + 1) > (prefixRemoveLength + suffixRemoveLength)) {
                // It's worth keeping the overlap, and removing the now-unused prefix
                if (currentStart < frameStart && !remove(currentStart, frameStart - 1)) {
                    resetNewFrame(frameStart, frameEnd);
                    return;
                }
                if (frameEnd < currentEnd && !remove(frameEnd + 1, currentEnd)) {
                    resetNewFrame(frameStart, frameEnd);
                    return;
                }
                if (frameStart < currentStart) {
                    accumulate(frameStart, currentStart - 1);
                }
                if (currentEnd < frameEnd) {
                    accumulate(currentEnd + 1, frameEnd);
                }
                currentStart = frameStart;
                currentEnd = frameEnd;
                return;
            }
        }

        // We couldn't or didn't want to modify the accumulation: instead, discard the current accumulation and start fresh.
        resetNewFrame(frameStart, frameEnd);
    }

    private void resetNewFrame(int frameStart, int frameEnd)
    {
        resetAccumulator();
        accumulate(frameStart, frameEnd);
        currentStart = frameStart;
        currentEnd = frameEnd;
    }

    private void accumulate(int start, int end)
    {
        accumulator.addInput(windowIndex, start, end);
    }

    private boolean remove(int start, int end)
    {
        return accumulator.removeInput(windowIndex, start, end);
    }

    private void resetAccumulator()
    {
        // currentStart >= 0 means the accumulator holds a contiguous frame; accumulatorHasExcludedFrame means it
        // holds a non-contiguous (EXCLUDE) frame while currentStart is -1. Either way there is state to discard.
        if (currentStart >= 0 || accumulatorHasExcludedFrame) {
            accumulator = accumulatorFactory.get();
            currentStart = -1;
            currentEnd = -1;
            accumulatorHasExcludedFrame = false;
            clearHole();
        }
    }
}
