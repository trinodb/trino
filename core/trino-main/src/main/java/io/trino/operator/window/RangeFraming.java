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

import io.trino.operator.PagesHashStrategy;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexComparator;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.WindowFrame;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.PRECEDING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;

public class RangeFraming
        implements Framing
{
    private final FrameInfo frameInfo;
    private final PagesIndexComparator startComparator;
    private final PagesIndexComparator endComparator;
    private final PagesIndex pagesIndex;
    private final PagesHashStrategy peerGroupHashStrategy;
    private final int partitionStart;
    private final int partitionEnd;

    // Recently computed frame bounds.
    // When computing frame start and frame end for a row, frame bounds for the previous row
    // are used as the starting point. Then they are moved backward or forward based on the sort order
    // until the matching position for a current row is found.
    // This approach is efficient in case when frame offset values are constant. It was chosen
    // based on the assumption that in most use cases frame offset is constant rather than
    // row-dependent.
    private Range recentRange;

    public RangeFraming(
            FrameInfo frameInfo,
            int partitionStart,
            int partitionEnd,
            PagesIndexComparator startComparator,
            PagesIndexComparator endComparator,
            PagesIndex pagesIndex,
            PagesHashStrategy peerGroupHashStrategy,
            Range initialRange)
    {
        checkArgument(frameInfo.getType() == WindowFrame.Type.RANGE, "Frame must be of type RANGE, actual: %s", frameInfo.getType());

        this.frameInfo = frameInfo;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.startComparator = startComparator;
        this.endComparator = endComparator;
        this.pagesIndex = pagesIndex;
        this.peerGroupHashStrategy = peerGroupHashStrategy;
        this.recentRange = initialRange;
    }

    @Override
    public Range getRange(int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd)
    {
        Framing.Range range = getFrameRange(currentPosition, peerGroupStart, peerGroupEnd);

        // handle empty frame. If the frame is out of partition bounds, record the nearest valid frame as the 'recentRange' for the next row.
        if (emptyFrame(range)) {
            recentRange = nearestValidFrame(range);
            return new Range(-1, -1);
        }

        recentRange = range;
        return range;
    }

    private Range getFrameRange(int currentPosition, int peerGroupStart, int peerGroupEnd)
    {
        // full partition
        if ((frameInfo.getStartType() == UNBOUNDED_PRECEDING && frameInfo.getEndType() == UNBOUNDED_FOLLOWING)) {
            return new Range(0, partitionEnd - partitionStart - 1);
        }

        // frame defined by peer group
        if (frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == CURRENT_ROW ||
                frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == UNBOUNDED_FOLLOWING ||
                frameInfo.getStartType() == UNBOUNDED_PRECEDING && frameInfo.getEndType() == CURRENT_ROW) {
            // same peer group as recent row
            if (currentPosition == partitionStart || pagesIndex.positionNotDistinctFromPosition(peerGroupHashStrategy, currentPosition - 1, currentPosition)) {
                return recentRange;
            }
            // next peer group
            return new Range(
                    frameInfo.getStartType() == UNBOUNDED_PRECEDING ? 0 : peerGroupStart - partitionStart,
                    frameInfo.getEndType() == UNBOUNDED_FOLLOWING ? partitionEnd - partitionStart - 1 : peerGroupEnd - partitionStart - 1);
        }

        // at this point, frame definition has at least one of: X PRECEDING, Y FOLLOWING
        // 1. leading or trailing nulls: frame consists of nulls peer group, possibly extended to partition start / end.
        // according to Spec, behavior of "X PRECEDING", "X FOLLOWING" frame boundaries is similar to "CURRENT ROW" for null values.
        if (pagesIndex.isNull(frameInfo.getSortKeyChannel(), currentPosition)) {
            return new Range(
                    frameInfo.getStartType() == UNBOUNDED_PRECEDING ? 0 : peerGroupStart - partitionStart,
                    frameInfo.getEndType() == UNBOUNDED_FOLLOWING ? partitionEnd - partitionStart - 1 : peerGroupEnd - partitionStart - 1);
        }

        // 2. non-null value in current row. Find frame boundaries starting from recentRange
        int frameStart;
        if (frameInfo.getStartType() == UNBOUNDED_PRECEDING) {
            frameStart = 0;
        }
        else if (frameInfo.getStartType() == CURRENT_ROW) {
            frameStart = peerGroupStart - partitionStart;
        }
        else if (frameInfo.getStartType() == PRECEDING) {
            frameStart = getFrameStartPreceding(currentPosition);
        }
        else {
            // frameInfo.getStartType() == FOLLOWING
            // note: this is the only case where frameStart might get out of partition bound
            frameStart = getFrameStartFollowing(currentPosition);
        }

        int frameEnd;
        if (frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
            frameEnd = partitionEnd - partitionStart - 1;
        }
        else if (frameInfo.getEndType() == CURRENT_ROW) {
            frameEnd = peerGroupEnd - partitionStart - 1;
        }
        else if (frameInfo.getEndType() == PRECEDING) {
            // note: this is the only case where frameEnd might get out of partition bound
            frameEnd = getFrameEndPreceding(currentPosition);
        }
        else {
            // frameInfo.getEndType() == FOLLOWING
            frameEnd = getFrameEndFollowing(currentPosition);
        }

        return new Range(frameStart, frameEnd);
    }

    private int getFrameStartPreceding(int currentPosition)
    {
        int sortKeyChannel = frameInfo.getSortKeyChannelForStartComparison();
        SortItem.Ordering ordering = frameInfo.getOrdering().get();

        int recent = recentRange.getStart();

        // If the recent frame start points at a null, it means that we are now processing first non-null position.
        // For frame start "X PRECEDING", the frame starts at the first null for all null values, and it never includes nulls for non-null values.
        if (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + recent)) {
            return currentPosition - partitionStart;
        }

        return seek(
                currentPosition,
                startComparator,
                sortKeyChannel,
                recent,
                -1,
                ordering == DESCENDING,
                0,
                p -> false);
    }

    private int getFrameStartFollowing(int currentPosition)
    {
        int sortKeyChannel = frameInfo.getSortKeyChannelForStartComparison();
        SortItem.Ordering ordering = frameInfo.getOrdering().get();

        int recent = recentRange.getStart();
        int position = recent;

        // If the recent frame start points at the beginning of partition and it is null, it means that we are now processing first non-null position.
        // frame start for first non-null position - leave section of leading nulls
        if (recent == 0 && pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart)) {
            position = currentPosition - partitionStart;
        }
        // leave section of trailing nulls
        while (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + position)) {
            position--;
        }

        return seek(
                currentPosition,
                startComparator,
                sortKeyChannel,
                position,
                -1,
                ordering == DESCENDING,
                0,
                p -> p >= partitionEnd - partitionStart || pagesIndex.isNull(sortKeyChannel, partitionStart + p));
    }

    private int getFrameEndPreceding(int currentPosition)
    {
        int sortKeyChannel = frameInfo.getSortKeyChannelForEndComparison();
        SortItem.Ordering ordering = frameInfo.getOrdering().get();

        int position = recentRange.getEnd();

        // leave section of leading nulls
        while (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + position)) {
            position++;
        }

        return seek(
                currentPosition,
                endComparator,
                sortKeyChannel,
                position,
                1,
                ordering == ASCENDING,
                partitionEnd - 1 - partitionStart,
                p -> p < 0 || pagesIndex.isNull(sortKeyChannel, partitionStart + p));
    }

    private int getFrameEndFollowing(int currentPosition)
    {
        SortItem.Ordering ordering = frameInfo.getOrdering().get();
        int sortKeyChannel = frameInfo.getSortKeyChannelForEndComparison();

        int recent = recentRange.getEnd();
        int position = recent;

        // frame end for first non-null position - leave section of leading nulls
        if (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + recent)) {
            position = currentPosition - partitionStart;
        }

        return seek(
                currentPosition,
                endComparator,
                sortKeyChannel,
                position,
                1,
                ordering == ASCENDING,
                partitionEnd - 1 - partitionStart,
                p -> false);
    }

    // This method assumes that `sortKeyChannel` is not null at `position`
    private int seek(
            int currentPosition,
            PagesIndexComparator comparator,
            int sortKeyChannel,
            int position,
            int step,
            boolean reverse,
            int limit,
            Predicate<Integer> bound)
    {
        int comparison = compare(comparator, partitionStart + position, currentPosition, reverse);
        while (comparison < 0) {
            position -= step;

            if (bound.test(position)) {
                return position;
            }

            comparison = compare(comparator, partitionStart + position, currentPosition, reverse);
        }
        while (true) {
            if (position == limit || pagesIndex.isNull(sortKeyChannel, partitionStart + position + step)) {
                break;
            }
            int newComparison = compare(comparator, partitionStart + position + step, currentPosition, reverse);
            if (newComparison >= 0) {
                position += step;
            }
            else {
                break;
            }
        }

        return position;
    }

    private int compare(PagesIndexComparator comparator, int left, int right, boolean reverse)
    {
        int result = comparator.compareTo(pagesIndex, left, right);

        if (reverse) {
            return -result;
        }

        return result;
    }

    private boolean emptyFrame(Range range)
    {
        return range.getStart() > range.getEnd() ||
                range.getStart() >= partitionEnd - partitionStart ||
                range.getEnd() < 0;
    }

    /**
     * Return the nearest valid frame. A frame is valid if its start and end are within partition.
     * Note: A valid frame might be empty i.e. its end might be before its start.
     */
    private Range nearestValidFrame(Range range)
    {
        return new Range(
                Math.min(partitionEnd - partitionStart - 1, range.getStart()),
                Math.max(0, range.getEnd()));
    }
}
