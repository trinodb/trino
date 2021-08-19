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
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.WindowFrame;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class GroupsFraming
        implements Framing
{
    private final FrameInfo frameInfo;
    private final int partitionStart;
    private final int partitionEnd;
    private final PagesIndex pagesIndex;
    private final Function<Integer, Integer> seekGroupStart;
    private final Function<Integer, Integer> seekGroupEnd;

    // Recently computed frame
    // Along frame start and frame end, it also captures indexes of peer groups
    // where frame bounds fall.
    // This information is used as the starting point when processing the next row.
    // This approach is efficient in case when group offset values are constant,
    // which is assumed to be the most common use case.
    private GroupsFrame recentFrame;

    private int lastPeerGroup = Integer.MAX_VALUE;

    public GroupsFraming(
            FrameInfo frameInfo,
            int partitionStart,
            int partitionEnd,
            PagesIndex pagesIndex,
            PagesHashStrategy peerGroupHashStrategy,
            int initialEnd)
    {
        checkArgument(frameInfo.getType() == WindowFrame.Type.GROUPS, "Frame must be of type GROUPS, actual: %s", frameInfo.getType());

        this.frameInfo = frameInfo;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.pagesIndex = pagesIndex;
        this.recentFrame = new GroupsFrame(0, 0, initialEnd, 0);

        seekGroupStart = position -> {
            requireNonNull(position, "position is null");
            while (position > 0 && pagesIndex.positionNotDistinctFromPosition(peerGroupHashStrategy, partitionStart + position, partitionStart + position - 1)) {
                position--;
            }
            return position;
        };

        seekGroupEnd = position -> {
            requireNonNull(position, "position is null");
            while (position < partitionEnd - 1 - partitionStart && pagesIndex.positionNotDistinctFromPosition(peerGroupHashStrategy, partitionStart + position, partitionStart + position + 1)) {
                position++;
            }
            return position;
        };
    }

    @Override
    public Range getRange(int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd)
    {
        GroupsFrame frame = getFrameRange(currentPosition, currentGroup, peerGroupStart, peerGroupEnd);
        // handle empty frame. If the frame is out of partition bounds, record the nearest valid frame as the 'recentFrame' for the next row.
        if (emptyFrame(frame.getRange())) {
            recentFrame = nearestValidFrame(frame);
            return new Range(-1, -1);
        }

        recentFrame = frame;
        return frame.getRange();
    }

    private GroupsFrame getFrameRange(int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd)
    {
        FrameBound.Type startType = frameInfo.getStartType();
        FrameBound.Type endType = frameInfo.getEndType();

        int start;
        int end;
        int startGroupIndex = GroupsFrame.ignoreIndex();
        int endGroupIndex = GroupsFrame.ignoreIndex();

        switch (startType) {
            case UNBOUNDED_PRECEDING:
                start = 0;
                break;
            case CURRENT_ROW:
                start = peerGroupStart - partitionStart;
                break;
            case PRECEDING: {
                PositionAndGroup frameStart = seek(
                        currentGroup,
                        -getValue(frameInfo.getStartChannel(), currentPosition),
                        recentFrame.getStart(),
                        recentFrame.getStartGroupIndex(),
                        seekGroupStart,
                        lastGroup -> new PositionAndGroup(0, 0));
                start = frameStart.getPosition();
                startGroupIndex = frameStart.getGroup();
                break;
            }
            case FOLLOWING: {
                PositionAndGroup frameStart = seek(
                        currentGroup,
                        getValue(frameInfo.getStartChannel(), currentPosition),
                        recentFrame.getStart(),
                        recentFrame.getStartGroupIndex(),
                        seekGroupStart,
                        lastGroup -> new PositionAndGroup(partitionEnd - partitionStart, GroupsFrame.ignoreIndex()));
                start = frameStart.getPosition();
                startGroupIndex = frameStart.getGroup();
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported frame start type: " + startType);
        }

        switch (endType) {
            case UNBOUNDED_FOLLOWING:
                end = partitionEnd - partitionStart - 1;
                break;
            case CURRENT_ROW:
                end = peerGroupEnd - partitionStart - 1;
                break;
            case PRECEDING: {
                PositionAndGroup frameEnd = seek(
                        currentGroup,
                        -getValue(frameInfo.getEndChannel(), currentPosition),
                        recentFrame.getEnd(),
                        recentFrame.getEndGroupIndex(),
                        seekGroupEnd,
                        lastGroup -> new PositionAndGroup(-1, GroupsFrame.ignoreIndex()));
                end = frameEnd.getPosition();
                endGroupIndex = frameEnd.getGroup();
                break;
            }
            case FOLLOWING: {
                PositionAndGroup frameEnd = seek(
                        currentGroup,
                        getValue(frameInfo.getEndChannel(), currentPosition),
                        recentFrame.getEnd(),
                        recentFrame.getEndGroupIndex(),
                        seekGroupEnd,
                        lastGroup -> new PositionAndGroup(partitionEnd - partitionStart - 1, lastPeerGroup));
                end = frameEnd.getPosition();
                endGroupIndex = frameEnd.getGroup();
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported frame end type: " + endType);
        }

        return new GroupsFrame(start, startGroupIndex, end, endGroupIndex);
    }

    private boolean emptyFrame(Range range)
    {
        return range.getStart() > range.getEnd() ||
                range.getStart() >= partitionEnd - partitionStart ||
                range.getEnd() < 0;
    }

    private static class PositionAndGroup
    {
        private final int position;
        private final int group;

        public PositionAndGroup(int position, int group)
        {
            this.position = position;
            this.group = group;
        }

        public int getPosition()
        {
            return position;
        }

        public int getGroup()
        {
            return group;
        }
    }

    private interface EdgeResultProvider
    {
        PositionAndGroup get(int lastPeerGroup);
    }

    /**
     * Window frame representation for frame of type GROUPS.
     * start, end - first and last row of the frame within window partition
     * startGroupIndex, endGroupIndex - indexes of respective peer groups within partition
     * start points at the first row of startGroupIndex-th peer group
     * end points at the last row of endGroupIndex-th peer group
     */
    private static class GroupsFrame
    {
        private static final int IGNORE_GROUP_INDEX = -1;

        private final int start;
        private final int startGroupIndex;
        private final int end;
        private final int endGroupIndex;

        public GroupsFrame(int start, int startGroupIndex, int end, int endGroupIndex)
        {
            this.start = start;
            this.startGroupIndex = startGroupIndex;
            this.end = end;
            this.endGroupIndex = endGroupIndex;
        }

        public static int ignoreIndex()
        {
            return IGNORE_GROUP_INDEX;
        }

        public GroupsFrame withStart(int start, int startGroupIndex)
        {
            return new GroupsFrame(start, startGroupIndex, this.end, this.endGroupIndex);
        }

        public GroupsFrame withEnd(int end, int endGroupIndex)
        {
            return new GroupsFrame(this.start, this.startGroupIndex, end, endGroupIndex);
        }

        public int getStart()
        {
            return start;
        }

        public int getStartGroupIndex()
        {
            checkState(startGroupIndex != IGNORE_GROUP_INDEX, "accessing ignored group index");
            return startGroupIndex;
        }

        public int getEnd()
        {
            return end;
        }

        public int getEndGroupIndex()
        {
            checkState(endGroupIndex != IGNORE_GROUP_INDEX, "accessing ignored group index");
            return endGroupIndex;
        }

        public Range getRange()
        {
            return new Range(start, end);
        }
    }

    /**
     * Return a valid frame. A frame is valid if its start and end are within partition.
     * If frame start or frame end is out of partition bounds, it is set to the nearest position
     * for which peer group index can be determined.
     */
    private GroupsFrame nearestValidFrame(GroupsFrame frame)
    {
        if (frame.getStart() > partitionEnd - partitionStart - 1) {
            return frame.withStart(partitionEnd - partitionStart - 1, lastPeerGroup);
        }
        if (frame.getEnd() < 0) {
            return frame.withEnd(0, 0);
        }
        return frame;
    }

    private PositionAndGroup seek(
            int currentGroupIndex,
            long offset,
            int recentPosition,
            int recentGroupIndex,
            Function<Integer, Integer> seekPositionWithinGroup,
            EdgeResultProvider edgeResult)
    {
        long searchedIndex = currentGroupIndex + offset;
        if (searchedIndex < 0 || searchedIndex > lastPeerGroup) {
            return edgeResult.get(lastPeerGroup);
        }
        int groupIndex = toIntExact(searchedIndex);
        while (recentGroupIndex > groupIndex) {
            recentPosition = seekGroupStart.apply(recentPosition);
            recentPosition--;
            recentGroupIndex--;
        }

        while (recentGroupIndex < groupIndex) {
            recentPosition = seekGroupEnd.apply(recentPosition);
            if (recentPosition == partitionEnd - partitionStart - 1) {
                lastPeerGroup = recentGroupIndex;
                return edgeResult.get(lastPeerGroup);
            }
            recentPosition++;
            recentGroupIndex++;
        }

        recentPosition = seekPositionWithinGroup.apply(recentPosition);
        if (recentPosition == partitionEnd - partitionStart - 1) {
            lastPeerGroup = recentGroupIndex;
        }
        return new PositionAndGroup(recentPosition, recentGroupIndex);
    }

    private long getValue(int channel, int currentPosition)
    {
        checkState(!pagesIndex.isNull(channel, currentPosition), "Window frame offset must not be null");
        long value = pagesIndex.getLong(channel, currentPosition);
        checkState(value >= 0, "Window frame offset must not be negative");
        return value;
    }
}
