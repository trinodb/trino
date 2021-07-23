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

import io.trino.operator.PagesIndex;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.WindowFrame;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.tree.FrameBound.Type.FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.PRECEDING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static java.lang.Math.toIntExact;

public class RowsFraming
        implements Framing
{
    private final FrameInfo frameInfo;
    private final PagesIndex pagesIndex;
    private final int partitionStart;
    private final int partitionEnd;

    public RowsFraming(FrameInfo frameInfo, int partitionStart, int partitionEnd, PagesIndex pagesIndex)
    {
        checkArgument(frameInfo.getType() == WindowFrame.Type.ROWS, "Frame must be of type ROWS, actual: %s", frameInfo.getType());

        this.frameInfo = frameInfo;
        this.pagesIndex = pagesIndex;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
    }

    @Override
    public Range getRange(int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd)
    {
        int rowPosition = currentPosition - partitionStart;
        int endPosition = partitionEnd - partitionStart - 1;

        // handle empty frame
        if (emptyFrame(frameInfo, rowPosition, endPosition)) {
            return new Range(-1, -1);
        }

        int frameStart;
        int frameEnd;

        // frame start
        if (frameInfo.getStartType() == UNBOUNDED_PRECEDING) {
            frameStart = 0;
        }
        else if (frameInfo.getStartType() == PRECEDING) {
            frameStart = preceding(rowPosition, getValue(frameInfo.getStartChannel(), currentPosition));
        }
        else if (frameInfo.getStartType() == FOLLOWING) {
            frameStart = following(rowPosition, endPosition, getValue(frameInfo.getStartChannel(), currentPosition));
        }
        else {
            frameStart = rowPosition;
        }

        // frame end
        if (frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
            frameEnd = endPosition;
        }
        else if (frameInfo.getEndType() == PRECEDING) {
            frameEnd = preceding(rowPosition, getValue(frameInfo.getEndChannel(), currentPosition));
        }
        else if (frameInfo.getEndType() == FOLLOWING) {
            frameEnd = following(rowPosition, endPosition, getValue(frameInfo.getEndChannel(), currentPosition));
        }
        else {
            frameEnd = rowPosition;
        }

        return new Range(frameStart, frameEnd);
    }

    private boolean emptyFrame(FrameInfo frameInfo, int rowPosition, int endPosition)
    {
        FrameBound.Type startType = frameInfo.getStartType();
        FrameBound.Type endType = frameInfo.getEndType();

        int positions = endPosition - rowPosition;

        if ((startType == UNBOUNDED_PRECEDING) && (endType == PRECEDING)) {
            return getValue(frameInfo.getEndChannel(), 0) > rowPosition;
        }

        if ((startType == FOLLOWING) && (endType == UNBOUNDED_FOLLOWING)) {
            return getValue(frameInfo.getStartChannel(), 0) > positions;
        }

        if (startType != endType) {
            return false;
        }

        FrameBound.Type type = frameInfo.getStartType();
        if ((type != PRECEDING) && (type != FOLLOWING)) {
            return false;
        }

        long start = getValue(frameInfo.getStartChannel(), 0);
        long end = getValue(frameInfo.getEndChannel(), 0);

        if (type == PRECEDING) {
            return (start < end) || ((start > rowPosition) && (end > rowPosition));
        }

        return (start > end) || (start > positions);
    }

    private static int preceding(int rowPosition, long value)
    {
        if (value > rowPosition) {
            return 0;
        }
        return toIntExact(rowPosition - value);
    }

    private static int following(int rowPosition, int endPosition, long value)
    {
        if (value > (endPosition - rowPosition)) {
            return endPosition;
        }
        return toIntExact(rowPosition + value);
    }

    private long getValue(int channel, int currentPosition)
    {
        checkState(!pagesIndex.isNull(channel, currentPosition), "Window frame offset must not be null");
        long value = pagesIndex.getLong(channel, currentPosition);
        checkState(value >= 0, "Window frame offset must not be negative");
        return value;
    }
}
