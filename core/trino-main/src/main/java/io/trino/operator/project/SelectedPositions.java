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
package io.trino.operator.project;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.System.arraycopy;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class SelectedPositions
{
    private static final long INSTANCE_SIZE = instanceSize(SelectedPositions.class);
    private static final SelectedPositions EMPTY = positionsRange(0, 0);

    private final boolean isList;
    private final int[] positions;
    private final int offset;
    private final int size;

    public static SelectedPositions positionsList(int[] positions, int offset, int size)
    {
        return new SelectedPositions(true, positions, offset, size);
    }

    public static SelectedPositions positionsRange(int offset, int size)
    {
        return new SelectedPositions(false, new int[0], offset, size);
    }

    private SelectedPositions(boolean isList, int[] positions, int offset, int size)
    {
        this.isList = isList;
        this.positions = requireNonNull(positions, "positions is null");
        this.offset = offset;
        this.size = size;

        checkArgument(offset >= 0, "offset is negative");
        checkArgument(size >= 0, "size is negative");
        if (isList) {
            checkPositionIndexes(offset, offset + size, positions.length);
        }
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(positions);
    }

    public boolean isList()
    {
        return isList;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int[] getPositions()
    {
        checkState(isList, "SelectedPositions is a range");
        return positions;
    }

    public int getOffset()
    {
        return offset;
    }

    public int size()
    {
        return size;
    }

    public SelectedPositions subRange(int start, int end)
    {
        checkPositionIndexes(start, end, size);

        int newOffset = this.offset + start;
        int newLength = end - start;
        return new SelectedPositions(isList, positions, newOffset, newLength);
    }

    /**
     * Returns a new SelectedPositions containing all the positions which are not present in `otherPositions`
     */
    public SelectedPositions difference(SelectedPositions otherPositions)
    {
        if (isEmpty() || otherPositions.isEmpty()) {
            return this;
        }

        int resultPositionsCount = 0;
        if (isList && otherPositions.isList()) {
            int[] result = new int[size];
            int currentIndex = offset;
            int endIndex = offset + size;
            int currentPosition = positions[currentIndex];

            int[] otherPositionsList = otherPositions.getPositions();
            int otherCurrentIndex = otherPositions.getOffset();
            int otherEndIndex = otherPositions.getOffset() + otherPositions.size();
            int otherCurrentPosition = otherPositionsList[otherCurrentIndex];

            while (true) {
                if (currentPosition < otherCurrentPosition) {
                    result[resultPositionsCount++] = currentPosition;
                    currentIndex++;
                    if (currentIndex >= endIndex) {
                        break;
                    }
                    currentPosition = positions[currentIndex];
                }
                else {
                    if (currentPosition == otherCurrentPosition) {
                        currentIndex++;
                        if (currentIndex >= endIndex) {
                            break;
                        }
                        currentPosition = positions[currentIndex];
                    }

                    otherCurrentIndex++;
                    if (otherCurrentIndex >= otherEndIndex) {
                        resultPositionsCount = copyList(positions, currentIndex, endIndex, result, resultPositionsCount);
                        return positionsList(result, 0, resultPositionsCount);
                    }
                    otherCurrentPosition = otherPositionsList[otherCurrentIndex];
                }
            }
            return positionsList(result, 0, resultPositionsCount);
        }
        else if (!isList && otherPositions.isList()) {
            int[] result = new int[size];
            int currentPosition = offset;
            int endPosition = offset + size;

            int[] otherPositionsList = otherPositions.getPositions();
            int otherCurrentIndex = otherPositions.getOffset();
            int otherEndIndex = otherPositions.getOffset() + otherPositions.size();
            int otherCurrentPosition = otherPositionsList[otherCurrentIndex];

            while (true) {
                if (currentPosition < otherCurrentPosition) {
                    result[resultPositionsCount++] = currentPosition;
                    currentPosition++;
                    if (currentPosition >= endPosition) {
                        break;
                    }
                }
                else {
                    if (currentPosition == otherCurrentPosition) {
                        currentPosition++;
                        if (currentPosition >= endPosition) {
                            break;
                        }
                    }
                    otherCurrentIndex++;
                    if (otherCurrentIndex >= otherEndIndex) {
                        resultPositionsCount = copyRange(currentPosition, endPosition, result, resultPositionsCount);
                        return positionsList(result, 0, resultPositionsCount);
                    }
                    otherCurrentPosition = otherPositionsList[otherCurrentIndex];
                }
            }
            return positionsList(result, 0, resultPositionsCount);
        }
        else if (isList && !otherPositions.isList()) {
            int[] result = new int[size];
            int currentIndex = offset;
            int endIndex = offset + size;
            int currentPosition = positions[currentIndex];

            int otherCurrentPosition = otherPositions.getOffset();
            int otherEndPosition = otherPositions.getOffset() + otherPositions.size();

            while (true) {
                if (currentPosition < otherCurrentPosition) {
                    result[resultPositionsCount++] = currentPosition;
                    currentIndex++;
                    if (currentIndex >= endIndex) {
                        break;
                    }
                    currentPosition = positions[currentIndex];
                }
                else {
                    if (currentPosition == otherCurrentPosition) {
                        currentIndex++;
                        if (currentIndex >= endIndex) {
                            break;
                        }
                        currentPosition = positions[currentIndex];
                    }
                    otherCurrentPosition++;
                    if (otherCurrentPosition >= otherEndPosition) {
                        resultPositionsCount = copyList(positions, currentIndex, endIndex, result, resultPositionsCount);
                        return positionsList(result, 0, resultPositionsCount);
                    }
                }
            }
            return positionsList(result, 0, resultPositionsCount);
        }

        verify(!isList && !otherPositions.isList());
        // both are ranges
        int endPosition = offset + size;
        int otherEndPosition = otherPositions.getOffset() + otherPositions.size();

        if (!overlaps(offset, size, otherPositions.getOffset(), otherPositions.size())) {
            return this;
        }

        if (offset < otherPositions.getOffset()) {
            if (endPosition <= otherEndPosition) {
                return positionsRange(offset, otherPositions.getOffset() - offset);
            }
            int[] result = new int[size - otherPositions.size()];
            resultPositionsCount = copyRange(offset, otherPositions.getOffset(), result, resultPositionsCount);
            resultPositionsCount = copyRange(otherEndPosition, endPosition, result, resultPositionsCount);
            return positionsList(result, 0, resultPositionsCount);
        }
        // otherPositions.getOffset() <= offset
        if (otherEndPosition < endPosition) {
            return positionsRange(otherEndPosition, endPosition - otherEndPosition);
        }
        return EMPTY;
    }

    public SelectedPositions union(SelectedPositions otherPositions)
    {
        if (otherPositions.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return otherPositions;
        }

        if (isList && otherPositions.isList()) {
            int[] result = new int[size + otherPositions.size()];
            int resultPositionsCount = 0;
            int currentIndex = offset;
            int endIndex = offset + size;
            int currentPosition = positions[currentIndex];

            int[] otherPositionsList = otherPositions.getPositions();
            int otherCurrentIndex = otherPositions.getOffset();
            int otherEndIndex = otherPositions.getOffset() + otherPositions.size();
            int otherCurrentPosition = otherPositionsList[otherCurrentIndex];

            while (true) {
                if (currentPosition < otherCurrentPosition) {
                    result[resultPositionsCount++] = currentPosition;
                    currentIndex++;

                    if (currentIndex >= endIndex) {
                        resultPositionsCount = copyList(otherPositionsList, otherCurrentIndex, otherEndIndex, result, resultPositionsCount);
                        return positionsList(result, 0, resultPositionsCount);
                    }
                    currentPosition = positions[currentIndex];
                }
                else if (currentPosition == otherCurrentPosition) {
                    result[resultPositionsCount++] = currentPosition;
                    currentIndex++;
                    otherCurrentIndex++;

                    if (currentIndex >= endIndex) {
                        resultPositionsCount = copyList(otherPositionsList, otherCurrentIndex, otherEndIndex, result, resultPositionsCount);
                        return positionsList(result, 0, resultPositionsCount);
                    }

                    if (otherCurrentIndex >= otherEndIndex) {
                        resultPositionsCount = copyList(positions, currentIndex, endIndex, result, resultPositionsCount);
                        return positionsList(result, 0, resultPositionsCount);
                    }
                    currentPosition = positions[currentIndex];
                    otherCurrentPosition = otherPositionsList[otherCurrentIndex];
                }
                else {
                    result[resultPositionsCount++] = otherCurrentPosition;
                    otherCurrentIndex++;

                    if (otherCurrentIndex >= otherEndIndex) {
                        resultPositionsCount = copyList(positions, currentIndex, endIndex, result, resultPositionsCount);
                        return positionsList(result, 0, resultPositionsCount);
                    }
                    otherCurrentPosition = otherPositionsList[otherCurrentIndex];
                }
            }
            // unreachable
        }
        else if (!isList && otherPositions.isList()) {
            return union(offset, size, otherPositions.getPositions(), otherPositions.getOffset(), otherPositions.size());
        }
        else if (isList && !otherPositions.isList()) {
            return union(otherPositions.getOffset(), otherPositions.size(), positions, offset, size);
        }
        // both are ranges
        verify(!isList && !otherPositions.isList());
        int otherOffset = otherPositions.getOffset();
        int otherSize = otherPositions.size();

        if (overlaps(offset, size, otherOffset, otherSize)) {
            int endOffset = Math.max(offset + size, otherOffset + otherSize);
            int startOffset = Math.min(offset, otherOffset);
            return positionsRange(startOffset, endOffset - startOffset);
        }

        // There's no overlap
        int[] result = new int[size + otherPositions.size()];
        int resultPositionsCount = 0;
        if (offset < otherOffset) {
            resultPositionsCount = copyRange(offset, offset + size, result, resultPositionsCount);
            resultPositionsCount = copyRange(otherOffset, otherOffset + otherSize, result, resultPositionsCount);
        }
        else {
            resultPositionsCount = copyRange(otherOffset, otherOffset + otherSize, result, resultPositionsCount);
            resultPositionsCount = copyRange(offset, offset + size, result, resultPositionsCount);
        }
        return positionsList(result, 0, resultPositionsCount);
    }

    private static SelectedPositions union(int rangeStart, int rangeSize, int[] positions, int offset, int size)
    {
        int[] result = new int[rangeSize + size];
        int resultPositionsCount = 0;
        int rangeEndPosition = rangeStart + rangeSize;
        int rangeCurrentPosition = rangeStart;

        int listCurrentIndex = offset;
        int listEndIndex = offset + size;
        int listCurrentPosition = positions[listCurrentIndex];

        while (true) {
            if (rangeCurrentPosition < listCurrentPosition) {
                result[resultPositionsCount++] = rangeCurrentPosition;
                rangeCurrentPosition++;

                if (rangeCurrentPosition >= rangeEndPosition) {
                    resultPositionsCount = copyList(positions, listCurrentIndex, listEndIndex, result, resultPositionsCount);
                    return positionsList(result, 0, resultPositionsCount);
                }
            }
            else if (rangeCurrentPosition == listCurrentPosition) {
                result[resultPositionsCount++] = rangeCurrentPosition;
                rangeCurrentPosition++;
                listCurrentIndex++;

                if (rangeCurrentPosition >= rangeEndPosition) {
                    resultPositionsCount = copyList(positions, listCurrentIndex, listEndIndex, result, resultPositionsCount);
                    return positionsList(result, 0, resultPositionsCount);
                }

                if (listCurrentIndex >= listEndIndex) {
                    resultPositionsCount = copyRange(rangeCurrentPosition, rangeEndPosition, result, resultPositionsCount);
                    return positionsList(result, 0, resultPositionsCount);
                }
                listCurrentPosition = positions[listCurrentIndex];
            }
            else {
                result[resultPositionsCount++] = listCurrentPosition;
                listCurrentIndex++;

                if (listCurrentIndex >= listEndIndex) {
                    resultPositionsCount = copyRange(rangeCurrentPosition, rangeEndPosition, result, resultPositionsCount);
                    return positionsList(result, 0, resultPositionsCount);
                }
                listCurrentPosition = positions[listCurrentIndex];
            }
        }
    }

    private static int copyRange(int rangeStart, int rangeEnd, int[] result, int offset)
    {
        checkFromIndexSize(offset, rangeEnd - rangeStart, result.length);
        while (rangeStart < rangeEnd) {
            result[offset++] = rangeStart++;
        }
        return offset;
    }

    private static int copyList(int[] positions, int startIndex, int endIndex, int[] result, int offset)
    {
        int remainingLength = endIndex - startIndex;
        arraycopy(positions, startIndex, result, offset, remainingLength);
        return offset + remainingLength;
    }

    private static boolean overlaps(int leftOffset, int leftSize, int rightOffset, int rightSize)
    {
        return isNotFullyBefore(leftOffset + leftSize, rightOffset)
                && isNotFullyBefore(rightOffset + rightSize, leftOffset);
    }

    private static boolean isNotFullyBefore(int leftHigh, int rightLow)
    {
        return leftHigh >= rightLow;
    }
}
