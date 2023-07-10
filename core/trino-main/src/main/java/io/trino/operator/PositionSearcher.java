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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class PositionSearcher
{
    private PositionSearcher()
    {
    }

    /**
     * @param startPosition - inclusive
     * @param endPosition - exclusive
     * @param comparator - returns true if positions given as parameters are equal
     * @return the end of the group position exclusive
     */
    public static int findEndPosition(int startPosition, int endPosition, PositionComparator comparator)
    {
        checkArgument(startPosition >= 0, "startPosition must be greater or equal than zero: %s", startPosition);
        checkArgument(startPosition < endPosition, "startPosition (%s) must be less than endPosition (%s)", startPosition, endPosition);

        // exponential search to find the range enclosing the searched position, optimized for small partitions
        // use long to avoid int overflow
        long left;
        long right = startPosition;
        long distance = 1;
        do {
            left = right;
            right += distance;
            distance *= 2;
        }
        while (right < endPosition && comparator.test(toIntExact(left), toIntExact(right)));

        // binary search to find the searched position within the range
        // intLeft is always a position within the group
        int intLeft = toIntExact(left);
        // intRight is always a position out of the group
        int intRight = toIntExact(min(right, endPosition));

        while (intRight - intLeft > 1) {
            int middle = (intLeft + intRight) >>> 1;

            if (comparator.test(startPosition, middle)) {
                intLeft = middle;
            }
            else {
                intRight = middle;
            }
        }
        // the returned value is the first position out of the group
        return intRight;
    }

    public interface PositionComparator
    {
        boolean test(int first, int second);
    }
}
