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
package io.trino.plugin.iceberg.delete;

import java.util.function.IntPredicate;

/**
 * Simplified copy of io.trino.operator.project.SelectedPositions
 */
public final class Positions
{
    private static final int[] EMPTY_POSITIONS = new int[0];
    private static final Positions EMPTY = new Positions(false, EMPTY_POSITIONS, 0);

    private final boolean isList;
    private final int[] positions;
    private final int size;

    private Positions(boolean isList, int[] positions, int size)
    {
        this.isList = isList;
        this.positions = positions;
        this.size = size;
    }

    static Positions range(int size)
    {
        return new Positions(false, EMPTY_POSITIONS, size);
    }

    static Positions list(int[] positions, int size)
    {
        return new Positions(true, positions, size);
    }

    boolean isEmpty()
    {
        return size == 0;
    }

    int size()
    {
        return size;
    }

    int[] getPositions()
    {
        return positions;
    }

    Positions filter(IntPredicate shouldRetain)
    {
        if (isList) {
            int retainedCount = 0;
            int[] retainedPositions = positions;
            for (int i = 0; i < size; i++) {
                int position = retainedPositions[i];
                if (shouldRetain.test(position)) {
                    retainedPositions[retainedCount] = position;
                    retainedCount++;
                }
            }
            if (retainedCount == 0) {
                return EMPTY;
            }
            return list(retainedPositions, retainedCount);
        }
        int retainedCount = 0;
        int[] retainedPositions = new int[size];
        for (int i = 0; i < size; i++) {
            if (shouldRetain.test(i)) {
                retainedPositions[retainedCount] = i;
                retainedCount++;
            }
        }
        if (retainedCount == 0) {
            return EMPTY;
        }
        return list(retainedPositions, retainedCount);
    }
}
