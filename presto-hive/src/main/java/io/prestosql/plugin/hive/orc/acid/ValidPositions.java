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

package io.prestosql.plugin.hive.orc.acid;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ValidPositions
{
    private int[] validPositions;
    private final int positionCount;

    public ValidPositions(int[] validPositions, int positionCount)
    {
        this.validPositions = requireNonNull(validPositions, "validPositions is null");
        checkState(positionCount >= 0 && positionCount <= validPositions.length, "invalid positionCount: %s, validPositions length: %s", positionCount, validPositions.length);
        this.positionCount = positionCount;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public int getPosition(int index)
    {
        checkState(index < positionCount, "Invalid index requested: %s when positions count is %s", index, positionCount);
        return validPositions[index];
    }

    public int[] getValidPositions()
    {
        return validPositions;
    }
}
