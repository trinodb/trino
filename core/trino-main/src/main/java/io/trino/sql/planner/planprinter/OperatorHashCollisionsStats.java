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
package io.trino.sql.planner.planprinter;

class OperatorHashCollisionsStats
{
    private final double weightedHashCollisions;
    private final double weightedSumSquaredHashCollisions;
    private final double weightedExpectedHashCollisions;
    private final long inputPositions;

    public OperatorHashCollisionsStats(
            double weightedHashCollisions,
            double weightedSumSquaredHashCollisions,
            double weightedExpectedHashCollisions,
            long inputPositions)
    {
        this.weightedHashCollisions = weightedHashCollisions;
        this.weightedSumSquaredHashCollisions = weightedSumSquaredHashCollisions;
        this.weightedExpectedHashCollisions = weightedExpectedHashCollisions;
        this.inputPositions = inputPositions;
    }

    public double getWeightedHashCollisions()
    {
        return weightedHashCollisions;
    }

    public double getWeightedSumSquaredHashCollisions()
    {
        return weightedSumSquaredHashCollisions;
    }

    public double getWeightedExpectedHashCollisions()
    {
        return weightedExpectedHashCollisions;
    }

    public long getInputPositions()
    {
        return inputPositions;
    }

    public static OperatorHashCollisionsStats merge(OperatorHashCollisionsStats first, OperatorHashCollisionsStats second)
    {
        return new OperatorHashCollisionsStats(
                first.weightedHashCollisions + second.weightedHashCollisions,
                first.weightedSumSquaredHashCollisions + second.weightedSumSquaredHashCollisions,
                first.weightedExpectedHashCollisions + second.weightedExpectedHashCollisions,
                first.inputPositions + second.inputPositions);
    }
}
