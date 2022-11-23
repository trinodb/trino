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
package io.trino.plugin.hudi.split;

import io.airlift.units.DataSize;
import io.trino.spi.SplitWeight;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Doubles.constrainToRange;
import static java.util.Objects.requireNonNull;

public class SizeBasedSplitWeightProvider
        implements HudiSplitWeightProvider
{
    private final double minimumWeight;
    private final double standardSplitSizeInBytes;

    public SizeBasedSplitWeightProvider(double minimumWeight, DataSize standardSplitSize)
    {
        checkArgument(
                Double.isFinite(minimumWeight) && minimumWeight > 0 && minimumWeight <= 1,
                "minimumWeight must be > 0 and <= 1, found: %s", minimumWeight);
        this.minimumWeight = minimumWeight;
        long standardSplitSizeInBytesLong = requireNonNull(standardSplitSize, "standardSplitSize is null").toBytes();
        checkArgument(standardSplitSizeInBytesLong > 0, "standardSplitSize must be > 0, found: %s", standardSplitSize);
        this.standardSplitSizeInBytes = (double) standardSplitSizeInBytesLong;
    }

    @Override
    public SplitWeight calculateSplitWeight(long splitSizeInBytes)
    {
        double computedWeight = splitSizeInBytes / standardSplitSizeInBytes;
        // Clamp the value between the minimum weight and 1.0 (standard weight)
        return SplitWeight.fromProportion(constrainToRange(computedWeight, minimumWeight, 1.0));
    }
}
