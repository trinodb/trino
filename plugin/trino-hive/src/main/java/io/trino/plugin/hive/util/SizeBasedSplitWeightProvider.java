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
package io.trino.plugin.hive.util;

import io.airlift.units.DataSize;
import io.trino.plugin.hive.HiveSplitWeightProvider;
import io.trino.spi.SplitWeight;

import static com.google.common.base.Preconditions.checkArgument;

public class SizeBasedSplitWeightProvider
        implements HiveSplitWeightProvider
{
    private final double minimumWeight;
    private final double targetSplitSizeInBytes;

    public SizeBasedSplitWeightProvider(double minimumWeight, DataSize targetSplitSize)
    {
        checkArgument(Double.isFinite(minimumWeight) && minimumWeight > 0 && minimumWeight <= 1, "minimumWeight must be > 0 and <= 1, found: %s", minimumWeight);
        this.minimumWeight = minimumWeight;
        long targetSizeInBytes = targetSplitSize.toBytes();
        checkArgument(targetSizeInBytes > 0, "targetSplitSize must be > 0, found: %s", targetSplitSize);
        this.targetSplitSizeInBytes = (double) targetSizeInBytes;
    }

    @Override
    public SplitWeight weightForSplitSizeInBytes(long splitSizeInBytes)
    {
        double computedWeight = splitSizeInBytes / targetSplitSizeInBytes;
        // Clamp the value be between the minimum weight and 1.0 (standard weight)
        return SplitWeight.fromProportion(Math.min(Math.max(computedWeight, minimumWeight), 1.0));
    }
}
