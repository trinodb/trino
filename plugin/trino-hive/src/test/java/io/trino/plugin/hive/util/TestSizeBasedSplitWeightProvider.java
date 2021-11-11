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
import io.trino.spi.SplitWeight;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestSizeBasedSplitWeightProvider
{
    @Test
    public void testSimpleProportions()
    {
        SizeBasedSplitWeightProvider provider = new SizeBasedSplitWeightProvider(0.01, DataSize.of(64, MEGABYTE));
        assertEquals(provider.weightForSplitSizeInBytes(DataSize.of(64, MEGABYTE).toBytes()), SplitWeight.fromProportion(1));
        assertEquals(provider.weightForSplitSizeInBytes(DataSize.of(32, MEGABYTE).toBytes()), SplitWeight.fromProportion(0.5));
        assertEquals(provider.weightForSplitSizeInBytes(DataSize.of(16, MEGABYTE).toBytes()), SplitWeight.fromProportion(0.25));
    }

    @Test
    public void testMinimumAndMaximumSplitWeightHandling()
    {
        double minimumWeight = 0.05;
        DataSize targetSplitSize = DataSize.of(64, MEGABYTE);
        SizeBasedSplitWeightProvider provider = new SizeBasedSplitWeightProvider(minimumWeight, targetSplitSize);
        assertEquals(provider.weightForSplitSizeInBytes(1), SplitWeight.fromProportion(minimumWeight));

        DataSize largerThanTarget = DataSize.of(128, MEGABYTE);
        assertEquals(provider.weightForSplitSizeInBytes(largerThanTarget.toBytes()), SplitWeight.standard());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "^minimumWeight must be > 0 and <= 1, found: 1\\.01$")
    public void testInvalidMinimumWeight()
    {
        new SizeBasedSplitWeightProvider(1.01, DataSize.of(64, MEGABYTE));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "^targetSplitSize must be > 0, found:.*$")
    public void testInvalidTargetSplitSize()
    {
        new SizeBasedSplitWeightProvider(0.01, DataSize.ofBytes(0));
    }
}
