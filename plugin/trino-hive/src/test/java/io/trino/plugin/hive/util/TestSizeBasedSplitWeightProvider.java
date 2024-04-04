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
import org.junit.jupiter.api.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSizeBasedSplitWeightProvider
{
    @Test
    public void testSimpleProportions()
    {
        SizeBasedSplitWeightProvider provider = new SizeBasedSplitWeightProvider(0.01, DataSize.of(64, MEGABYTE));
        assertThat(provider.weightForSplitSizeInBytes(DataSize.of(64, MEGABYTE).toBytes())).isEqualTo(SplitWeight.fromProportion(1));
        assertThat(provider.weightForSplitSizeInBytes(DataSize.of(32, MEGABYTE).toBytes())).isEqualTo(SplitWeight.fromProportion(0.5));
        assertThat(provider.weightForSplitSizeInBytes(DataSize.of(16, MEGABYTE).toBytes())).isEqualTo(SplitWeight.fromProportion(0.25));
    }

    @Test
    public void testMinimumAndMaximumSplitWeightHandling()
    {
        double minimumWeight = 0.05;
        DataSize targetSplitSize = DataSize.of(64, MEGABYTE);
        SizeBasedSplitWeightProvider provider = new SizeBasedSplitWeightProvider(minimumWeight, targetSplitSize);
        assertThat(provider.weightForSplitSizeInBytes(1)).isEqualTo(SplitWeight.fromProportion(minimumWeight));

        DataSize largerThanTarget = DataSize.of(128, MEGABYTE);
        assertThat(provider.weightForSplitSizeInBytes(largerThanTarget.toBytes())).isEqualTo(SplitWeight.standard());
    }

    @Test
    public void testInvalidMinimumWeight()
    {
        assertThatThrownBy(() -> new SizeBasedSplitWeightProvider(1.01, DataSize.of(64, MEGABYTE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("^minimumWeight must be > 0 and <= 1, found: 1\\.01$");
    }

    @Test
    public void testInvalidTargetSplitSize()
    {
        assertThatThrownBy(() -> new SizeBasedSplitWeightProvider(0.01, DataSize.ofBytes(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("^targetSplitSize must be > 0, found:.*$");
    }
}
