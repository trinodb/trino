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
package io.trino.plugin.resourcegroups;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestResourceGroupSpec
{
    @Test
    public void testSameConfigDetectsSoftMemoryFractionChange()
    {
        // A percentage soft memory limit is stored as a fraction (softMemoryLimitFraction), with the
        // absolute softMemoryLimit left empty. sameConfig must treat a fraction change as a config change,
        // otherwise DbResourceGroupConfigurationManager never reconfigures the running group.
        ResourceGroupSpec fifty = spec(Optional.of("50%"));
        ResourceGroupSpec thirty = spec(Optional.of("30%"));

        assertThat(fifty.sameConfig(fifty)).isTrue();
        assertThat(fifty.sameConfig(thirty)).isFalse();
    }

    @Test
    public void testSameConfigDetectsAbsoluteSoftMemoryChange()
    {
        assertThat(spec(Optional.of("1GB")).sameConfig(spec(Optional.of("2GB")))).isFalse();
    }

    @Test
    public void testSameConfigEqualForIdenticalFractions()
    {
        assertThat(spec(Optional.of("40%")).sameConfig(spec(Optional.of("40%")))).isTrue();
    }

    private static ResourceGroupSpec spec(Optional<String> softMemoryLimit)
    {
        return new ResourceGroupSpec(
                new ResourceGroupNameTemplate("global"),
                softMemoryLimit,
                100,
                Optional.empty(),
                Optional.of(100),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
