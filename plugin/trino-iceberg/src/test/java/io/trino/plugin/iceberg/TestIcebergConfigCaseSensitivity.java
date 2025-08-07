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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergConfigCaseSensitivity
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(IcebergConfig.class)
                .setCaseSensitiveNameMatching(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.case-sensitive-name-matching", "true")
                .buildOrThrow();

        IcebergConfig expected = new IcebergConfig()
                .setCaseSensitiveNameMatching(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testCaseSensitiveNameMatchingProperty()
    {
        IcebergConfig config = new IcebergConfig();

        // Test default value
        assertThat(config.isCaseSensitiveNameMatching()).isFalse();

        // Test setting to true
        config.setCaseSensitiveNameMatching(true);
        assertThat(config.isCaseSensitiveNameMatching()).isTrue();

        // Test setting back to false
        config.setCaseSensitiveNameMatching(false);
        assertThat(config.isCaseSensitiveNameMatching()).isFalse();
    }
}
