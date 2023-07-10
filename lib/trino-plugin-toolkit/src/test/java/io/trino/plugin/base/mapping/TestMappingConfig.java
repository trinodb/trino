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
package io.trino.plugin.base.mapping;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestMappingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MappingConfig.class)
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setCaseInsensitiveNameMatchingConfigFile(null)
                .setCaseInsensitiveNameMatchingConfigFileRefreshPeriod(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        String configFile = createRuleBasedIdentifierMappingFile().toFile().getAbsolutePath();
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("case-insensitive-name-matching", "true")
                .put("case-insensitive-name-matching.cache-ttl", "1s")
                .put("case-insensitive-name-matching.config-file", configFile)
                .put("case-insensitive-name-matching.config-file.refresh-period", "1s")
                .buildOrThrow();

        MappingConfig expected = new MappingConfig()
                .setCaseInsensitiveNameMatching(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setCaseInsensitiveNameMatchingConfigFile(configFile)
                .setCaseInsensitiveNameMatchingConfigFileRefreshPeriod(new Duration(1, SECONDS));

        assertFullMapping(properties, expected);
    }
}
