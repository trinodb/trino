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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestFakerConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FakerConfig.class)
                .setNullProbability(0.5)
                .setDefaultLimit(1000L)
                .setLocale("en")
                .setSequenceDetectionEnabled(true)
                .setDictionaryDetectionEnabled(true));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("faker.null-probability", "1.0")
                .put("faker.default-limit", "10")
                .put("faker.locale", "pl-PL")
                .put("faker.sequence-detection-enabled", "false")
                .put("faker.dictionary-detection-enabled", "false")
                .buildOrThrow();

        FakerConfig expected = new FakerConfig()
                .setNullProbability(1.0)
                .setDefaultLimit(10L)
                .setLocale("pl-PL")
                .setSequenceDetectionEnabled(false)
                .setDictionaryDetectionEnabled(false);

        assertFullMapping(properties, expected);
    }
}
