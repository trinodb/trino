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
package io.trino.plugin.hive.crypto;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestParquetEncryptionConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ParquetEncryptionConfig.class)
                .setEnvironmentKeyRetrieverEnabled(false)
                .setAadPrefix(null)
                .setCheckFooterIntegrity(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pme.environment-key-retriever.enabled", "true")
                .put("pme.aad-prefix", "tenant‑42")
                .put("pme.check-footer-integrity", "false")
                .buildOrThrow();

        ParquetEncryptionConfig expected = new ParquetEncryptionConfig()
                .setEnvironmentKeyRetrieverEnabled(true)
                .setAadPrefix("tenant‑42")
                .setCheckFooterIntegrity(false);

        assertFullMapping(properties, expected);
    }
}
