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
package io.trino.plugin.iceberg.catalog.rest;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestIcebergRestCatalogS3Config
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergRestCatalogS3Config.class)
                .setRemoteSigningEnabled(false));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of("iceberg.rest-catalog.remote-signing-enabled", "true");
        IcebergRestCatalogS3Config expected = new IcebergRestCatalogS3Config()
                .setRemoteSigningEnabled(true);

        assertFullMapping(properties, expected);
    }
}
