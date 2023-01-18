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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestIcebergRestCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergRestCatalogConfig.class)
                .setBaseUri(null)
                .setWarehouse(null)
                .setSessionType(IcebergRestCatalogConfig.SessionType.NONE)
                .setSecurity(IcebergRestCatalogConfig.Security.NONE));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest-catalog.uri", "http://localhost:1234")
                .put("iceberg.rest-catalog.warehouse", "test_warehouse_identifier")
                .put("iceberg.rest-catalog.security", "OAUTH2")
                .put("iceberg.rest-catalog.session", "USER")
                .buildOrThrow();

        IcebergRestCatalogConfig expected = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost:1234")
                .setWarehouse("test_warehouse_identifier")
                .setSessionType(IcebergRestCatalogConfig.SessionType.USER)
                .setSecurity(IcebergRestCatalogConfig.Security.OAUTH2);

        assertFullMapping(properties, expected);
    }
}
