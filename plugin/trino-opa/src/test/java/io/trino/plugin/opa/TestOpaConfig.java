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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestOpaConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpaConfig.class)
                .setOpaUri(null)
                .setOpaBatchUri(null)
                .setOpaRowFiltersUri(null)
                .setOpaColumnMaskingUri(null)
                .setLogRequests(false)
                .setLogResponses(false)
                .setAllowPermissionManagementOperations(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("opa.policy.uri", "https://opa.example.com")
                .put("opa.policy.batched-uri", "https://opa-batch.example.com")
                .put("opa.policy.row-filters-uri", "https://opa-row-filtering.example.com")
                .put("opa.policy.column-masking-uri", "https://opa-column-masking.example.com")
                .put("opa.log-requests", "true")
                .put("opa.log-responses", "true")
                .put("opa.allow-permission-management-operations", "true")
                .buildOrThrow();

        OpaConfig expected = new OpaConfig()
                .setOpaUri(URI.create("https://opa.example.com"))
                .setOpaBatchUri(URI.create("https://opa-batch.example.com"))
                .setOpaRowFiltersUri(URI.create("https://opa-row-filtering.example.com"))
                .setOpaColumnMaskingUri(URI.create("https://opa-column-masking.example.com"))
                .setLogRequests(true)
                .setLogResponses(true)
                .setAllowPermissionManagementOperations(true);

        assertFullMapping(properties, expected);
    }
}
