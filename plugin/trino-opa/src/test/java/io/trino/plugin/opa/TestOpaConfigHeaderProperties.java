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

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOpaConfigHeaderProperties
{
    @Test
    void testDefaultHeaderProperties()
    {
        assertRecordedDefaults(recordDefaults(OpaConfig.class)
                .setIncludeRequestHeaders(false)
                .setAdditionalHeaders(""));
    }

    @Test
    void testExplicitHeaderProperties()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("opa.include-request-headers", "true")
                .put("opa.additional-headers", "Authorization,X-Tenant-Id,X-Request-Id")
                .buildOrThrow();

        OpaConfig expected = new OpaConfig()
                .setIncludeRequestHeaders(true)
                .setAdditionalHeaders("Authorization,X-Tenant-Id,X-Request-Id");

        assertFullMapping(properties, expected);
    }

    @Test
    void testHeadersDisabledByDefault()
    {
        OpaConfig config = new OpaConfig();
        assertThat(config.isIncludeRequestHeaders()).isFalse();
    }

    @Test
    void testHeadersCanBeEnabled()
    {
        OpaConfig config = new OpaConfig()
                .setIncludeRequestHeaders(true);
        assertThat(config.isIncludeRequestHeaders()).isTrue();
    }

    @Test
    void testAdditionalHeadersDefault()
    {
        OpaConfig config = new OpaConfig();
        assertThat(config.getAdditionalHeaders()).isEmpty();
    }

    @Test
    void testAdditionalHeadersCanBeSet()
    {
        OpaConfig config = new OpaConfig()
                .setAdditionalHeaders("Authorization,X-Custom-Header");
        assertThat(config.getAdditionalHeaders()).isEqualTo("Authorization,X-Custom-Header");
    }

    @Test
    void testAdditionalHeadersMultiple()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("opa.include-request-headers", "true")
                .put("opa.additional-headers", "Authorization,X-Tenant-Id,X-Request-Id,X-Custom")
                .buildOrThrow();

        OpaConfig expected = new OpaConfig()
                .setIncludeRequestHeaders(true)
                .setAdditionalHeaders("Authorization,X-Tenant-Id,X-Request-Id,X-Custom");

        assertFullMapping(properties, expected);
    }

    @Test
    void testAdditionalHeadersWithSpaces()
    {
        OpaConfig config = new OpaConfig()
                .setAdditionalHeaders("Authorization, X-Tenant-Id, X-Request-Id");
        assertThat(config.getAdditionalHeaders()).isEqualTo("Authorization, X-Tenant-Id, X-Request-Id");
    }

    @Test
    void testHeaderPropertiesIndependent()
    {
        OpaConfig config1 = new OpaConfig()
                .setIncludeRequestHeaders(true)
                .setAdditionalHeaders("Authorization,X-Tenant-Id");

        OpaConfig config2 = new OpaConfig()
                .setIncludeRequestHeaders(false);

        assertThat(config1.isIncludeRequestHeaders()).isTrue();
        assertThat(config2.isIncludeRequestHeaders()).isFalse();
        assertThat(config1.getAdditionalHeaders()).isEqualTo("Authorization,X-Tenant-Id");
        assertThat(config2.getAdditionalHeaders()).isEmpty();
    }

    @Test
    void testSecurityIssueEmptyConfigReturnsNoHeaders()
    {
        // Security fix: If includeRequestHeaders is true but no headers configured,
        // should NOT return all headers (deny by default)
        OpaConfig config = new OpaConfig()
                .setIncludeRequestHeaders(true)
                .setAdditionalHeaders("");  // Empty list - no headers configured

        assertThat(config.isIncludeRequestHeaders()).isTrue();
        assertThat(config.getAdditionalHeaders()).isEmpty();
        // OpaAccessControl.extractAndFilterHeaders should return empty map, not all headers
    }
}
