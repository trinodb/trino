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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.opa.schema.OpaPluginContext;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.plugin.opa.schema.TrinoIdentity;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class TestOpaQueryContextSerializationWithHeaders
{
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final TrinoIdentity TEST_IDENTITY = new TrinoIdentity("user", Collections.emptySet());
    private static final OpaPluginContext OPA_PLUGIN_CONTEXT = new OpaPluginContext("1.0");

    @Test
    void testOpaQueryContextWithHeaders()
            throws Exception
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "Authorization", java.util.List.of("Bearer token123"),
                "X-Request-Id", java.util.List.of("req-456"));

        OpaQueryContext context = new OpaQueryContext(
                TEST_IDENTITY,
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QueryId.valueOf("query123")),
                Optional.of(headers));

        String json = objectMapper.writeValueAsString(context);
        assertThat(json).contains("requestHeaders");
        assertThat(json).contains("Authorization");
        assertThat(json).contains("Bearer token123");
        assertThat(json).contains("X-Request-Id");
        assertThat(json).contains("req-456");
    }

    @Test
    void testOpaQueryContextWithoutHeaders()
            throws Exception
    {
        OpaQueryContext context = new OpaQueryContext(
                TEST_IDENTITY,
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QueryId.valueOf("query123")),
                Optional.empty());

        String json = objectMapper.writeValueAsString(context);
        assertThat(json).doesNotContain("requestHeaders");
    }

    @Test
    void testOpaQueryContextBackwardCompatibility()
            throws Exception
    {
        // Test that old constructor still works (no requestHeaders parameter)
        OpaQueryContext context = new OpaQueryContext(
                TEST_IDENTITY,
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QueryId.valueOf("query123")));

        String json = objectMapper.writeValueAsString(context);
        assertThat(json).doesNotContain("requestHeaders");
    }

    @Test
    void testOpaQueryContextWithEmptyHeaders()
            throws Exception
    {
        OpaQueryContext context = new OpaQueryContext(
                TEST_IDENTITY,
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QueryId.valueOf("query123")),
                Optional.of(Collections.emptyMap()));

        String json = objectMapper.writeValueAsString(context);
        // Empty map should be included due to Optional.of()
        assertThat(json).contains("requestHeaders");
    }

    @Test
    void testOpaQueryContextWithMultipleHeaders()
            throws Exception
    {
        Map<String, List<String>> headers = ImmutableMap.<String, List<String>>builder()
                .put("Authorization", java.util.List.of("Bearer admin-token"))
                .put("X-Tenant-Id", java.util.List.of("acme"))
                .put("X-Request-Id", java.util.List.of("req-789"))
                .put("X-Custom-Header", java.util.List.of("custom-value"))
                .buildOrThrow();

        OpaQueryContext context = new OpaQueryContext(
                TEST_IDENTITY,
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QueryId.valueOf("query123")),
                Optional.of(headers));

        String json = objectMapper.writeValueAsString(context);
        assertThat(json).contains("Authorization");
        assertThat(json).contains("X-Tenant-Id");
        assertThat(json).contains("X-Request-Id");
        assertThat(json).contains("X-Custom-Header");
    }

    @Test
    void testOpaQueryContextHeadersAreImmutable()
            throws Exception
    {
        Map<String, List<String>> mutableHeaders = new java.util.HashMap<>();
        mutableHeaders.put("Authorization", new java.util.ArrayList<>(java.util.List.of("Bearer token")));
        mutableHeaders.put("X-Request-Id", new java.util.ArrayList<>(java.util.List.of("req-123")));

        OpaQueryContext context = new OpaQueryContext(
                TEST_IDENTITY,
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QueryId.valueOf("query123")),
                Optional.of(mutableHeaders));

        String json = objectMapper.writeValueAsString(context);
        assertThat(json).contains("Authorization");
        assertThat(json).contains("Bearer token");

        // Modify original map shouldn't affect context
        mutableHeaders.put("Authorization", new java.util.ArrayList<>(java.util.List.of("Bearer modified")));
        String json2 = objectMapper.writeValueAsString(context);

        // Original value should still be in serialization
        assertThat(json2).contains("Bearer token");
        assertThat(json2).doesNotContain("Bearer modified");
    }
}
