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
import io.trino.plugin.opa.schema.OpaPluginContext;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class TestOpaAccessControlHeaderIntegration
{
    private static final OpaPluginContext OPA_PLUGIN_CONTEXT = new OpaPluginContext("1.0");
    private static final QueryId QUERY_ID = QueryId.valueOf("query123");

    @Test
    void testOpaQueryContextWithHeaders()
    {
        Map<String, java.util.List<String>> headers = ImmutableMap.of(
                "Authorization", java.util.List.of("Bearer token123"),
                "X-Request-Id", java.util.List.of("req-456"));

        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(headers));

        assertThat(context.requestHeaders()).isPresent();
        assertThat(context.requestHeaders().get()).containsAllEntriesOf(headers);
    }

    @Test
    void testOpaQueryContextWithoutHeaders()
    {
        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.empty());

        assertThat(context.requestHeaders()).isEmpty();
    }

    @Test
    void testOpaQueryContextHeadersFiltering()
    {
        Map<String, java.util.List<String>> allHeaders = ImmutableMap.of(
                "Authorization", java.util.List.of("Bearer token"),
                "X-Request-Id", java.util.List.of("req-123"),
                "X-Tenant", java.util.List.of("acme"),
                "Content-Type", java.util.List.of("application/json"));

        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(allHeaders));

        assertThat(context.requestHeaders()).isPresent();
        Map<String, java.util.List<String>> headers = context.requestHeaders().get();
        assertThat(headers).containsAllEntriesOf(allHeaders);
    }

    @Test
    void testOpaQueryContextMultiValueHeaders()
    {
        Map<String, java.util.List<String>> headers = ImmutableMap.of(
                "Accept", java.util.List.of("application/json", "text/html"),
                "Authorization", java.util.List.of("Bearer token"));

        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(headers));

        assertThat(context.requestHeaders()).isPresent();
        assertThat(context.requestHeaders().get().get("Accept")).containsExactly("application/json", "text/html");
    }

    @Test
    void testOpaQueryContextSpecialCharacterHeaders()
    {
        Map<String, java.util.List<String>> headers = ImmutableMap.of(
                "Authorization", java.util.List.of("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"),
                "X-Special", java.util.List.of("value!@#$%^&*()"));

        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(headers));

        assertThat(context.requestHeaders()).isPresent();
        Map<String, java.util.List<String>> retrievedHeaders = context.requestHeaders().get();
        assertThat(retrievedHeaders.get("Authorization").get(0)).startsWith("Bearer eyJ");
        assertThat(retrievedHeaders.get("X-Special").get(0)).isEqualTo("value!@#$%^&*()");
    }

    @Test
    void testOpaQueryContextWithEmptyHeaders()
    {
        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(Collections.emptyMap()));

        assertThat(context.requestHeaders()).isPresent();
        assertThat(context.requestHeaders().get()).isEmpty();
    }

    @Test
    void testOpaQueryContextWithMultiValuedXForwardedFor()
    {
        // Real scenario: X-Forwarded-For with proxy chain
        // Client IP, Proxy1, Proxy2 should all be available to OPA
        Map<String, java.util.List<String>> headers = ImmutableMap.of(
                "X-Forwarded-For", java.util.List.of("203.0.113.195", "70.41.3.18", "150.172.238.178"));

        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(headers));

        assertThat(context.requestHeaders()).isPresent();
        java.util.List<String> ips = context.requestHeaders().get().get("X-Forwarded-For");
        assertThat(ips).containsExactly("203.0.113.195", "70.41.3.18", "150.172.238.178");
    }

    @Test
    void testOpaQueryContextWithSetCookieHeaders()
    {
        // Real scenario: Multiple Set-Cookie headers
        Map<String, java.util.List<String>> headers = ImmutableMap.of(
                "Set-Cookie", java.util.List.of(
                        "sessionid=abc123; Path=/",
                        "userid=john; Path=/",
                        "preferences=dark_mode; Path=/"));

        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(headers));

        assertThat(context.requestHeaders()).isPresent();
        java.util.List<String> cookies = context.requestHeaders().get().get("Set-Cookie");
        assertThat(cookies)
                .containsExactly(
                        "sessionid=abc123; Path=/",
                        "userid=john; Path=/",
                        "preferences=dark_mode; Path=/");
    }

    @Test
    void testOpaQueryContextWithAcceptHeaders()
    {
        // Real scenario: Accept header with multiple content types
        Map<String, java.util.List<String>> headers = ImmutableMap.of(
                "Accept", java.util.List.of("application/json", "text/html;q=0.9", "*/*;q=0.8"));

        OpaQueryContext context = new OpaQueryContext(
                new io.trino.plugin.opa.schema.TrinoIdentity("testuser", Collections.emptySet()),
                OPA_PLUGIN_CONTEXT,
                Collections.emptyMap(),
                Optional.of(QUERY_ID),
                Optional.of(headers));

        assertThat(context.requestHeaders()).isPresent();
        java.util.List<String> accept = context.requestHeaders().get().get("Accept");
        assertThat(accept).containsExactly("application/json", "text/html;q=0.9", "*/*;q=0.8");
    }
}
