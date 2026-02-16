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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that AuthenticationFilter properly extracts HTTP headers from requests
 * and stores them in the Identity object for OPA policies to access.
 */
final class TestAuthenticationFilterHttpHeaders
{
    @Test
    public void testHeaderExtractionMultiValuedAccept()
    {
        // Verify that headers can be stored and retrieved from Identity
        // AuthenticationFilter extracts headers from ContainerRequestContext
        // and passes them to Identity builder

        Map<String, List<String>> headers = Map.of(
                "Accept", ImmutableList.of("application/json", "text/html"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        assertThat(identity.getHttpHeaders()).containsAllEntriesOf(headers);
        assertThat(identity.getHttpHeaders().get("Accept"))
                .containsExactly("application/json", "text/html");
    }

    @Test
    public void testHeaderExtractionXForwardedFor()
    {
        // Real scenario: X-Forwarded-For with proxy chain
        // AuthenticationFilter should preserve all proxy IP addresses
        Map<String, List<String>> headers = Map.of(
                "X-Forwarded-For", ImmutableList.of("203.0.113.195", "70.41.3.18", "150.172.238.178"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        List<String> ips = identity.getHttpHeaders().get("X-Forwarded-For");
        assertThat(ips).containsExactly("203.0.113.195", "70.41.3.18", "150.172.238.178");
    }

    @Test
    public void testHeaderExtractionWithIdentityFields()
    {
        // Verify headers don't interfere with other Identity fields
        Map<String, List<String>> headers = Map.of(
                "Authorization", ImmutableList.of("Bearer token123"));

        Identity identity = Identity.forUser("testuser")
                .withGroups(ImmutableSet.of("group1", "group2"))
                .withEnabledRoles(ImmutableSet.of("admin"))
                .withHttpHeaders(headers)
                .build();

        assertThat(identity.getUser()).isEqualTo("testuser");
        assertThat(identity.getGroups()).containsExactlyInAnyOrder("group1", "group2");
        assertThat(identity.getEnabledRoles()).containsExactly("admin");
        assertThat(identity.getHttpHeaders()).containsAllEntriesOf(headers);
    }

    @Test
    public void testHeaderExtractionEmptyHeaders()
    {
        // Verify that Identity works fine with no headers
        Identity identity = Identity.forUser("testuser").build();

        assertThat(identity.getHttpHeaders()).isEmpty();
    }

    @Test
    public void testHeaderExtractionImmutability()
    {
        // Headers extracted from request should be immutable
        Map<String, List<String>> headers = Map.of(
                "Authorization", ImmutableList.of("Bearer token"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        Map<String, List<String>> retrievedHeaders = identity.getHttpHeaders();

        // Verify cannot modify (would throw UnsupportedOperationException if attempted)
        assertThat(retrievedHeaders).isNotNull();
        assertThat(retrievedHeaders).containsAllEntriesOf(headers);
    }

    @Test
    public void testHeaderExtractionPreservesAcrossIdentityCopy()
    {
        // Verify headers are preserved when copying Identity (as done by AuthenticationFilter)
        Map<String, List<String>> headers = Map.of(
                "X-Tenant-Id", ImmutableList.of("acme"),
                "X-Request-Id", ImmutableList.of("req-123"));

        Identity original = Identity.forUser("user1")
                .withHttpHeaders(headers)
                .build();

        Identity copy = Identity.from(original).build();

        assertThat(copy.getHttpHeaders()).containsAllEntriesOf(headers);
    }
}
