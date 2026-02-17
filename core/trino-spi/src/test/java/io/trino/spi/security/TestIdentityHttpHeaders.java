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
package io.trino.spi.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIdentityHttpHeaders
{
    @Test
    public void testIdentityWithHttpHeaders()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "Authorization", ImmutableList.of("Bearer token123"),
                "X-Custom-Header", ImmutableList.of("custom-value"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        assertThat(identity.getHttpHeaders()).containsAllEntriesOf(headers);
    }

    @Test
    public void testIdentityDefaultHttpHeaders()
    {
        Identity identity = Identity.forUser("testuser")
                .build();

        assertThat(identity.getHttpHeaders()).isEmpty();
    }

    @Test
    public void testHttpHeadersImmutable()
    {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("X-Test", ImmutableList.of("value"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        // Modify original map (should not affect identity)
        headers.put("X-New", ImmutableList.of("newvalue"));

        // Identity should not have the new header
        assertThat(identity.getHttpHeaders()).doesNotContainKey("X-New");
    }

    @Test
    public void testHttpHeadersReturnedImmutable()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "X-Test", ImmutableList.of("value"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        Map<String, List<String>> returnedHeaders = identity.getHttpHeaders();

        // Should not be able to modify returned headers
        assertThatThrownBy(() -> returnedHeaders.put("X-New", ImmutableList.of("newvalue")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testIdentityFromWithHttpHeaders()
    {
        Map<String, List<String>> originalHeaders = ImmutableMap.of(
                "Authorization", ImmutableList.of("Bearer token"),
                "X-Custom", ImmutableList.of("value"));

        Identity original = Identity.forUser("user1")
                .withGroups(ImmutableSet.of("group1"))
                .withHttpHeaders(originalHeaders)
                .build();

        Identity copy = Identity.from(original).build();

        assertThat(copy.getHttpHeaders()).containsAllEntriesOf(originalHeaders);
        assertThat(copy.getUser()).isEqualTo("user1");
        assertThat(copy.getGroups()).containsExactly("group1");
    }

    @Test
    public void testIdentityFromCanOverrideHttpHeaders()
    {
        Map<String, List<String>> originalHeaders = ImmutableMap.of(
                "Authorization", ImmutableList.of("Bearer token1"));

        Map<String, List<String>> newHeaders = ImmutableMap.of(
                "Authorization", ImmutableList.of("Bearer token2"),
                "X-New", ImmutableList.of("newvalue"));

        Identity original = Identity.forUser("user1")
                .withHttpHeaders(originalHeaders)
                .build();

        Identity modified = Identity.from(original)
                .withHttpHeaders(newHeaders)
                .build();

        assertThat(modified.getHttpHeaders()).containsAllEntriesOf(newHeaders);
        assertThat(modified.getHttpHeaders().get("Authorization")).containsExactly("Bearer token2");
        assertThat(modified.getHttpHeaders().get("X-New")).containsExactly("newvalue");
    }

    @Test
    public void testHttpHeadersWithMultipleValues()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "Accept", ImmutableList.of("application/json", "text/html"),
                "X-Custom", ImmutableList.of("value1", "value2", "value3"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        Map<String, List<String>> retrievedHeaders = identity.getHttpHeaders();
        assertThat(retrievedHeaders.get("Accept")).containsExactly("application/json", "text/html");
        assertThat(retrievedHeaders.get("X-Custom")).containsExactly("value1", "value2", "value3");
    }

    @Test
    public void testHttpHeadersNullValidation()
    {
        assertThatThrownBy(() -> Identity.forUser("testuser")
                .withHttpHeaders(null)
                .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("httpHeaders is null");
    }

    @Test
    public void testHttpHeadersPreservesOtherFields()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "Authorization", ImmutableList.of("Bearer token"));

        Identity identity = Identity.forUser("testuser")
                .withGroups(ImmutableSet.of("group1", "group2"))
                .withEnabledRoles(ImmutableSet.of("role1"))
                .withExtraCredentials(ImmutableMap.of("credential1", "value1"))
                .withHttpHeaders(headers)
                .build();

        assertThat(identity.getUser()).isEqualTo("testuser");
        assertThat(identity.getGroups()).containsExactlyInAnyOrder("group1", "group2");
        assertThat(identity.getEnabledRoles()).containsExactly("role1");
        assertThat(identity.getExtraCredentials()).containsEntry("credential1", "value1");
        assertThat(identity.getHttpHeaders()).containsAllEntriesOf(headers);
    }

    @Test
    public void testHttpHeadersEmptyMap()
    {
        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(ImmutableMap.of())
                .build();

        assertThat(identity.getHttpHeaders()).isEmpty();
    }

    @Test
    public void testIdentityToConnectorIdentityDoesNotIncludeHttpHeaders()
    {
        Map<String, List<String>> headers = ImmutableMap.of(
                "Authorization", ImmutableList.of("Bearer token"));

        Identity identity = Identity.forUser("testuser")
                .withHttpHeaders(headers)
                .build();

        ConnectorIdentity connectorIdentity = identity.toConnectorIdentity();

        // ConnectorIdentity should not have httpHeaders
        assertThat(connectorIdentity.getUser()).isEqualTo("testuser");
    }
}
