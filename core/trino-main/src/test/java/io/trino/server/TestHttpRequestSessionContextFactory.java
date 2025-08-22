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
package io.trino.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.jaxrs.testing.GuavaMultivaluedMap;
import io.trino.client.ProtocolHeaders;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingGroupProvider;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashSet;
import java.util.Optional;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.client.ProtocolHeaders.createProtocolHeaders;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestHttpRequestSessionContextFactory
{
    @Test
    public void testSessionContext()
    {
        assertSessionContext(TRINO_HEADERS);
        assertSessionContext(createProtocolHeaders("taco"));
    }

    private static void assertSessionContext(ProtocolHeaders protocolHeaders)
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(protocolHeaders.requestUser(), "testUser")
                .put(protocolHeaders.requestSource(), "testSource")
                .put(protocolHeaders.requestCatalog(), "testCatalog")
                .put(protocolHeaders.requestSchema(), "testSchema")
                .put(protocolHeaders.requestPath(), "testPath")
                .put(protocolHeaders.requestLanguage(), "zh-TW")
                .put(protocolHeaders.requestTimeZone(), "Asia/Taipei")
                .put(protocolHeaders.requestClientInfo(), "client-info")
                .put(protocolHeaders.requestSession(), QUERY_MAX_MEMORY + "=1GB")
                .put(protocolHeaders.requestSession(), "catalog.some_session_property=1GB")
                .put(protocolHeaders.requestSession(), "catalog.with.a.dot.some_session_property=1GB")
                .put(protocolHeaders.requestSession(), JOIN_DISTRIBUTION_TYPE + "=partitioned," + MAX_HASH_PARTITION_COUNT + " = 43")
                .put(protocolHeaders.requestSession(), "some_session_property=some value with %2C comma")
                .put(protocolHeaders.requestPreparedStatement(), "query1=select * from foo,query2=select * from bar")
                .put(protocolHeaders.requestRole(), "system=ROLE{system-role}")
                .put(protocolHeaders.requestRole(), "foo_connector=ALL")
                .put(protocolHeaders.requestRole(), "bar_connector=NONE")
                .put(protocolHeaders.requestRole(), "foobar_connector=ROLE{catalog-role}")
                .put(protocolHeaders.requestExtraCredential(), "test.token.foo=bar")
                .put(protocolHeaders.requestExtraCredential(), "test.token.abc=xyz")
                .build());

        SessionContext context = sessionContextFactory(protocolHeaders).createSessionContext(
                headers,
                Optional.of("testRemote"),
                Optional.empty());
        assertThat(context.getSource().orElse(null)).isEqualTo("testSource");
        assertThat(context.getCatalog().orElse(null)).isEqualTo("testcatalog"); // lowercased
        assertThat(context.getSchema().orElse(null)).isEqualTo("testschema"); // lowercased
        assertThat(context.getPath().orElse(null)).isEqualTo("testPath");
        assertThat(context.getIdentity()).isEqualTo(Identity.forUser("testUser")
                .withGroups(ImmutableSet.of("testUser"))
                .withConnectorRoles(ImmutableMap.of(
                        "foo_connector", new SelectedRole(SelectedRole.Type.ALL, Optional.empty()),
                        "bar_connector", new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
                        "foobar_connector", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("catalog-role"))))
                .withEnabledRoles(ImmutableSet.of("system-role"))
                .build());
        assertThat(context.getClientInfo().orElse(null)).isEqualTo("client-info");
        assertThat(context.getLanguage().orElse(null)).isEqualTo("zh-TW");
        assertThat(context.getTimeZoneId().orElse(null)).isEqualTo("Asia/Taipei");
        assertThat(context.getCatalogSessionProperties()).isEqualTo(ImmutableMap.of(
                "catalog", ImmutableMap.of("some_session_property", "1GB"),
                "catalog.with.a.dot", ImmutableMap.of("some_session_property", "1GB")));
        assertThat(context.getSystemProperties()).isEqualTo(ImmutableMap.of(
                QUERY_MAX_MEMORY, "1GB",
                JOIN_DISTRIBUTION_TYPE, "partitioned",
                MAX_HASH_PARTITION_COUNT, "43",
                "some_session_property", "some value with , comma"));
        assertThat(context.getPreparedStatements()).isEqualTo(ImmutableMap.of("query1", "select * from foo", "query2", "select * from bar"));
        assertThat(context.getSelectedRole()).isEqualTo(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("system-role")));
        assertThat(context.getIdentity().getExtraCredentials()).isEqualTo(ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz"));
    }

    @Test
    public void testMappedUser()
    {
        assertMappedUser(TRINO_HEADERS);
        assertMappedUser(createProtocolHeaders("taco"));
    }

    private static void assertMappedUser(ProtocolHeaders protocolHeaders)
    {
        MultivaluedMap<String, String> userHeaders = new GuavaMultivaluedMap<>(ImmutableListMultimap.of(protocolHeaders.requestUser(), "testUser"));
        MultivaluedMap<String, String> emptyHeaders = new MultivaluedHashMap<>();

        SessionContext context = sessionContextFactory(protocolHeaders).createSessionContext(
                userHeaders,
                Optional.of("testRemote"),
                Optional.empty());
        assertThat(context.getIdentity())
                .isEqualTo(Identity.forUser("testUser")
                        .withGroups(ImmutableSet.of("testUser"))
                        .withEnabledRoles(ImmutableSet.of("system-role"))
                        .build());

        context = sessionContextFactory(protocolHeaders).createSessionContext(
                emptyHeaders,
                Optional.of("testRemote"),
                Optional.of(Identity.forUser("mappedUser").withGroups(ImmutableSet.of("test")).build()));
        assertThat(context.getIdentity())
                .isEqualTo(Identity.forUser("mappedUser")
                        .withGroups(ImmutableSet.of("test", "mappedUser"))
                        .withEnabledRoles(ImmutableSet.of("system-role"))
                        .build());

        context = sessionContextFactory(protocolHeaders).createSessionContext(
                userHeaders,
                Optional.of("testRemote"),
                Optional.of(Identity.ofUser("mappedUser")));
        assertThat(context.getIdentity())
                .isEqualTo(Identity.forUser("testUser")
                        .withGroups(ImmutableSet.of("testUser"))
                        .withEnabledRoles(ImmutableSet.of("system-role"))
                        .build());

        assertInvalidSession(protocolHeaders, emptyHeaders)
                .matches(e -> ((WebApplicationException) e).getResponse().getStatus() == 400);
    }

    @Test
    public void testPreparedStatementsHeaderDoesNotParse()
    {
        assertPreparedStatementsHeaderDoesNotParse(TRINO_HEADERS);
        assertPreparedStatementsHeaderDoesNotParse(createProtocolHeaders("taco"));
    }

    private static void assertPreparedStatementsHeaderDoesNotParse(ProtocolHeaders protocolHeaders)
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(protocolHeaders.requestUser(), "testUser")
                .put(protocolHeaders.requestSource(), "testSource")
                .put(protocolHeaders.requestCatalog(), "testCatalog")
                .put(protocolHeaders.requestSchema(), "testSchema")
                .put(protocolHeaders.requestPath(), "testPath")
                .put(protocolHeaders.requestLanguage(), "zh-TW")
                .put(protocolHeaders.requestTimeZone(), "Asia/Taipei")
                .put(protocolHeaders.requestClientInfo(), "null")
                .put(protocolHeaders.requestPreparedStatement(), "query1=abcdefg")
                .build());

        assertInvalidSession(protocolHeaders, headers)
                .hasMessageMatching("Invalid " + protocolHeaders.requestPreparedStatement() + " header: line 1:1: mismatched input 'abcdefg'. Expecting: .*");
    }

    @Test
    public void testInternalExtraCredentialName()
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(TRINO_HEADERS.requestUser(), "testUser")
                .put(TRINO_HEADERS.requestExtraCredential(), "internal$abc=xyz")
                .build());

        assertInvalidSession(TRINO_HEADERS, headers)
                .hasMessage("Invalid extra credential name: internal$abc");
    }

    @Test
    public void testImpersonation()
    {
        TestingGroupProvider groupProvider = new TestingGroupProvider();
        groupProvider.setUserGroups(ImmutableMap.of(
                "alice", ImmutableSet.of("aliceGroup"),
                "bob", ImmutableSet.of("bobGroup"),
                "otis", ImmutableSet.of("otisGroup")));
        TestingAccessControlManager accessControl = new TestingAccessControlManager(
                createTestTransactionManager(),
                emptyEventListenerManager(),
                new SecretsResolver(ImmutableMap.of()));

        accessControl.loadSystemAccessControl("allow-all", ImmutableMap.of());

        HttpRequestSessionContextFactory sessionContextFactory = sessionContextFactory(TRINO_HEADERS, groupProvider, accessControl);

        HashSet<String> impersonatedUsers = new HashSet<>();
        accessControl.denyImpersonation((identity, targetUser) -> {
            impersonatedUsers.add(targetUser);
            if ("alice".equals(identity.getUser())) {
                assertThat(targetUser).isEqualTo("otis");
                assertThat(identity.getEnabledRoles()).containsExactlyInAnyOrder("system-role");
                assertThat(identity.getGroups()).containsExactlyInAnyOrder("aliceGroup");
            }
            if ("otis".equals(identity.getUser())) {
                assertThat(targetUser).isEqualTo("bob");
                assertThat(identity.getEnabledRoles()).containsExactlyInAnyOrder("system-role");
                assertThat(identity.getGroups()).containsExactlyInAnyOrder("otisGroup");
            }
            return true;
        });

        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.of(
                TRINO_HEADERS.requestUser(), "bob",
                TRINO_HEADERS.requestOriginalUser(), "otis"));

        Identity authenticatedIdentity = Identity.forUser("alice").build();

        Identity authorizedIdentity = sessionContextFactory.extractAuthorizedIdentity(Optional.of(authenticatedIdentity), headers);

        assertThat(impersonatedUsers).containsExactlyInAnyOrder("otis", "bob");
        assertThat(authorizedIdentity.getUser()).isEqualTo("bob");
        assertThat(authorizedIdentity.getEnabledRoles()).containsExactlyInAnyOrder("system-role");
        assertThat(authorizedIdentity.getGroups()).containsExactlyInAnyOrder("bobGroup");
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertInvalidSession(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        return assertThatThrownBy(
                () -> sessionContextFactory(protocolHeaders)
                        .createSessionContext(headers, Optional.of("testRemote"), Optional.empty()))
                .isInstanceOf(WebApplicationException.class);
    }

    private static HttpRequestSessionContextFactory sessionContextFactory(ProtocolHeaders headers)
    {
        return sessionContextFactory(headers, ImmutableSet::of, new AllowAllAccessControl());
    }

    private static HttpRequestSessionContextFactory sessionContextFactory(ProtocolHeaders headers,
                                                                          GroupProvider groupProvider,
                                                                          AccessControl accessControl)
    {
        return new HttpRequestSessionContextFactory(
                new PreparedStatementEncoder(new ProtocolConfig()),
                createTestMetadataManager(),
                groupProvider,
                accessControl,
                new ProtocolConfig()
                        .setAlternateHeaderName(headers.getProtocolName()),
                QueryDataEncoder.EncoderSelector.noEncoder());
    }
}
