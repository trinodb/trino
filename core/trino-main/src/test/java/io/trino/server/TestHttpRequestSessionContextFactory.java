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
import io.airlift.jaxrs.testing.GuavaMultivaluedMap;
import io.trino.client.ProtocolHeaders;
import io.trino.security.AllowAllAccessControl;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import org.testng.annotations.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import java.util.Optional;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.client.ProtocolHeaders.createProtocolHeaders;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHttpRequestSessionContextFactory
{
    private static final HttpRequestSessionContextFactory SESSION_CONTEXT_FACTORY = new HttpRequestSessionContextFactory(
            new PreparedStatementEncoder(new ProtocolConfig()),
            createTestMetadataManager(),
            ImmutableSet::of,
            new AllowAllAccessControl());

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

        SessionContext context = SESSION_CONTEXT_FACTORY.createSessionContext(
                headers,
                Optional.of(protocolHeaders.getProtocolName()),
                Optional.of("testRemote"),
                Optional.empty());
        assertThat(context.getSource().orElse(null)).isEqualTo("testSource");
        assertThat(context.getCatalog().orElse(null)).isEqualTo("testCatalog");
        assertThat(context.getSchema().orElse(null)).isEqualTo("testSchema");
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
        assertThat(context.getSystemProperties()).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                QUERY_MAX_MEMORY, "1GB",
                JOIN_DISTRIBUTION_TYPE, "partitioned",
                MAX_HASH_PARTITION_COUNT, "43",
                "some_session_property", "some value with , comma"));
        assertThat(context.getPreparedStatements()).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("query1", "select * from foo", "query2", "select * from bar"));
        assertThat(context.getSelectedRole()).isEqualTo(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("system-role")));
        assertThat(context.getIdentity().getExtraCredentials()).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz"));
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

        SessionContext context = SESSION_CONTEXT_FACTORY.createSessionContext(
                userHeaders,
                Optional.of(protocolHeaders.getProtocolName()),
                Optional.of("testRemote"),
                Optional.empty());
        assertThat(context.getIdentity()).isEqualTo(Identity.forUser("testUser").withGroups(ImmutableSet.of("testUser")).build());

        context = SESSION_CONTEXT_FACTORY.createSessionContext(
                emptyHeaders,
                Optional.of(protocolHeaders.getProtocolName()),
                Optional.of("testRemote"),
                Optional.of(Identity.forUser("mappedUser").withGroups(ImmutableSet.of("test")).build()));
        assertThat(context.getIdentity()).isEqualTo(Identity.forUser("mappedUser").withGroups(ImmutableSet.of("test", "mappedUser")).build());

        context = SESSION_CONTEXT_FACTORY.createSessionContext(
                userHeaders,
                Optional.of(protocolHeaders.getProtocolName()),
                Optional.of("testRemote"),
                Optional.of(Identity.ofUser("mappedUser")));
        assertThat(context.getIdentity()).isEqualTo(Identity.forUser("testUser").withGroups(ImmutableSet.of("testUser")).build());

        assertThatThrownBy(
                () -> SESSION_CONTEXT_FACTORY.createSessionContext(
                        emptyHeaders,
                        Optional.of(protocolHeaders.getProtocolName()),
                        Optional.of("testRemote"),
                        Optional.empty()))
                .isInstanceOf(WebApplicationException.class)
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

        assertThatThrownBy(
                () -> SESSION_CONTEXT_FACTORY.createSessionContext(
                        headers,
                        Optional.of(protocolHeaders.getProtocolName()),
                        Optional.of("testRemote"),
                        Optional.empty()))
                .isInstanceOf(WebApplicationException.class)
                .hasMessageMatching("Invalid " + protocolHeaders.requestPreparedStatement() + " header: line 1:1: mismatched input 'abcdefg'. Expecting: .*");
    }
}
