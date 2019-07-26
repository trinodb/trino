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
package io.prestosql.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import java.util.Optional;

import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;
import static io.prestosql.SystemSessionProperties.HASH_PARTITION_COUNT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.prestosql.client.PrestoHeaders.PRESTO_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static io.prestosql.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static io.prestosql.client.PrestoHeaders.PRESTO_LANGUAGE;
import static io.prestosql.client.PrestoHeaders.PRESTO_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static io.prestosql.client.PrestoHeaders.PRESTO_ROLE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_SOURCE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.dispatcher.DispatcherConfig.HeaderSupport.ACCEPT;
import static io.prestosql.dispatcher.DispatcherConfig.HeaderSupport.IGNORE;
import static io.prestosql.dispatcher.DispatcherConfig.HeaderSupport.WARN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHttpRequestSessionContext
{
    @Test
    public void testSessionContext()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_PATH, "testPath")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "client-info")
                        .put(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                        .put(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                        .put(PRESTO_SESSION, "some_session_property=some value with %2C comma")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=select * from foo,query2=select * from bar")
                        .put(PRESTO_ROLE, "foo_connector=ALL")
                        .put(PRESTO_ROLE, "bar_connector=NONE")
                        .put(PRESTO_ROLE, "foobar_connector=ROLE{role}")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.foo=bar")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.abc=xyz")
                        .build(),
                "testRemote");

        HttpRequestSessionContext context = new HttpRequestSessionContext(WARN, request);
        assertEquals(context.getSource(), "testSource");
        assertEquals(context.getCatalog(), "testCatalog");
        assertEquals(context.getSchema(), "testSchema");
        assertEquals(context.getPath(), "testPath");
        assertEquals(context.getIdentity(), new Identity("testUser", Optional.empty()));
        assertEquals(context.getClientInfo(), "client-info");
        assertEquals(context.getLanguage(), "zh-TW");
        assertEquals(context.getTimeZoneId(), "Asia/Taipei");
        assertEquals(context.getSystemProperties(), ImmutableMap.of(
                QUERY_MAX_MEMORY, "1GB",
                JOIN_DISTRIBUTION_TYPE, "partitioned",
                HASH_PARTITION_COUNT, "43",
                "some_session_property", "some value with , comma"));
        assertEquals(context.getPreparedStatements(), ImmutableMap.of("query1", "select * from foo", "query2", "select * from bar"));
        assertEquals(context.getIdentity().getRoles(), ImmutableMap.of(
                "foo_connector", new SelectedRole(SelectedRole.Type.ALL, Optional.empty()),
                "bar_connector", new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
                "foobar_connector", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role"))));
        assertEquals(context.getIdentity().getExtraCredentials(), ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz"));
    }

    @Test
    public void testPreparedStatementsHeaderDoesNotParse()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_PATH, "testPath")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "null")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=abcdefg")
                        .build(),
                "testRemote");
        assertThatThrownBy(() -> new HttpRequestSessionContext(WARN, request))
                .isInstanceOf(WebApplicationException.class)
                .hasMessageMatching("Invalid X-Presto-Prepared-Statement header: line 1:1: mismatched input 'abcdefg'. Expecting: .*");
    }

    @Test
    public void testXForwardedFor()
    {
        HttpServletRequest plainRequest = requestWithXForwardedFor(Optional.empty(), "remote_address");
        HttpServletRequest requestWithXForwardedFor = requestWithXForwardedFor(Optional.of("forwarded_client"), "proxy_address");

        assertEquals(new HttpRequestSessionContext(IGNORE, plainRequest).getRemoteUserAddress(), "remote_address");
        assertEquals(new HttpRequestSessionContext(IGNORE, requestWithXForwardedFor).getRemoteUserAddress(), "proxy_address");

        assertEquals(new HttpRequestSessionContext(ACCEPT, plainRequest).getRemoteUserAddress(), "remote_address");
        assertEquals(new HttpRequestSessionContext(ACCEPT, requestWithXForwardedFor).getRemoteUserAddress(), "forwarded_client");

        assertEquals(new HttpRequestSessionContext(WARN, plainRequest).getRemoteUserAddress(), "remote_address");
        assertEquals(new HttpRequestSessionContext(WARN, requestWithXForwardedFor).getRemoteUserAddress(), "proxy_address"); // this generates a warning to logs
    }

    private static HttpServletRequest requestWithXForwardedFor(Optional<String> xForwardedFor, String remoteAddress)
    {
        ImmutableListMultimap.Builder<String, String> headers = ImmutableListMultimap.<String, String>builder()
                .put(PRESTO_USER, "testUser");
        xForwardedFor.ifPresent(value -> headers.put(X_FORWARDED_FOR, value));
        return new MockHttpServletRequest(headers.build(), remoteAddress);
    }
}
