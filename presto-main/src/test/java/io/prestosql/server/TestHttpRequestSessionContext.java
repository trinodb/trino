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
import com.google.common.collect.ImmutableSet;
import io.airlift.jaxrs.testing.GuavaMultivaluedMap;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import org.testng.annotations.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import java.util.Optional;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHttpRequestSessionContext
{
    @Test
    public void testSessionContext()
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
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
                .build());

        SessionContext context = new HttpRequestSessionContext(headers, "testRemote", Optional.empty(), user -> ImmutableSet.of(user));
        assertEquals(context.getSource(), "testSource");
        assertEquals(context.getCatalog(), "testCatalog");
        assertEquals(context.getSchema(), "testSchema");
        assertEquals(context.getPath(), "testPath");
        assertEquals(context.getIdentity(), Identity.ofUser("testUser"));
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
        assertEquals(context.getIdentity().getGroups(), ImmutableSet.of("testUser"));
    }

    @Test
    public void testMappedUser()
    {
        MultivaluedMap<String, String> userHeaders = new GuavaMultivaluedMap<>(ImmutableListMultimap.of(PRESTO_USER, "testUser"));
        MultivaluedMap<String, String> emptyHeaders = new MultivaluedHashMap<>();

        HttpRequestSessionContext context = new HttpRequestSessionContext(userHeaders, "testRemote", Optional.empty(), user -> ImmutableSet.of(user));
        assertEquals(context.getIdentity(), Identity.forUser("testUser").withGroups(ImmutableSet.of("testUser")).build());

        context = new HttpRequestSessionContext(
                emptyHeaders,
                "testRemote",
                Optional.of(Identity.forUser("mappedUser").withGroups(ImmutableSet.of("test")).build()),
                user -> ImmutableSet.of(user));
        assertEquals(context.getIdentity(), Identity.forUser("mappedUser").withGroups(ImmutableSet.of("test", "mappedUser")).build());

        context = new HttpRequestSessionContext(userHeaders, "testRemote", Optional.of(Identity.ofUser("mappedUser")), user -> ImmutableSet.of(user));
        assertEquals(context.getIdentity(), Identity.forUser("testUser").withGroups(ImmutableSet.of("testUser")).build());

        assertThatThrownBy(() -> new HttpRequestSessionContext(emptyHeaders, "testRemote", Optional.empty(), user -> ImmutableSet.of()))
                .isInstanceOf(WebApplicationException.class)
                .matches(e -> ((WebApplicationException) e).getResponse().getStatus() == 400);
    }

    @Test
    public void testPreparedStatementsHeaderDoesNotParse()
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(PRESTO_USER, "testUser")
                .put(PRESTO_SOURCE, "testSource")
                .put(PRESTO_CATALOG, "testCatalog")
                .put(PRESTO_SCHEMA, "testSchema")
                .put(PRESTO_PATH, "testPath")
                .put(PRESTO_LANGUAGE, "zh-TW")
                .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                .put(PRESTO_CLIENT_INFO, "null")
                .put(PRESTO_PREPARED_STATEMENT, "query1=abcdefg")
                .build());

        assertThatThrownBy(() -> new HttpRequestSessionContext(headers, "testRemote", Optional.empty(), user -> ImmutableSet.of()))
                .isInstanceOf(WebApplicationException.class)
                .hasMessageMatching("Invalid X-Presto-Prepared-Statement header: line 1:1: mismatched input 'abcdefg'. Expecting: .*");
    }
}
