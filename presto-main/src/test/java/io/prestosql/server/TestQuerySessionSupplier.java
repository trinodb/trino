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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.airlift.jaxrs.testing.GuavaMultivaluedMap;
import io.prestosql.Session;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.sql.SqlEnvironmentConfig;
import io.prestosql.sql.SqlPath;
import io.prestosql.sql.SqlPathElement;
import io.prestosql.sql.tree.Identifier;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.HASH_PARTITION_COUNT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.prestosql.client.PrestoHeaders.PRESTO_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_CAPABILITIES;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static io.prestosql.client.PrestoHeaders.PRESTO_LANGUAGE;
import static io.prestosql.client.PrestoHeaders.PRESTO_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static io.prestosql.client.PrestoHeaders.PRESTO_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_SOURCE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestQuerySessionSupplier
{
    private static final MultivaluedMap<String, String> TEST_HEADERS = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
            .put(PRESTO_USER, "testUser")
            .put(PRESTO_SOURCE, "testSource")
            .put(PRESTO_CATALOG, "testCatalog")
            .put(PRESTO_SCHEMA, "testSchema")
            .put(PRESTO_PATH, "testPath")
            .put(PRESTO_LANGUAGE, "zh-TW")
            .put(PRESTO_TIME_ZONE, "Asia/Taipei")
            .put(PRESTO_CLIENT_INFO, "client-info")
            .put(PRESTO_CLIENT_TAGS, "tag1,tag2 ,tag3, tag2")
            .put(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
            .put(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
            .put(PRESTO_PREPARED_STATEMENT, "query1=select * from foo,query2=select * from bar")
            .build());

    @Test
    public void testCreateSession()
    {
        SessionContext context = new HttpRequestSessionContext(TEST_HEADERS, "testRemote", Optional.empty(), user -> ImmutableSet.of());
        QuerySessionSupplier sessionSupplier = createSessionSupplier(new SqlEnvironmentConfig());
        Session session = sessionSupplier.createSession(new QueryId("test_query_id"), context);

        assertEquals(session.getQueryId(), new QueryId("test_query_id"));
        assertEquals(session.getUser(), "testUser");
        assertEquals(session.getSource().get(), "testSource");
        assertEquals(session.getCatalog().get(), "testCatalog");
        assertEquals(session.getSchema().get(), "testSchema");
        assertEquals(session.getPath().getRawPath().get(), "testPath");
        assertEquals(session.getLocale(), Locale.TAIWAN);
        assertEquals(session.getTimeZoneKey(), getTimeZoneKey("Asia/Taipei"));
        assertEquals(session.getRemoteUserAddress().get(), "testRemote");
        assertEquals(session.getClientInfo().get(), "client-info");
        assertEquals(session.getClientTags(), ImmutableSet.of("tag1", "tag2", "tag3"));
        assertEquals(session.getSystemProperties(), ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(HASH_PARTITION_COUNT, "43")
                .build());
        assertEquals(session.getPreparedStatements(), ImmutableMap.<String, String>builder()
                .put("query1", "select * from foo")
                .put("query2", "select * from bar")
                .build());
    }

    @Test
    public void testEmptyClientTags()
    {
        MultivaluedMap<String, String> headers1 = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(PRESTO_USER, "testUser")
                .build());
        SessionContext context1 = new HttpRequestSessionContext(headers1, "remoteAddress", Optional.empty(), user -> ImmutableSet.of());
        assertEquals(context1.getClientTags(), ImmutableSet.of());

        MultivaluedMap<String, String> headers2 = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(PRESTO_USER, "testUser")
                .put(PRESTO_CLIENT_TAGS, "")
                .build());
        SessionContext context2 = new HttpRequestSessionContext(headers2, "remoteAddress", Optional.empty(), user -> ImmutableSet.of());
        assertEquals(context2.getClientTags(), ImmutableSet.of());
    }

    @Test
    public void testClientCapabilities()
    {
        MultivaluedMap<String, String> headers1 = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(PRESTO_USER, "testUser")
                .put(PRESTO_CLIENT_CAPABILITIES, "foo, bar")
                .build());
        SessionContext context1 = new HttpRequestSessionContext(headers1, "remoteAddress", Optional.empty(), user -> ImmutableSet.of());
        assertEquals(context1.getClientCapabilities(), ImmutableSet.of("foo", "bar"));

        MultivaluedMap<String, String> headers2 = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(PRESTO_USER, "testUser")
                .build());
        SessionContext context2 = new HttpRequestSessionContext(headers2, "remoteAddress", Optional.empty(), user -> ImmutableSet.of());
        assertEquals(context2.getClientCapabilities(), ImmutableSet.of());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidTimeZone()
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(PRESTO_USER, "testUser")
                .put(PRESTO_TIME_ZONE, "unknown_timezone")
                .build());
        SessionContext context = new HttpRequestSessionContext(headers, "remoteAddress", Optional.empty(), user -> ImmutableSet.of());
        QuerySessionSupplier sessionSupplier = createSessionSupplier(new SqlEnvironmentConfig());
        sessionSupplier.createSession(new QueryId("test_query_id"), context);
    }

    @Test
    public void testSqlPathCreation()
    {
        ImmutableList.Builder<SqlPathElement> correctValues = ImmutableList.builder();
        correctValues.add(new SqlPathElement(
                Optional.of(new Identifier("normal")),
                new Identifier("schema")));
        correctValues.add(new SqlPathElement(
                Optional.of(new Identifier("who.uses.periods")),
                new Identifier("in.schema.names")));
        correctValues.add(new SqlPathElement(
                Optional.of(new Identifier("same,deal")),
                new Identifier("with,commas")));
        correctValues.add(new SqlPathElement(
                Optional.of(new Identifier("aterrible")),
                new Identifier("thing!@#$%^&*()")));
        List<SqlPathElement> expected = correctValues.build();

        SqlPath path = new SqlPath(Optional.of("normal.schema,"
                + "\"who.uses.periods\".\"in.schema.names\","
                + "\"same,deal\".\"with,commas\","
                + "aterrible.\"thing!@#$%^&*()\""));

        assertEquals(path.getParsedPath(), expected);
        assertEquals(path.toString(), Joiner.on(", ").join(expected));
    }

    @Test
    public void testDefaultCatalogAndSchema()
    {
        // none specified
        Session session = createSession(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .build(),
                new SqlEnvironmentConfig());
        assertFalse(session.getCatalog().isPresent());
        assertFalse(session.getSchema().isPresent());

        // catalog
        session = createSession(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .build(),
                new SqlEnvironmentConfig()
                        .setDefaultCatalog("catalog"));
        assertEquals(session.getCatalog(), Optional.of("catalog"));
        assertFalse(session.getSchema().isPresent());

        // catalog and schema
        session = createSession(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .build(),
                new SqlEnvironmentConfig()
                        .setDefaultCatalog("catalog")
                        .setDefaultSchema("schema"));
        assertEquals(session.getCatalog(), Optional.of("catalog"));
        assertEquals(session.getSchema(), Optional.of("schema"));

        // only schema
        assertThatThrownBy(() -> createSessionSupplier(new SqlEnvironmentConfig().setDefaultSchema("schema")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Default schema cannot be set if catalog is not set");
    }

    @Test
    public void testCatalogAndSchemaOverrides()
    {
        // none specified
        Session session = createSession(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_CATALOG, "catalog")
                        .put(PRESTO_SCHEMA, "schema")
                        .build(),
                new SqlEnvironmentConfig()
                        .setDefaultCatalog("default-catalog")
                        .setDefaultSchema("default-schema"));
        assertEquals(session.getCatalog(), Optional.of("catalog"));
        assertEquals(session.getSchema(), Optional.of("schema"));
    }

    private static Session createSession(ListMultimap<String, String> headers, SqlEnvironmentConfig config)
    {
        MultivaluedMap<String, String> headerMap = new GuavaMultivaluedMap<>(headers);
        SessionContext context = new HttpRequestSessionContext(headerMap, "testRemote", Optional.empty(), user -> ImmutableSet.of());
        QuerySessionSupplier sessionSupplier = createSessionSupplier(config);
        return sessionSupplier.createSession(new QueryId("test_query_id"), context);
    }

    private static QuerySessionSupplier createSessionSupplier(SqlEnvironmentConfig config)
    {
        return new QuerySessionSupplier(
                createTestTransactionManager(),
                new AllowAllAccessControl(),
                new SessionPropertyManager(),
                config);
    }
}
