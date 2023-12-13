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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.airlift.jaxrs.testing.GuavaMultivaluedMap;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.SqlPath;
import io.trino.transaction.TransactionManager;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Optional;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.metadata.LanguageFunctionManager.QUERY_LOCAL_SCHEMA;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.metadata.MetadataManager.testMetadataManagerBuilder;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQuerySessionSupplier
{
    private static final MultivaluedMap<String, String> TEST_HEADERS = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
            .put(TRINO_HEADERS.requestUser(), "testUser")
            .put(TRINO_HEADERS.requestSource(), "testSource")
            .put(TRINO_HEADERS.requestCatalog(), "testCatalog")
            .put(TRINO_HEADERS.requestSchema(), "testSchema")
            .put(TRINO_HEADERS.requestPath(), "testPath")
            .put(TRINO_HEADERS.requestLanguage(), "zh-TW")
            .put(TRINO_HEADERS.requestTimeZone(), "Asia/Taipei")
            .put(TRINO_HEADERS.requestClientInfo(), "client-info")
            .put(TRINO_HEADERS.requestClientTags(), "tag1,tag2 ,tag3, tag2")
            .put(TRINO_HEADERS.requestSession(), QUERY_MAX_MEMORY + "=1GB")
            .put(TRINO_HEADERS.requestSession(), JOIN_DISTRIBUTION_TYPE + "=partitioned," + MAX_HASH_PARTITION_COUNT + " = 43")
            .put(TRINO_HEADERS.requestPreparedStatement(), "query1=select * from foo,query2=select * from bar")
            .build());
    private static final HttpRequestSessionContextFactory SESSION_CONTEXT_FACTORY = new HttpRequestSessionContextFactory(
            new PreparedStatementEncoder(new ProtocolConfig()),
            createTestMetadataManager(),
            ImmutableSet::of,
            new AllowAllAccessControl());

    @Test
    public void testCreateSession()
    {
        SessionContext context = SESSION_CONTEXT_FACTORY.createSessionContext(TEST_HEADERS, Optional.empty(), Optional.of("testRemote"), Optional.empty());
        QuerySessionSupplier sessionSupplier = createSessionSupplier(new SqlEnvironmentConfig());
        Session session = sessionSupplier.createSession(new QueryId("test_query_id"), Span.getInvalid(), context);

        assertThat(session.getQueryId()).isEqualTo(new QueryId("test_query_id"));
        assertThat(session.getUser()).isEqualTo("testUser");
        assertThat(session.getSource().get()).isEqualTo("testSource");
        assertThat(session.getCatalog().get()).isEqualTo("testCatalog");
        assertThat(session.getSchema().get()).isEqualTo("testSchema");
        assertThat(session.getPath().getRawPath()).isEqualTo("testPath");
        assertThat(session.getLocale()).isEqualTo(Locale.TAIWAN);
        assertThat(session.getTimeZoneKey()).isEqualTo(getTimeZoneKey("Asia/Taipei"));
        assertThat(session.getRemoteUserAddress().get()).isEqualTo("testRemote");
        assertThat(session.getClientInfo().get()).isEqualTo("client-info");
        assertThat(session.getClientTags()).isEqualTo(ImmutableSet.of("tag1", "tag2", "tag3"));
        assertThat(session.getSystemProperties()).isEqualTo(ImmutableMap.<String, String>builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(MAX_HASH_PARTITION_COUNT, "43")
                .buildOrThrow());
        assertThat(session.getPreparedStatements()).isEqualTo(ImmutableMap.<String, String>builder()
                .put("query1", "select * from foo")
                .put("query2", "select * from bar")
                .buildOrThrow());
    }

    @Test
    public void testEmptyClientTags()
    {
        MultivaluedMap<String, String> headers1 = new GuavaMultivaluedMap<>(ImmutableListMultimap.of(TRINO_HEADERS.requestUser(), "testUser"));
        SessionContext context1 = SESSION_CONTEXT_FACTORY.createSessionContext(headers1, Optional.empty(), Optional.of("remoteAddress"), Optional.empty());
        assertThat(context1.getClientTags()).isEqualTo(ImmutableSet.of());

        MultivaluedMap<String, String> headers2 = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(TRINO_HEADERS.requestUser(), "testUser")
                .put(TRINO_HEADERS.requestClientTags(), "")
                .build());
        SessionContext context2 = SESSION_CONTEXT_FACTORY.createSessionContext(headers2, Optional.empty(), Optional.of("remoteAddress"), Optional.empty());
        assertThat(context2.getClientTags()).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testClientCapabilities()
    {
        MultivaluedMap<String, String> headers1 = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(TRINO_HEADERS.requestUser(), "testUser")
                .put(TRINO_HEADERS.requestClientCapabilities(), "foo, bar")
                .build());
        SessionContext context1 = SESSION_CONTEXT_FACTORY.createSessionContext(headers1, Optional.empty(), Optional.of("remoteAddress"), Optional.empty());
        assertThat(context1.getClientCapabilities()).isEqualTo(ImmutableSet.of("foo", "bar"));

        MultivaluedMap<String, String> headers2 = new GuavaMultivaluedMap<>(ImmutableListMultimap.of(TRINO_HEADERS.requestUser(), "testUser"));
        SessionContext context2 = SESSION_CONTEXT_FACTORY.createSessionContext(headers2, Optional.empty(), Optional.of("remoteAddress"), Optional.empty());
        assertThat(context2.getClientCapabilities()).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testInvalidTimeZone()
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(TRINO_HEADERS.requestUser(), "testUser")
                .put(TRINO_HEADERS.requestTimeZone(), "unknown_timezone")
                .build());
        SessionContext context = SESSION_CONTEXT_FACTORY.createSessionContext(headers, Optional.empty(), Optional.of("remoteAddress"), Optional.empty());
        QuerySessionSupplier sessionSupplier = createSessionSupplier(new SqlEnvironmentConfig());
        assertThatThrownBy(() -> sessionSupplier.createSession(new QueryId("test_query_id"), Span.getInvalid(), context))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Time zone not supported: unknown_timezone");
    }

    @Test
    public void testSqlPathCreation()
    {
        String rawPath = "normal.schema,"
                + "\"who.uses.periods\".\"in.schema.names\","
                + "\"same,deal\".\"with,commas\","
                + "aterrible.\"thing!@#$%^&*()\"";
        SqlPath path = SqlPath.buildPath(
                rawPath,
                Optional.empty());

        assertThat(path.getPath()).isEqualTo(ImmutableList.<CatalogSchemaName>builder()
                .add(new CatalogSchemaName(GlobalSystemConnector.NAME, QUERY_LOCAL_SCHEMA))
                .add(new CatalogSchemaName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA))
                .add(new CatalogSchemaName("normal", "schema"))
                .add(new CatalogSchemaName("who.uses.periods", "in.schema.names"))
                .add(new CatalogSchemaName("same,deal", "with,commas"))
                .add(new CatalogSchemaName("aterrible", "thing!@#$%^&*()"))
                .build());

        assertThat(path.toString()).isEqualTo(rawPath);
    }

    @Test
    public void testDefaultCatalogAndSchema()
    {
        // no session or defaults
        Session session = createSession(
                ImmutableListMultimap.of(TRINO_HEADERS.requestUser(), "testUser"),
                new SqlEnvironmentConfig());
        assertThat(session.getCatalog()).isEmpty();
        assertThat(session.getSchema()).isEmpty();

        // no session with default catalog
        session = createSession(
                ImmutableListMultimap.of(TRINO_HEADERS.requestUser(), "testUser"),
                new SqlEnvironmentConfig()
                        .setDefaultCatalog("default-catalog"));
        assertThat(session.getCatalog()).contains("default-catalog");
        assertThat(session.getSchema()).isEmpty();

        // no session with default catalog and schema
        session = createSession(
                ImmutableListMultimap.of(TRINO_HEADERS.requestUser(), "testUser"),
                new SqlEnvironmentConfig()
                        .setDefaultCatalog("default-catalog")
                        .setDefaultSchema("default-schema"));
        assertThat(session.getCatalog()).contains("default-catalog");
        assertThat(session.getSchema()).contains("default-schema");

        // only default schema
        assertThatThrownBy(() -> createSessionSupplier(new SqlEnvironmentConfig().setDefaultSchema("schema")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Default schema cannot be set if catalog is not set");

        // both session and defaults set
        session = createSession(
                ImmutableListMultimap.<String, String>builder()
                        .put(TRINO_HEADERS.requestUser(), "testUser")
                        .put(TRINO_HEADERS.requestCatalog(), "catalog")
                        .put(TRINO_HEADERS.requestSchema(), "schema")
                        .build(),
                new SqlEnvironmentConfig()
                        .setDefaultCatalog("default-catalog")
                        .setDefaultSchema("default-schema"));
        assertThat(session.getCatalog()).contains("catalog");
        assertThat(session.getSchema()).contains("schema");

        // default schema not used when session catalog is set
        session = createSession(
                ImmutableListMultimap.<String, String>builder()
                        .put(TRINO_HEADERS.requestUser(), "testUser")
                        .put(TRINO_HEADERS.requestCatalog(), "catalog")
                        .build(),
                new SqlEnvironmentConfig()
                        .setDefaultCatalog("default-catalog")
                        .setDefaultSchema("default-schema"));
        assertThat(session.getCatalog()).contains("catalog");
        assertThat(session.getSchema()).isEmpty();
    }

    private static Session createSession(ListMultimap<String, String> headers, SqlEnvironmentConfig config)
    {
        MultivaluedMap<String, String> headerMap = new GuavaMultivaluedMap<>(headers);
        SessionContext context = SESSION_CONTEXT_FACTORY.createSessionContext(headerMap, Optional.empty(), Optional.of("testRemote"), Optional.empty());
        QuerySessionSupplier sessionSupplier = createSessionSupplier(config);
        return sessionSupplier.createSession(new QueryId("test_query_id"), Span.getInvalid(), context);
    }

    private static QuerySessionSupplier createSessionSupplier(SqlEnvironmentConfig config)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = testMetadataManagerBuilder()
                .withTransactionManager(transactionManager)
                .build();
        return new QuerySessionSupplier(
                metadata,
                new AllowAllAccessControl(),
                new SessionPropertyManager(),
                config);
    }
}
