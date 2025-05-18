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
package io.trino;

import io.trino.connector.MockConnectorFactory;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeId;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.testing.TestingConnectorContext;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSession
{
    @Test
    public void testSetCatalogProperty()
    {
        Session session = Session.builder(testSessionBuilder().build())
                .setCatalogSessionProperty("some_catalog", "first_property", "some_value")
                .build();

        assertThat(session.getCatalogProperties())
                .isEqualTo(Map.of("some_catalog", Map.of("first_property", "some_value")));
    }

    @Test
    public void testBuildWithCatalogProperty()
    {
        Session session = Session.builder(testSessionBuilder().build())
                .setCatalogSessionProperty("some_catalog", "first_property", "some_value")
                .build();
        session = Session.builder(session)
                .build();

        assertThat(session.getCatalogProperties())
                .isEqualTo(Map.of("some_catalog", Map.of("first_property", "some_value")));
    }

    @Test
    public void testAddSecondCatalogProperty()
    {
        Session session = Session.builder(testSessionBuilder().build())
                .setCatalogSessionProperty("some_catalog", "first_property", "some_value")
                .build();
        session = Session.builder(session)
                .setCatalogSessionProperty("some_catalog", "second_property", "another_value")
                .build();

        assertThat(session.getCatalogProperties())
                .isEqualTo(Map.of("some_catalog", Map.of(
                        "first_property", "some_value",
                        "second_property", "another_value")));
    }

    @Test
    public void testCreateViewSession()
    {
        final var catalog = Optional.of("test_catalog");
        final var schema = Optional.of("test_schema");
        final var queryId = new QueryId("test_query_id");
        final var transactionId = TransactionId.create();
        final var identity = new Identity.Builder("test_user").build();
        final var originalIdentity = new Identity.Builder("test_original_user").build();
        final var source = Optional.of("test_source");
        final var timeZoneKey = TimeZoneKey.UTC_KEY;
        final var locale = Locale.ENGLISH;
        final var remoteUserAddress = Optional.of("1.1.1.1");
        final var userAgent = Optional.of("test_agent");
        final var clientInfo = Optional.of("test_client_info");
        final var traceToken = Optional.of("test_trace_token");
        final var start = Instant.ofEpochMilli(2L);

        Session originalSession = Session.builder(testSessionBuilder().build())
                .setQueryId(queryId)
                .setTransactionId(transactionId)
                .setOriginalIdentity(originalIdentity)
                .setSource(source)
                .setTimeZoneKey(timeZoneKey)
                .setLocale(locale)
                .setRemoteUserAddress(remoteUserAddress)
                .setUserAgent(userAgent)
                .setClientInfo(clientInfo)
                .setTraceToken(traceToken)
                .setStart(start)
                .build();

        Session viewSession = originalSession.createViewSession(catalog, schema, identity,
                Collections.emptyList());

        assertThat(viewSession).isNotNull();
        assertThat(viewSession.getQueryId()).isEqualTo(queryId);
        assertThat(viewSession.getTransactionId()).isEqualTo(Optional.of(transactionId));
        assertThat(viewSession.getOriginalIdentity()).isEqualTo(originalIdentity);
        assertThat(viewSession.getSource()).isEqualTo(source);
        assertThat(viewSession.getTimeZoneKey()).isEqualTo(timeZoneKey);
        assertThat(viewSession.getLocale()).isEqualTo(locale);
        assertThat(viewSession.getRemoteUserAddress()).isEqualTo(remoteUserAddress);
        assertThat(viewSession.getUserAgent()).isEqualTo(userAgent);
        assertThat(viewSession.getClientInfo()).isEqualTo(clientInfo);
        assertThat(viewSession.getTraceToken()).isEqualTo(traceToken);
        assertThat(viewSession.getStart()).isEqualTo(start);
    }

    @Test
    public void testCreateViewSessionWithinView()
    {
        final var catalog = Optional.of("test_catalog");
        final var queryId = "test_query_id";
        final var originalIdentity = new Identity.Builder("test_original_user").build();
        final var source = Optional.of("test_source");
        final var timeZoneKey = TimeZoneKey.UTC_KEY;
        final var locale = Locale.ENGLISH;
        final var traceToken = Optional.of("test_trace_token");
        final var start = Instant.ofEpochMilli(2L);
        final var schemaTableName = new SchemaTableName("test_schema", "test_table");
        final var viewDefinition = new ConnectorViewDefinition(
                "", Optional.empty(), Optional.empty(),
                singletonList(new ConnectorViewDefinition.ViewColumn("test_column", TypeId.of("test_type"), Optional.empty())),
                Optional.empty(), Optional.empty(), true, emptyList());

        Session originalSession = Session.builder(testSessionBuilder().build())
                .setQueryId(new QueryId(queryId))
                .setOriginalIdentity(originalIdentity)
                .setSource(source)
                .setTimeZoneKey(timeZoneKey)
                .setLocale(locale)
                .setTraceToken(traceToken)
                .setStart(start)
                .build();

        ConnectorSession connectorSession = originalSession.toConnectorSession();

        final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>>
                viewAssertion = (viewSession, schemaTablePrefix) -> {
                    assertThat(viewSession).isNotNull();
                    assertThat(viewSession.getQueryId()).isEqualTo(queryId);
                    assertThat(viewSession.getSource()).isEqualTo(source);
                    assertThat(viewSession.getTimeZoneKey()).isEqualTo(timeZoneKey);
                    assertThat(viewSession.getLocale()).isEqualTo(locale);
                    assertThat(viewSession.getTraceToken()).isEqualTo(traceToken);
                    assertThat(viewSession.getStart()).isEqualTo(start);

                    return singletonMap(schemaTableName, viewDefinition);
                };

        final var connector = MockConnectorFactory.builder()
                .withGetViews(viewAssertion)
                .build()
                .create(catalog.get(), Collections.emptyMap(), new TestingConnectorContext());

        final var view = connector.getMetadata(connectorSession, TestingConnectorTransactionHandle.INSTANCE)
                .getView(connectorSession, schemaTableName);

        // Ensure that the custom getViews has been called
        assertThat(view).contains(viewDefinition);
    }
}
