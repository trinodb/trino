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

import com.google.common.collect.ImmutableList;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
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
        Optional<String> catalog = Optional.of("test_catalog");
        Optional<String> schema = Optional.of("test_schema");
        QueryId queryId = new QueryId("test_query_id");
        TransactionId transactionId = TransactionId.create();
        Identity identity = new Identity.Builder("test_user").build();
        Identity originalIdentity = new Identity.Builder("test_original_user").build();
        Optional<String> source = Optional.of("test_source");
        TimeZoneKey timeZoneKey = TimeZoneKey.UTC_KEY;
        Locale locale = Locale.ENGLISH;
        Optional<String> remoteUserAddress = Optional.of("1.1.1.1");
        Optional<String> userAgent = Optional.of("test_agent");
        Optional<String> clientInfo = Optional.of("test_client_info");
        Optional<String> traceToken = Optional.of("test_trace_token");
        Instant start = Instant.ofEpochMilli(2L);

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

        Session viewSession = originalSession.createViewSession(catalog, schema, identity, ImmutableList.of());

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
}
