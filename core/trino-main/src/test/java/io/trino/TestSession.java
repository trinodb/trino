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
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableHandle.ResolvingIdentity;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.TimeZoneKey;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
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
    public void testTableConnectorSessionUsesResolvingIdentity()
    {
        SelectedRole role = new SelectedRole(SelectedRole.Type.ROLE, Optional.of("owner_role"));
        ConnectorIdentity owner = ConnectorIdentity.forUser("owner")
                .withGroups(Set.of("owners"))
                .withPrincipal(new BasicPrincipal("owner_principal"))
                .withEnabledSystemRoles(Set.of("system_role"))
                .withConnectorRole(role)
                .build();
        Session session = testSessionBuilder()
                .setIdentity(Identity.forUser("user")
                        .withExtraCredentials(Map.of("user_credential", "value"))
                        .build())
                .build();

        TableHandle ownerTable = tableHandle(ResolvingIdentity.from(owner));
        assertThat(ownerTable.isResolvedAs(session.getIdentity())).isFalse();
        ConnectorSession ownerSession = session.toTableConnectorSession(ownerTable);
        assertThat(ownerSession.getIdentity().getUser()).isEqualTo("owner");
        assertThat(ownerSession.getIdentity().getGroups()).isEqualTo(owner.getGroups());
        assertThat(ownerSession.getIdentity().getPrincipal()).isEqualTo(owner.getPrincipal());
        assertThat(ownerSession.getIdentity().getEnabledSystemRoles()).isEqualTo(owner.getEnabledSystemRoles());
        assertThat(ownerSession.getIdentity().getConnectorRole()).contains(role);
        // extra credentials belong to the session identity only
        assertThat(ownerSession.getIdentity().getExtraCredentials()).isEmpty();
        assertThat(ownerSession.getQueryId()).isEqualTo(session.getQueryId().toString());
        assertThat(ownerSession.getStart()).isEqualTo(session.getStart());

        TableHandle userTable = tableHandle(ResolvingIdentity.from(session.getIdentity().toConnectorIdentity(TEST_CATALOG_NAME)));
        assertThat(userTable.isResolvedAs(session.getIdentity())).isTrue();
        ConnectorSession userSession = session.toTableConnectorSession(userTable);
        assertThat(userSession.getIdentity().getUser()).isEqualTo("user");
        assertThat(userSession.getIdentity().getExtraCredentials()).isEqualTo(Map.of("user_credential", "value"));

        // e.g. a SECURITY DEFINER view owned by the querying user: the table was resolved without the session credentials
        TableHandle ownedViewTable = tableHandle(ResolvingIdentity.from(ConnectorIdentity.ofUser("user")));
        assertThat(ownedViewTable.isResolvedAs(session.getIdentity())).isFalse();
        ConnectorSession ownedViewSession = session.toTableConnectorSession(ownedViewTable);
        assertThat(ownedViewSession.getIdentity().getUser()).isEqualTo("user");
        assertThat(ownedViewSession.getIdentity().getExtraCredentials()).isEmpty();
    }

    @Test
    public void testWithIdentityPreservesQueryContext()
    {
        Identity invoker = Identity.ofUser("invoker");
        Identity originalIdentity = Identity.ofUser("authenticated_user");
        Identity owner = Identity.forUser("view_owner")
                .withGroups(Set.of("view_owners"))
                .build();
        TransactionId transactionId = TransactionId.create();
        Session session = testSessionBuilder()
                .setIdentity(invoker)
                .setOriginalIdentity(originalIdentity)
                .setQueryId(new QueryId("identity_redirection"))
                .setStart(Instant.ofEpochSecond(123))
                .setClientTags(Set.of("client_tag"))
                .addPreparedStatement("statement", "SELECT 1")
                .setTransactionId(transactionId)
                .build()
                .withProperties(
                        Map.of("join_distribution_type", "BROADCAST"),
                        Map.of("destination", Map.of("lookup_context", "preserved")));

        Session ownerSession = session.withIdentity(owner);

        assertThat(ownerSession.getIdentity()).isSameAs(owner);
        assertThat(ownerSession.getOriginalIdentity()).isSameAs(originalIdentity);
        assertThat(ownerSession.getRequiredTransactionId()).isEqualTo(transactionId);
        assertThat(ownerSession.getSystemProperties()).isEqualTo(session.getSystemProperties());
        assertThat(ownerSession.getCatalogProperties()).isEqualTo(session.getCatalogProperties());
        assertThat(ownerSession)
                .usingRecursiveComparison()
                .ignoringFields("identity")
                .isEqualTo(session);
        assertThat(session.getIdentity()).isSameAs(invoker);
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

    private static TableHandle tableHandle(ResolvingIdentity resolvingIdentity)
    {
        return new TableHandle(TEST_CATALOG_HANDLE, TEST_TABLE_HANDLE.connectorHandle(), TEST_TABLE_HANDLE.transaction(), resolvingIdentity);
    }
}
