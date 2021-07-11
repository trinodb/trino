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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.client.ProtocolHeaders;
import io.trino.connector.CatalogName;
import io.trino.server.SessionContext;
import io.trino.spi.security.Identity;
import io.trino.spi.session.ResourceEstimates;
import io.trino.transaction.TransactionId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Strings.emptyToNull;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

public class TestingSessionContext
        implements SessionContext
{
    private final Session session;

    public TestingSessionContext(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public ProtocolHeaders getProtocolHeaders()
    {
        return TRINO_HEADERS;
    }

    @Override
    public Optional<Identity> getAuthenticatedIdentity()
    {
        return Optional.empty();
    }

    @Override
    public Identity getIdentity()
    {
        return session.getIdentity();
    }

    @Override
    public Optional<String> getCatalog()
    {
        return session.getCatalog();
    }

    @Override
    public Optional<String> getSchema()
    {
        return session.getSchema();
    }

    @Override
    public Optional<String> getPath()
    {
        return Optional.ofNullable(emptyToNull(session.getPath().toString()));
    }

    @Override
    public Optional<String> getSource()
    {
        return session.getSource();
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return session.getTraceToken();
    }

    @Override
    public Optional<String> getRemoteUserAddress()
    {
        return session.getRemoteUserAddress();
    }

    @Override
    public Optional<String> getUserAgent()
    {
        return session.getUserAgent();
    }

    @Override
    public Optional<String> getClientInfo()
    {
        return session.getClientInfo();
    }

    @Override
    public Set<String> getClientTags()
    {
        return session.getClientTags();
    }

    @Override
    public Set<String> getClientCapabilities()
    {
        return session.getClientCapabilities();
    }

    @Override
    public ResourceEstimates getResourceEstimates()
    {
        return session.getResourceEstimates();
    }

    @Override
    public Optional<String> getTimeZoneId()
    {
        return Optional.of(session.getTimeZoneKey().getId());
    }

    @Override
    public Optional<String> getLanguage()
    {
        return Optional.of(session.getLocale().getLanguage());
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return session.getSystemProperties();
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        ImmutableMap.Builder<String, Map<String, String>> catalogSessionProperties = ImmutableMap.builder();
        for (Entry<CatalogName, Map<String, String>> entry : session.getConnectorProperties().entrySet()) {
            catalogSessionProperties.put(entry.getKey().getCatalogName(), entry.getValue());
        }
        return catalogSessionProperties.build();
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return session.getPreparedStatements();
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        return session.getTransactionId();
    }

    @Override
    public boolean supportClientTransaction()
    {
        return session.isClientTransactionSupport();
    }
}
