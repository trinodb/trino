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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FullConnectorSession
        implements ConnectorSession
{
    private final Session session;
    private final ConnectorIdentity identity;
    private final Map<String, String> properties;
    private final CatalogHandle catalogHandle;
    private final String catalogName;
    private final SessionPropertyManager sessionPropertyManager;

    public FullConnectorSession(Session session, ConnectorIdentity identity)
    {
        this.session = requireNonNull(session, "session is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.properties = null;
        this.catalogHandle = null;
        this.catalogName = null;
        this.sessionPropertyManager = null;
    }

    public FullConnectorSession(
            Session session,
            ConnectorIdentity identity,
            Map<String, String> properties,
            CatalogHandle catalogHandle,
            String catalogName,
            SessionPropertyManager sessionPropertyManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    public Session getSession()
    {
        return session;
    }

    @Override
    public String getQueryId()
    {
        return session.getQueryId().toString();
    }

    @Override
    public Optional<String> getSource()
    {
        return session.getSource();
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return identity;
    }

    @Override
    public TimeZoneKey getTimeZoneKey()
    {
        return session.getTimeZoneKey();
    }

    @Override
    public Locale getLocale()
    {
        return session.getLocale();
    }

    @Override
    public Instant getStart()
    {
        return session.getStart();
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return session.getTraceToken();
    }

    @Override
    public <T> T getProperty(String propertyName, Class<T> type)
    {
        if (properties == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("Unknown session property: %s.%s", catalogName, propertyName));
        }

        return sessionPropertyManager.decodeCatalogPropertyValue(catalogHandle, catalogName, propertyName, properties.get(propertyName), type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", getQueryId())
                .add("user", getUser())
                .add("source", getSource().orElse(null))
                .add("traceToken", getTraceToken().orElse(null))
                .add("timeZoneKey", getTimeZoneKey())
                .add("locale", getLocale())
                .add("start", getStart())
                .add("properties", properties)
                .omitNullValues()
                .toString();
    }
}
