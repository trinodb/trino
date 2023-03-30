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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.execution.QueryIdGenerator;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TimeZoneKey;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TestingConnectorSession
        implements ConnectorSession
{
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    public static final ConnectorSession SESSION = builder().build();

    private final String queryId = queryIdGenerator.createNextQueryId().toString();
    private final ConnectorIdentity identity;
    private final Optional<String> source;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final Optional<String> traceToken;
    private final Instant start;
    private final Map<String, PropertyMetadata<?>> properties;
    private final Map<String, Object> propertyValues;

    private TestingConnectorSession(
            ConnectorIdentity identity,
            Optional<String> source,
            Optional<String> traceToken,
            TimeZoneKey timeZoneKey,
            Locale locale,
            Instant start,
            List<PropertyMetadata<?>> propertyMetadatas,
            Map<String, Object> propertyValues)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.source = requireNonNull(source, "source is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.start = start;
        this.properties = Maps.uniqueIndex(propertyMetadatas, PropertyMetadata::getName);
        this.propertyValues = ImmutableMap.copyOf(propertyValues);
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public Optional<String> getSource()
    {
        return source;
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return identity;
    }

    @Override
    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @Override
    public Locale getLocale()
    {
        return locale;
    }

    @Override
    public Instant getStart()
    {
        return start;
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    @Override
    public <T> T getProperty(String name, Class<T> type)
    {
        PropertyMetadata<?> metadata = properties.get(name);
        if (metadata == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }
        Object value = propertyValues.get(name);
        if (value == null) {
            return type.cast(metadata.getDefaultValue());
        }
        return type.cast(metadata.decode(value));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", getUser())
                .add("source", source.orElse(null))
                .add("traceToken", traceToken.orElse(null))
                .add("timeZoneKey", timeZoneKey)
                .add("locale", locale)
                .add("start", start)
                .add("properties", propertyValues)
                .omitNullValues()
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ConnectorIdentity identity = ConnectorIdentity.ofUser("user");
        private Optional<String> source = Optional.of("test");
        private TimeZoneKey timeZoneKey = UTC_KEY;
        private final Locale locale = ENGLISH;
        private Optional<String> traceToken = Optional.empty();
        private Optional<Instant> start = Optional.empty();
        private List<PropertyMetadata<?>> propertyMetadatas = ImmutableList.of();
        private Map<String, Object> propertyValues = ImmutableMap.of();

        public Builder setIdentity(ConnectorIdentity identity)
        {
            this.identity = requireNonNull(identity, "identity is null");
            return this;
        }

        public Builder setTimeZoneKey(TimeZoneKey timeZoneKey)
        {
            this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
            return this;
        }

        public Builder setStart(Instant start)
        {
            this.start = Optional.of(start);
            return this;
        }

        public Builder setSource(String source)
        {
            this.source = Optional.of(source);
            return this;
        }

        public Builder setTraceToken(String token)
        {
            this.traceToken = Optional.of(token);
            return this;
        }

        public Builder setPropertyMetadata(List<PropertyMetadata<?>> propertyMetadatas)
        {
            requireNonNull(propertyMetadatas, "propertyMetadatas is null");
            this.propertyMetadatas = propertyMetadatas;
            return this;
        }

        public Builder setPropertyValues(Map<String, Object> propertyValues)
        {
            requireNonNull(propertyValues, "propertyValues is null");
            this.propertyValues = ImmutableMap.copyOf(propertyValues);
            return this;
        }

        public TestingConnectorSession build()
        {
            return new TestingConnectorSession(
                    identity,
                    source,
                    traceToken,
                    timeZoneKey,
                    locale,
                    start.orElseGet(Instant::now),
                    propertyMetadatas,
                    propertyValues);
        }
    }
}
