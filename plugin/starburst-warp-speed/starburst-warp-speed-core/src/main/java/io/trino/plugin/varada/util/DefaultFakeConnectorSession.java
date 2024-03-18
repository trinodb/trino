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
package io.trino.plugin.varada.util;

import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;

public class DefaultFakeConnectorSession
        implements ConnectorSession
{
    public static final ConnectorSession INSTANCE = new DefaultFakeConnectorSession();

    private DefaultFakeConnectorSession() {}

    @Override
    public String getQueryId()
    {
        return "a";
    }

    @Override
    public Optional<String> getSource()
    {
        return Optional.empty();
    }

    @Override
    public String getUser()
    {
        return "fake-user";
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return ConnectorIdentity.forUser("fake-user").build();
    }

    @Override
    public TimeZoneKey getTimeZoneKey()
    {
        return TimeZoneKey.getTimeZoneKey("GMT");
    }

    @Override
    public Locale getLocale()
    {
        return Locale.ENGLISH;
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return Optional.empty();
    }

    @Override
    public Instant getStart()
    {
        return Instant.now();
    }

    @Override
    public <T> T getProperty(String name, Class<T> type)
    {
        // TODO - fix for hive removal
        if ("timestamp_precision".equals(name)) {
            return (T) HiveTimestampPrecision.DEFAULT_PRECISION;
        }
        if ("allow_legacy_snapshot_syntax".equals(name)) {
            return (T) Boolean.FALSE;
        }
        return null;
    }
}
