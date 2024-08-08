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
package io.trino.plugin.jdbc;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public class DefaultJdbcMetadataFactory
        implements JdbcMetadataFactory
{
    private final JdbcClient jdbcClient;
    private final TimestampTimeZoneDomain timestampTimeZoneDomain;
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;
    private final IdentityCacheMapping identityCacheMapping;

    @Inject
    public DefaultJdbcMetadataFactory(
            JdbcClient jdbcClient,
            TimestampTimeZoneDomain timestampTimeZoneDomain,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners,
            IdentityCacheMapping identityCacheMapping)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.timestampTimeZoneDomain = requireNonNull(timestampTimeZoneDomain, "timestampTimeZoneDomain is null");
        this.jdbcQueryEventListeners = ImmutableSet.copyOf(requireNonNull(jdbcQueryEventListeners, "queryEventListeners is null"));
        this.identityCacheMapping = requireNonNull(identityCacheMapping, "identityCacheMapping is null");
    }

    @Override
    public JdbcMetadata create(JdbcTransactionHandle transaction)
    {
        return create(new CachingJdbcClient(
                Ticker.systemTicker(),
                jdbcClient,
                Set.of(),
                identityCacheMapping,
                new Duration(1, DAYS),
                new Duration(1, DAYS),
                new Duration(1, DAYS),
                new Duration(1, DAYS),
                true,
                Integer.MAX_VALUE));
    }

    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new DefaultJdbcMetadata(
                transactionCachingJdbcClient,
                timestampTimeZoneDomain,
                true,
                jdbcQueryEventListeners);
    }
}
