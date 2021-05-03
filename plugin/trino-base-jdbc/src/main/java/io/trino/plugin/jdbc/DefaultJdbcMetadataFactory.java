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

import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public class DefaultJdbcMetadataFactory
        implements JdbcMetadataFactory
{
    private final JdbcClient jdbcClient;
    private final boolean allowDropTable;

    @Inject
    public DefaultJdbcMetadataFactory(JdbcClient jdbcClient, JdbcMetadataConfig config)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
    }

    @Override
    public JdbcMetadata create(JdbcTransactionHandle transaction)
    {
        // Session stays the same per transaction, therefore session properties don't need to
        // be a part of cache keys in CachingJdbcClient.
        return create(new CachingJdbcClient(
                        jdbcClient,
                        Set.of(),
                        new SingletonJdbcIdentityCacheMapping(),
                        new Duration(1, DAYS), true),
                allowDropTable);
    }

    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient, boolean allowDropTable)
    {
        return new DefaultJdbcMetadata(transactionCachingJdbcClient, allowDropTable);
    }
}
