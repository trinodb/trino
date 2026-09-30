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
package io.trino.plugin.ydb;

import com.google.inject.Inject;
import io.trino.plugin.base.cache.identity.IdentityCacheMapping;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;

import java.util.Set;

public class YdbMetadataFactory
        extends DefaultJdbcMetadataFactory
{
    private final TimestampTimeZoneDomain timestampTimeZoneDomain;
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    @Inject
    public YdbMetadataFactory(
            JdbcClient jdbcClient,
            TimestampTimeZoneDomain timestampTimeZoneDomain,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners,
            IdentityCacheMapping identityCacheMapping)
    {
        super(jdbcClient, timestampTimeZoneDomain, jdbcQueryEventListeners, identityCacheMapping);
        this.timestampTimeZoneDomain = timestampTimeZoneDomain;
        this.jdbcQueryEventListeners = jdbcQueryEventListeners;
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new YdbMetadata(transactionCachingJdbcClient, timestampTimeZoneDomain, jdbcQueryEventListeners);
    }
}
