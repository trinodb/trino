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
package com.starburstdata.presto.plugin.snowflake.auth;

import io.prestosql.plugin.jdbc.JdbcIdentity;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import static java.util.Objects.requireNonNull;

public class StatsCollectingSnowflakeAuthClient
        implements SnowflakeAuthClient
{
    private final SnowflakeAuthClient delegate;
    private final SnowflakeAuthClientStats stats = new SnowflakeAuthClientStats();

    public StatsCollectingSnowflakeAuthClient(SnowflakeAuthClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Managed
    @Flatten
    public SnowflakeAuthClientStats getStats()
    {
        return stats;
    }

    @Override
    public SamlRequest generateSamlRequest(JdbcIdentity identity)
    {
        return stats.getGenerateSamlRequest().call(() -> delegate.generateSamlRequest(identity));
    }

    @Override
    public OauthCredential requestOauthToken(SamlResponse samlResponse)
    {
        return stats.getRequestOauthToken().call(() -> delegate.requestOauthToken(samlResponse));
    }

    @Override
    public OauthCredential refreshCredential(OauthCredential credential)
    {
        return stats.getRefreshCredential().call(() -> delegate.refreshCredential(credential));
    }
}
