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
package io.trino.plugin.lance;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class LanceConfig
{
    /**
     * URL used to access a lancedb via REST client
     */
    private URI lanceDbUri = URI.create("dummy://db.connect");

    private Duration connectionTimeout = new Duration(1, TimeUnit.MINUTES);

    private int fetchRetryCount = 5;

    private String connectorType = Type.DATASET.name();

    public URI getLanceDbUri()
    {
        return lanceDbUri;
    }

    @Config("lance.uri")
    public LanceConfig setLanceDbUri(String lanceDbUri)
    {
        this.lanceDbUri = URI.create(lanceDbUri);
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("lance.connection-timeout")
    public LanceConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public Integer getFetchRetryCount()
    {
        return this.fetchRetryCount;
    }

    @Config("lance.connection-retry-count")
    public LanceConfig setFetchRetryCount(int fetchRetryCount)
    {
        this.fetchRetryCount = fetchRetryCount;
        return this;
    }

    public Type getConnectorType()
    {
        return Type.valueOf(this.connectorType);
    }

    @Config("lance.connector-type")
    public LanceConfig setConnectorType(String connectorType)
    {
        // use enum to check and ensure connector type is supported
        this.connectorType = Type.valueOf(connectorType).name();
        return this;
    }

    public enum Type
    {
        DATASET,
        FRAGMENT
    }
}
