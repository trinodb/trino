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
package io.trino.plugin.loki;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class LokiConnectorConfig
{
    private URI lokiUri;
    private String user;
    private String password;
    private Duration queryTimeout = new Duration(10, TimeUnit.SECONDS);

    @NotNull
    public URI getLokiUri()
    {
        return lokiUri;
    }

    @Config("loki.uri")
    @ConfigDescription("Loki API endpoint base")
    public LokiConnectorConfig setLokiUri(URI lokiUri)
    {
        this.lokiUri = lokiUri;
        return this;
    }

    @NotNull
    public Optional<String> getUser()
    {
        return Optional.ofNullable(user);
    }

    @Config("loki.auth.user")
    public LokiConnectorConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("loki.auth.password")
    @ConfigSecuritySensitive
    public LokiConnectorConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @MinDuration("1s")
    public Duration getQueryTimeout()
    {
        return queryTimeout;
    }

    @Config("loki.query-timeout")
    @ConfigDescription("How much time a query to Loki has before timing out")
    public LokiConnectorConfig setQueryTimeout(Duration queryTimeout)
    {
        this.queryTimeout = queryTimeout;
        return this;
    }

    @AssertFalse(message = "Either User and password must be specified or none.")
    public boolean isConfigValid()
    {
        return getPassword().isPresent() ^ getUser().isPresent();
    }
}
