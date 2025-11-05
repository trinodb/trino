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
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

import static java.util.concurrent.TimeUnit.SECONDS;

public class LokiConfig
{
    private URI uri;
    private Duration queryTimeout = new Duration(10, SECONDS);

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("loki.uri")
    @ConfigDescription("Loki API endpoint base")
    public LokiConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    @MinDuration("1s")
    public Duration getQueryTimeout()
    {
        return queryTimeout;
    }

    @Config("loki.query-timeout")
    @ConfigDescription("How much time a query to Loki has before timing out")
    public LokiConfig setQueryTimeout(Duration queryTimeout)
    {
        this.queryTimeout = queryTimeout;
        return this;
    }
}
