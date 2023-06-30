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
package io.trino.server.security.oauth2;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class OidcDiscoveryConfig
{
    private Duration discoveryTimeout = new Duration(30, SECONDS);
    private boolean userinfoEndpointEnabled = true;

    @NotNull
    public Duration getDiscoveryTimeout()
    {
        return discoveryTimeout;
    }

    @Config("http-server.authentication.oauth2.oidc.discovery.timeout")
    @ConfigDescription("OpenID Connect discovery timeout")
    public OidcDiscoveryConfig setDiscoveryTimeout(Duration discoveryTimeout)
    {
        this.discoveryTimeout = discoveryTimeout;
        return this;
    }

    public boolean isUserinfoEndpointEnabled()
    {
        return userinfoEndpointEnabled;
    }

    @Config("http-server.authentication.oauth2.oidc.use-userinfo-endpoint")
    @ConfigDescription("Use userinfo endpoint from OpenID connect metadata document")
    public OidcDiscoveryConfig setUserinfoEndpointEnabled(boolean userinfoEndpointEnabled)
    {
        this.userinfoEndpointEnabled = userinfoEndpointEnabled;
        return this;
    }
}
