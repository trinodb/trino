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
package io.prestosql.server.security.oauth2;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.oauth.OAuth20Service;
import io.airlift.http.server.HttpServerConfig;

import javax.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static io.prestosql.server.security.oauth2.OAuth2Resource.CALLBACK_ENDPOINT;
import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_API_PREFIX;
import static java.util.Objects.requireNonNull;

public class OAuth2Service
{
    private final HttpServerConfig httpServerConfig;
    private final OAuth20Service service;

    @Inject
    public OAuth2Service(OAuth2Config oauth2Config, HttpServerConfig httpServerConfig)
    {
        requireNonNull(oauth2Config, "oauth2Config is null");
        this.httpServerConfig = requireNonNull(httpServerConfig, "httpServerConfig is null");
        this.service = new ServiceBuilder(oauth2Config.getClientId())
                .apiSecret(oauth2Config.getClientSecret())
                .defaultScope("openid")
                .callback(serverUrl() + OAUTH2_API_PREFIX + CALLBACK_ENDPOINT)
                .build(new DefaultApi20()
                {
                    @Override
                    public String getAccessTokenEndpoint()
                    {
                        return oauth2Config.getTokenUrl();
                    }

                    @Override
                    protected String getAuthorizationBaseUrl()
                    {
                        return oauth2Config.getAuthUrl();
                    }
                });
    }

    Challenge.Started startChallenge()
    {
        State state = State.randomState();
        String authorizationUrl = service.getAuthorizationUrl(state.get());
        return new Challenge.Started(state, authorizationUrl);
    }

    String finishChallenge(State state, String code)
    {
        requireNonNull(state, "state is null");
        requireNonNull(code, "code is null");
        try {
            return service.getAccessToken(code).getAccessToken();
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private String serverUrl()
    {
        // TODO: use server external address
        String host = "host.testcontainers.internal";
        if (httpServerConfig.isHttpsEnabled()) {
            return "https://" + host + ":" + httpServerConfig.getHttpsPort();
        }
        else {
            return "http://" + host + ":" + httpServerConfig.getHttpPort();
        }
    }
}
