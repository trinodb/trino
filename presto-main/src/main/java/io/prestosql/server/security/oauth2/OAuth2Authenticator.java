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
import io.prestosql.server.security.Authenticator;
import io.prestosql.server.security.RedirectAuthenticationException;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.security.SecureRandom;
import java.util.Base64;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OAuth2Authenticator
        implements Authenticator
{
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final OAuth20Service service;

    @Inject
    public OAuth2Authenticator(OAuth2Config oauth2Config, HttpServerConfig httpServerConfig)
    {
        requireNonNull(oauth2Config, "oauth2Config is null");
        requireNonNull(httpServerConfig, "httpServerConfig is null");
        this.service = new ServiceBuilder(oauth2Config.getClientId())
                .apiSecret(oauth2Config.getClientSecret())
                .defaultScope("openid")
                .callback(format("https://127.0.0.1:%s/callback", httpServerConfig.getHttpsPort()))
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

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws RedirectAuthenticationException
    {
        String authorizationUrl = service.getAuthorizationUrl(randomState());
        throw new RedirectAuthenticationException(
                "Unauthorized",
                format("Bearer realm=\"Presto\", redirectUrl=\"%s\", status=\"STARTED\"", authorizationUrl),
                authorizationUrl);
    }

    private static String randomState()
    {
        byte[] randomBytes = new byte[16];
        SECURE_RANDOM.nextBytes(randomBytes);
        return Base64.getEncoder().encodeToString(randomBytes);
    }
}
