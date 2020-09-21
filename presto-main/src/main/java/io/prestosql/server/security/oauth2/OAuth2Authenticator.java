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

import io.prestosql.server.security.Authenticator;
import io.prestosql.server.security.RedirectAuthenticationException;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OAuth2Authenticator
        implements Authenticator
{
    private final OAuth2Service service;

    @Inject
    public OAuth2Authenticator(OAuth2Service service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws RedirectAuthenticationException
    {
        Challenge.Started challenge = service.startChallenge();
        throw new RedirectAuthenticationException(
                "Unauthorized",
                format("Bearer realm=\"Presto\", authorizationUrl=\"%s\", status=\"%s\"",
                        challenge.getAuthorizationUrl(),
                        challenge.getStatus()),
                challenge.getAuthorizationUrl());
    }
}
