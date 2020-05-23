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
package io.prestosql.server.ui;

import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static io.prestosql.server.ServletSecurityUtils.withAuthenticatedIdentity;
import static io.prestosql.server.ui.FormWebUiAuthenticationManager.redirectAllFormLoginToUi;
import static java.util.Objects.requireNonNull;

public class FixedUserWebUiAuthenticationManager
        implements WebUiAuthenticationManager
{
    private final Identity webUiIdentity;

    @Inject
    public FixedUserWebUiAuthenticationManager(FixedUserWebUiConfig config)
    {
        this(basicIdentity(requireNonNull(config, "config is null").getUsername()));
    }

    public FixedUserWebUiAuthenticationManager(Identity webUiIdentity)
    {
        this.webUiIdentity = requireNonNull(webUiIdentity, "webUiIdentity is null");
    }

    @Override
    public void handleUiRequest(HttpServletRequest request, HttpServletResponse response, FilterChain nextFilter)
            throws IOException, ServletException
    {
        if (redirectAllFormLoginToUi(request, response)) {
            return;
        }

        withAuthenticatedIdentity(nextFilter, request, response, webUiIdentity);
    }

    private static Identity basicIdentity(String username)
    {
        requireNonNull(username, "username is null");
        return Identity.forUser(username)
                .withPrincipal(new BasicPrincipal(username))
                .build();
    }
}
