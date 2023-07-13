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
package io.trino.server.ui;

import com.google.inject.Inject;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;

import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.redirectAllFormLoginToUi;
import static java.util.Objects.requireNonNull;

public class FixedUserWebUiAuthenticationFilter
        implements WebUiAuthenticationFilter
{
    private final Identity webUiIdentity;

    @Inject
    public FixedUserWebUiAuthenticationFilter(FixedUserWebUiConfig config)
    {
        this(basicIdentity(config.getUsername()));
    }

    public FixedUserWebUiAuthenticationFilter(Identity webUiIdentity)
    {
        this.webUiIdentity = requireNonNull(webUiIdentity, "webUiIdentity is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        if (redirectAllFormLoginToUi(request)) {
            return;
        }

        setAuthenticatedIdentity(request, webUiIdentity);
    }

    private static Identity basicIdentity(String username)
    {
        requireNonNull(username, "username is null");
        return Identity.forUser(username)
                .withPrincipal(new BasicPrincipal(username))
                .build();
    }
}
