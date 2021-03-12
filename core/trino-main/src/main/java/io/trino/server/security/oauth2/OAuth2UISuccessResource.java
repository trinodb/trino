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

import io.trino.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import java.io.IOException;

import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static io.trino.server.ui.OAuthWebUiCookie.OAUTH2_COOKIE;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

@Path(OAuth2UISuccessResource.UI_LOGIN_SUCCESS)
public class OAuth2UISuccessResource
{
    public static final String UI_LOGIN_SUCCESS = UI_LOCATION + "login/success";

    private final OAuth2Service service;

    @Inject
    public OAuth2UISuccessResource(OAuth2Service service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @GET
    @Produces(TEXT_HTML)
    @ResourceSecurity(WEB_UI)
    public String getSuccessfulOAuth2LoginPage(@CookieParam(OAUTH2_COOKIE) String token)
            throws IOException
    {
        return service.getSuccessWebHtml(token);
    }
}
