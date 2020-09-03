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

import io.prestosql.server.security.ResourceSecurity;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import static io.prestosql.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.prestosql.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION_URI;
import static io.prestosql.server.ui.FormWebUiAuthenticationFilter.UI_LOGOUT;
import static io.prestosql.server.ui.OAuthWebUiCookie.delete;

@Path(UI_LOGOUT)
public class OAuth2WebUiLogoutResource
{
    @ResourceSecurity(WEB_UI)
    @GET
    public Response logout(@Context HttpHeaders httpHeaders, @Context UriInfo uriInfo, @Context SecurityContext securityContext)
    {
        return Response.seeOther(UI_LOCATION_URI)
                .cookie(delete(securityContext.isSecure()))
                .build();
    }
}
