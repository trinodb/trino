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

import io.prestosql.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.util.Optional;

import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_API_PREFIX;
import static io.prestosql.server.ui.FormWebUiAuthenticationFilter.PRESTO_UI_COOKIE;
import static io.prestosql.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FOUND;

@Path(OAUTH2_API_PREFIX)
public class OAuth2Resource
{
    static final String OAUTH2_API_PREFIX = "/oauth2";
    static final String CALLBACK_ENDPOINT = "/callback";

    private final OAuth2Service service;

    @Inject
    public OAuth2Resource(OAuth2Service service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path(CALLBACK_ENDPOINT)
    public Response callback(
            @QueryParam("state") String state,
            @QueryParam("code") String code,
            @QueryParam("error") String error,
            @QueryParam("error_description") String errorDescription,
            @QueryParam("error_uri") String errorUri,
            @Context SecurityContext securityContext)
    {
        if (error != null && !error.isBlank()) {
            return Response
                    .status(BAD_REQUEST)
                    .entity(new OAuth2Error(error, Optional.ofNullable(errorDescription), Optional.ofNullable(errorUri)))
                    .build();
        }
        if (state == null || state.isBlank()) {
            return Response
                    .status(BAD_REQUEST)
                    .entity(new OAuth2Error("State is null or empty", Optional.empty(), Optional.empty()))
                    .build();
        }
        if (code == null || code.isBlank()) {
            return Response
                    .status(BAD_REQUEST)
                    .entity(new OAuth2Error("Code is null or empty", Optional.empty(), Optional.empty()))
                    .build();
        }
        String accessToken = service.finishChallenge(new State(state), code);
        return Response
                .status(FOUND)
                .header(LOCATION, UI_LOCATION)
                .cookie(new NewCookie(
                        PRESTO_UI_COOKIE,
                        accessToken,
                        UI_LOCATION,
                        null,
                        Cookie.DEFAULT_VERSION,
                        null,
                        NewCookie.DEFAULT_MAX_AGE,
                        null, // TODO: get from token
                        securityContext.isSecure(),
                        true))
                .build();
    }
}
