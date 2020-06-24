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

import javax.inject.Inject;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static io.prestosql.server.ui.FormWebUiAuthenticationManager.DISABLED_LOCATION_URI;
import static io.prestosql.server.ui.FormWebUiAuthenticationManager.LOGIN_FORM_URI;
import static io.prestosql.server.ui.FormWebUiAuthenticationManager.UI_LOGIN;
import static io.prestosql.server.ui.FormWebUiAuthenticationManager.UI_LOGOUT;
import static io.prestosql.server.ui.FormWebUiAuthenticationManager.getDeleteCookie;
import static io.prestosql.server.ui.FormWebUiAuthenticationManager.redirectFromSuccessfulLoginResponse;
import static java.util.Objects.requireNonNull;

@Path("")
public class LoginResource
{
    private final FormWebUiAuthenticationManager formWebUiAuthenticationManager;

    @Inject
    public LoginResource(FormWebUiAuthenticationManager formWebUiAuthenticationManager)
    {
        this.formWebUiAuthenticationManager = requireNonNull(formWebUiAuthenticationManager, "formWebUiAuthenticationManager is null");
    }

    @POST
    @Path(UI_LOGIN)
    public Response login(
            @FormParam("username") String username,
            @FormParam("password") String password,
            @FormParam("redirectPath") String redirectPath,
            @Context SecurityContext securityContext)
    {
        username = emptyToNull(username);
        password = emptyToNull(password);
        redirectPath = emptyToNull(redirectPath);

        if (!formWebUiAuthenticationManager.isAuthenticationEnabled(securityContext)) {
            return Response.seeOther(DISABLED_LOCATION_URI).build();
        }

        Optional<NewCookie> authenticationCookie = formWebUiAuthenticationManager.checkLoginCredentials(username, password, securityContext.isSecure());
        if (!authenticationCookie.isPresent()) {
            // authentication failed, redirect back to the login page
            return Response.seeOther(LOGIN_FORM_URI).build();
        }

        return redirectFromSuccessfulLoginResponse(redirectPath)
                .cookie(authenticationCookie.get())
                .build();
    }

    @GET
    @Path(UI_LOGOUT)
    public Response logout(@Context HttpHeaders httpHeaders, @Context UriInfo uriInfo, @Context SecurityContext securityContext)
    {
        URI redirectLocation;
        if (formWebUiAuthenticationManager.isAuthenticationEnabled(securityContext)) {
            redirectLocation = LOGIN_FORM_URI;
        }
        else {
            redirectLocation = DISABLED_LOCATION_URI;
        }
        return Response.seeOther(redirectLocation)
                .cookie(getDeleteCookie(securityContext.isSecure()))
                .build();
    }
}
