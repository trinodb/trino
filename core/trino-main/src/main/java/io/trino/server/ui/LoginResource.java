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

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Verify.verify;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.DISABLED_LOCATION_URI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.LOGIN_FORM;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.LOGIN_FORM_URI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGIN;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGOUT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.getDeleteCookie;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.redirectFromSuccessfulLoginResponse;
import static jakarta.ws.rs.core.MediaType.TEXT_HTML;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Path("")
public class LoginResource
{
    private static final String REPLACEMENT_TEXT = "var hidePassword = false; // This value will be replaced";
    private final FormWebUiAuthenticationFilter formWebUiAuthenticationManager;
    private final String loginHtml;

    @Inject
    public LoginResource(FormWebUiAuthenticationFilter formWebUiAuthenticationManager)
            throws IOException
    {
        this.formWebUiAuthenticationManager = requireNonNull(formWebUiAuthenticationManager, "formWebUiAuthenticationManager is null");
        this.loginHtml = Resources.toString(Resources.getResource(getClass(), "/webapp/login.html"), UTF_8);
        verify(loginHtml.contains(REPLACEMENT_TEXT), "login.html does not contain the replacement text");
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path(LOGIN_FORM)
    public Response getFile(@Context SecurityContext securityContext)
    {
        boolean passwordAllowed = formWebUiAuthenticationManager.isPasswordAllowed(securityContext.isSecure());
        return Response.ok(loginHtml.replace(REPLACEMENT_TEXT, "var hidePassword = " + !passwordAllowed + ";"))
                .type(TEXT_HTML)
                .build();
    }

    @ResourceSecurity(WEB_UI)
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

        if (!formWebUiAuthenticationManager.isAuthenticationEnabled(securityContext.isSecure())) {
            return Response.seeOther(DISABLED_LOCATION_URI).build();
        }

        Optional<NewCookie> authenticationCookie = formWebUiAuthenticationManager.checkLoginCredentials(username, password, securityContext.isSecure());
        if (authenticationCookie.isEmpty()) {
            // authentication failed, redirect back to the login page
            return Response.seeOther(LOGIN_FORM_URI).build();
        }

        return redirectFromSuccessfulLoginResponse(redirectPath)
                .cookie(authenticationCookie.get())
                .build();
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path(UI_LOGOUT)
    public Response logout(@Context HttpHeaders httpHeaders, @Context UriInfo uriInfo, @Context SecurityContext securityContext)
    {
        URI redirectLocation;
        if (formWebUiAuthenticationManager.isAuthenticationEnabled(securityContext.isSecure())) {
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
