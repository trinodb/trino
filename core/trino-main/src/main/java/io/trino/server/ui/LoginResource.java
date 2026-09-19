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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_AUTH_INFO;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGIN_FORM;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGOUT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.getDeleteCookies;
import static jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.Response.Status.METHOD_NOT_ALLOWED;
import static java.util.Objects.requireNonNull;

@Path("")
@ResourceSecurity(WEB_UI)
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class LoginResource
{
    private static final String DEPRECATED_UI_LOGIN = "/ui/login";

    private final FormWebUiAuthenticationFilter formWebUiAuthenticationManager;

    @Inject
    public LoginResource(FormWebUiAuthenticationFilter formWebUiAuthenticationManager)
    {
        this.formWebUiAuthenticationManager = requireNonNull(formWebUiAuthenticationManager, "formWebUiAuthenticationManager is null");
    }

    @GET
    @Path(UI_AUTH_INFO)
    public AuthInfo getAuthInfo(@Context ContainerRequestContext request, @Context SecurityContext securityContext)
    {
        boolean isPasswordAllowed = formWebUiAuthenticationManager.isPasswordAllowed(securityContext.isSecure());
        Optional<String> username = formWebUiAuthenticationManager.getAuthenticatedUsername(request);
        return new AuthInfo("form", isPasswordAllowed, username.isPresent(), username);
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Path(DEPRECATED_UI_LOGIN)
    @Consumes(APPLICATION_FORM_URLENCODED)
    @Produces(TEXT_PLAIN)
    public Response deprecatedLogin()
    {
        return Response.status(METHOD_NOT_ALLOWED)
                .allow("GET", "HEAD", "OPTIONS")
                .type(TEXT_PLAIN)
                .entity("Web UI form login moved to POST /ui/auth/login in Trino 483. The new endpoint expects an application/json request.")
                .build();
    }

    @POST
    @Path(UI_LOGIN_FORM)
    public Response login(LoginForm loginForm, @Context SecurityContext securityContext)
    {
        String username = emptyToNull(loginForm.username());
        String password = emptyToNull(loginForm.password());

        if (!formWebUiAuthenticationManager.isAuthenticationEnabled(securityContext.isSecure())) {
            throw new ForbiddenException();
        }

        Optional<NewCookie[]> authenticationCookie = formWebUiAuthenticationManager.checkLoginCredentials(username, password, securityContext.isSecure());
        if (authenticationCookie.isEmpty()) {
            throw new ForbiddenException();
        }

        return Response.noContent()
                .cookie(authenticationCookie.get())
                .build();
    }

    @GET
    @Path(UI_LOGOUT)
    public Response logout(@Context HttpHeaders httpHeaders, @Context SecurityContext securityContext)
    {
        return Response.noContent()
                .cookie(getDeleteCookies(httpHeaders.getCookies(), securityContext.isSecure()))
                .build();
    }

    public record LoginForm(@JsonProperty String username, @JsonProperty String password)
    {
        @Override
        public String username()
        {
            return username;
        }

        @Override
        public String password()
        {
            return password;
        }
    }
}
