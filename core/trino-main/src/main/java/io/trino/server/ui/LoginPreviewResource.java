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
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_PREVIEW_AUTH_INFO;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_PREVIEW_LOGIN;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_PREVIEW_LOGOUT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.getDeleteCookies;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("")
public class LoginPreviewResource
{
    private final FormWebUiAuthenticationFilter formWebUiAuthenticationManager;

    @Inject
    public LoginPreviewResource(FormWebUiAuthenticationFilter formWebUiAuthenticationManager)
    {
        this.formWebUiAuthenticationManager = requireNonNull(formWebUiAuthenticationManager, "formWebUiAuthenticationManager is null");
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path(UI_PREVIEW_AUTH_INFO)
    @Produces(APPLICATION_JSON)
    public AuthInfo getAuthInfo(ContainerRequestContext request, @Context SecurityContext securityContext)
    {
        boolean isPasswordAllowed = formWebUiAuthenticationManager.isPasswordAllowed(securityContext.isSecure());
        Optional<String> username = formWebUiAuthenticationManager.getAuthenticatedUsername(request);
        return new AuthInfo("form", isPasswordAllowed, username.isPresent(), username);
    }

    @ResourceSecurity(WEB_UI)
    @POST
    @Path(UI_PREVIEW_LOGIN)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public Response login(LoginForm loginForm, @Context SecurityContext securityContext)
    {
        final String username = emptyToNull(loginForm.username);
        final String password = emptyToNull(loginForm.password);

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

    @ResourceSecurity(WEB_UI)
    @GET
    @Path(UI_PREVIEW_LOGOUT)
    @Produces(APPLICATION_JSON)
    public Response logout(@Context HttpHeaders httpHeaders, @Context SecurityContext securityContext)
    {
        return Response.noContent()
                .cookie(getDeleteCookies(httpHeaders.getCookies(), securityContext.isSecure()))
                .build();
    }

    public record LoginForm(String username, String password)
    {
        @Override
        @JsonProperty
        public String username()
        {
            return username;
        }

        @Override
        @JsonProperty
        public String password()
        {
            return password;
        }
    }
}
