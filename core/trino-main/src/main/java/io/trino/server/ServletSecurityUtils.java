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
package io.trino.server;

import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import java.security.Principal;
import java.util.Collection;

import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static io.trino.server.HttpRequestSessionContextFactory.AUTHENTICATED_IDENTITY;
import static javax.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

public final class ServletSecurityUtils
{
    private ServletSecurityUtils() {}

    public static void sendErrorMessage(ContainerRequestContext request, Status errorCode, String errorMessage)
    {
        request.abortWith(errorResponse(errorCode, errorMessage).build());
    }

    public static void sendWwwAuthenticate(ContainerRequestContext request, String errorMessage, Collection<String> authenticateHeaders)
    {
        request.abortWith(authenticateResponse(errorMessage, authenticateHeaders).build());
    }

    private static ResponseBuilder authenticateResponse(String errorMessage, Collection<String> authenticateHeaders)
    {
        ResponseBuilder response = errorResponse(UNAUTHORIZED, errorMessage);
        for (String value : authenticateHeaders) {
            response.header(WWW_AUTHENTICATE, value);
        }
        return response;
    }

    private static ResponseBuilder errorResponse(Status errorCode, String errorMessage)
    {
        // Clients should use the response body rather than the HTTP status
        // message (which does not exist with HTTP/2), but the status message
        // still needs to be sent for compatibility with existing clients.
        return Response.status(errorCode.getStatusCode(), errorMessage)
                .type(PLAIN_TEXT_UTF_8.toString())
                .entity(errorMessage);
    }

    public static void setAuthenticatedIdentity(ContainerRequestContext request, String username)
    {
        setAuthenticatedIdentity(request, Identity.forUser(username)
                .withPrincipal(new BasicPrincipal(username))
                .build());
    }

    public static void setAuthenticatedIdentity(ContainerRequestContext request, Identity authenticatedIdentity)
    {
        request.setProperty(AUTHENTICATED_IDENTITY, authenticatedIdentity);

        boolean secure = request.getSecurityContext().isSecure();
        Principal principal = authenticatedIdentity.getPrincipal().orElse(null);
        request.setSecurityContext(new SecurityContext()
        {
            @Override
            public Principal getUserPrincipal()
            {
                return principal;
            }

            @Override
            public boolean isUserInRole(String role)
            {
                return false;
            }

            @Override
            public boolean isSecure()
            {
                return secure;
            }

            @Override
            public String getAuthenticationScheme()
            {
                return "trino";
            }
        });
    }
}
