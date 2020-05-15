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
package io.prestosql.server;

import io.prestosql.spi.security.Identity;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.Principal;
import java.util.Optional;

import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static java.util.Objects.requireNonNull;

public final class ServletSecurityUtils
{
    private ServletSecurityUtils() {}

    public static void sendErrorMessage(HttpServletResponse response, int errorCode, String errorMessage)
            throws IOException
    {
        // Clients should use the response body rather than the HTTP status
        // message (which does not exist with HTTP/2), but the status message
        // still needs to be sent for compatibility with existing clients.
        response.setStatus(errorCode, errorMessage);
        response.setContentType(PLAIN_TEXT_UTF_8.toString());
        try (PrintWriter writer = response.getWriter()) {
            writer.write(errorMessage);
        }
    }

    public static void withAuthenticatedIdentity(FilterChain nextFilter, HttpServletRequest request, HttpServletResponse response, Identity authenticatedIdentity)
            throws IOException, ServletException
    {
        request.setAttribute(AUTHENTICATED_IDENTITY, authenticatedIdentity);
        try {
            nextFilter.doFilter(withPrincipal(request, authenticatedIdentity.getPrincipal()), response);
        }
        finally {
            // destroy identity if identity is still attached to the request
            Optional.ofNullable(request.getAttribute(AUTHENTICATED_IDENTITY))
                    .map(Identity.class::cast)
                    .ifPresent(Identity::destroy);
        }
    }

    public static ServletRequest withPrincipal(HttpServletRequest request, Optional<Principal> principal)
    {
        requireNonNull(principal, "principal is null");
        if (principal.isEmpty()) {
            return request;
        }
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return principal.get();
            }
        };
    }

    public static void skipRequestBody(HttpServletRequest request)
            throws IOException
    {
        // If we send the challenge without consuming the body of the request,
        // the server will close the connection after sending the response.
        // The client may interpret this as a failed request and not resend the
        // request with the authentication header. We can avoid this behavior
        // in the client by reading and discarding the entire body of the
        // unauthenticated request before sending the response.
        try (InputStream inputStream = request.getInputStream()) {
            copy(inputStream, nullOutputStream());
        }
    }
}
