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
package io.prestosql.server.security;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import io.prestosql.server.InternalAuthenticationManager;
import io.prestosql.server.ui.WebUiAuthenticationManager;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static io.prestosql.server.security.BasicAuthCredentials.extractBasicAuthCredentials;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class AuthenticationFilter
        implements Filter
{
    private static final String HTTPS_PROTOCOL = "https";

    private final List<Authenticator> authenticators;
    private final boolean httpsForwardingEnabled;
    private final InternalAuthenticationManager internalAuthenticationManager;
    private final WebUiAuthenticationManager uiAuthenticationManager;

    @Inject
    public AuthenticationFilter(List<Authenticator> authenticators,
            SecurityConfig securityConfig,
            InternalAuthenticationManager internalAuthenticationManager,
            WebUiAuthenticationManager uiAuthenticationManager)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        this.httpsForwardingEnabled = requireNonNull(securityConfig, "securityConfig is null").getEnableForwardingHttps();
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        this.uiAuthenticationManager = requireNonNull(uiAuthenticationManager, "uiAuthenticationManager is null");
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (internalAuthenticationManager.isInternalRequest(request)) {
            Principal principal = internalAuthenticationManager.authenticateInternalRequest(request);
            if (principal == null) {
                response.setStatus(SC_UNAUTHORIZED);
                response.setContentType(PLAIN_TEXT_UTF_8.toString());
                return;
            }
            Identity identity = Identity.forUser("<internal>")
                    .withPrincipal(principal)
                    .build();
            withAuthenticatedIdentity(nextFilter, request, response, identity);
            return;
        }

        if (WebUiAuthenticationManager.isUiRequest(request)) {
            uiAuthenticationManager.handleUiRequest(request, response, nextFilter);
            return;
        }

        // skip authentication if non-secure or not configured
        if (!doesRequestSupportAuthentication(request)) {
            handleInsecureRequest(nextFilter, request, response);
            return;
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            Identity authenticatedIdentity;
            try {
                authenticatedIdentity = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                if (e.getMessage() != null) {
                    messages.add(e.getMessage());
                }
                e.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                continue;
            }

            // authentication succeeded
            withAuthenticatedIdentity(nextFilter, request, response, authenticatedIdentity);
            return;
        }

        // authentication failed
        skipRequestBody(request);

        for (String value : authenticateHeaders) {
            response.addHeader(WWW_AUTHENTICATE, value);
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }

        // The error string is used by clients for exception messages and
        // is presented to the end user, thus it should be a single line.
        String error = Joiner.on(" | ").join(messages);
        sendErrorMessage(response, SC_UNAUTHORIZED, error);
    }

    private static void sendErrorMessage(HttpServletResponse response, int errorCode, String errorMessage)
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

    private static void handleInsecureRequest(FilterChain nextFilter, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException
    {
        Optional<BasicAuthCredentials> basicAuthCredentials;
        try {
            basicAuthCredentials = extractBasicAuthCredentials(request);
        }
        catch (AuthenticationException e) {
            sendErrorMessage(response, SC_FORBIDDEN, e.getMessage());
            return;
        }

        if (!basicAuthCredentials.isPresent()) {
            nextFilter.doFilter(request, response);
            return;
        }

        if (basicAuthCredentials.get().getPassword().isPresent()) {
            sendErrorMessage(response, SC_FORBIDDEN, "Password not allowed for insecure request");
            return;
        }

        withAuthenticatedIdentity(nextFilter, request, response, Identity.ofUser(basicAuthCredentials.get().getUser()));
    }

    private boolean doesRequestSupportAuthentication(HttpServletRequest request)
    {
        if (authenticators.isEmpty()) {
            return false;
        }
        if (request.isSecure()) {
            return true;
        }
        return httpsForwardingEnabled && Strings.nullToEmpty(request.getHeader(HttpHeaders.X_FORWARDED_PROTO)).equalsIgnoreCase(HTTPS_PROTOCOL);
    }

    private static void withAuthenticatedIdentity(FilterChain nextFilter, HttpServletRequest request, HttpServletResponse response, Identity authenticatedIdentity)
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

    private static ServletRequest withPrincipal(HttpServletRequest request, Optional<Principal> principal)
    {
        requireNonNull(principal, "principal is null");
        if (!principal.isPresent()) {
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

    private static void skipRequestBody(HttpServletRequest request)
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
