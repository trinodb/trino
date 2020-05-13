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

import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import io.airlift.http.client.HttpUriBuilder;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prestosql.server.ServletSecurityUtils;
import io.prestosql.server.security.AuthenticationException;
import io.prestosql.server.security.Authenticator;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.security.SecureRandom;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilder;
import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static javax.servlet.http.HttpServletResponse.SC_SEE_OTHER;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class FormWebUiAuthenticationManager
        implements WebUiAuthenticationManager
{
    private static final String PRESTO_UI_AUDIENCE = "presto-ui";
    private static final String PRESTO_UI_COOKIE = "Presto-UI-Token";
    private static final String LOGIN_FORM = "/ui/login.html";
    private static final String DISABLED_LOCATION = "/ui/disabled.html";
    private static final String UI_LOCATION = "/ui/";

    private final Function<String, String> jwtParser;
    private final Function<String, String> jwtGenerator;
    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final Optional<Authenticator> authenticator;

    @Inject
    public FormWebUiAuthenticationManager(
            FormWebUiConfig config,
            PasswordAuthenticatorManager passwordAuthenticatorManager,
            @ForWebUi Optional<Authenticator> authenticator)
    {
        byte[] hmac;
        if (config.getSharedSecret().isPresent()) {
            hmac = Hashing.sha256().hashString(config.getSharedSecret().get(), UTF_8).asBytes();
        }
        else {
            hmac = new byte[32];
            new SecureRandom().nextBytes(hmac);
        }

        this.jwtParser = jwt -> parseJwt(hmac, jwt);

        long sessionTimeoutNanos = config.getSessionTimeout().roundTo(NANOSECONDS);
        this.jwtGenerator = username -> generateJwt(hmac, username, sessionTimeoutNanos);

        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        this.authenticator = requireNonNull(authenticator, "authenticator is null");
    }

    @Override
    public void handleUiRequest(HttpServletRequest request, HttpServletResponse response, FilterChain nextFilter)
            throws IOException, ServletException
    {
        if (request.getPathInfo() == null || request.getPathInfo().equals("/")) {
            sendRedirect(response, getUiLocation(request));
            return;
        }

        if (isPublic(request)) {
            nextFilter.doFilter(request, response);
            return;
        }

        // authenticator over a secure connection bypasses the form login
        if (authenticator.isPresent() && request.isSecure()) {
            handleProtocolLoginRequest(authenticator.get(), request, response, nextFilter);
            return;
        }

        if (request.getPathInfo().equals("/ui/login")) {
            handleFormLoginRequest(request, response);
            return;
        }
        if (request.getPathInfo().equals("/ui/logout")) {
            handleLogoutRequest(request, response);
            return;
        }

        Optional<String> username = getAuthenticatedUsername(request);
        if (username.isPresent()) {
            if (request.getPathInfo().equals(LOGIN_FORM)) {
                sendRedirectFromSuccessfulLogin(request, response, request.getQueryString());
                return;
            }
            nextFilter.doFilter(withUsername(request, username.get()), response);
            return;
        }

        // clear authentication cookie if present
        getAuthenticationCookie(request)
                .ifPresent(ignored -> response.addCookie(getDeleteCookie(request)));

        // drain the input
        try (InputStream inputStream = request.getInputStream()) {
            ByteStreams.exhaust(inputStream);
        }

        // send 401 to REST api calls and redirect to others
        if (request.getPathInfo().startsWith("/ui/api/")) {
            response.setHeader(WWW_AUTHENTICATE, "Presto-Form-Login");
            response.setStatus(SC_UNAUTHORIZED);
            return;
        }

        if (!isAuthenticationEnabled(request)) {
            sendRedirect(response, getRedirectLocation(request, DISABLED_LOCATION));
            return;
        }

        if (request.getPathInfo().equals(LOGIN_FORM)) {
            nextFilter.doFilter(request, response);
            return;
        }

        // redirect to login page
        sendRedirect(response, getRedirectLocation(request, LOGIN_FORM, encodeCurrentLocationForLoginRedirect(request)));
    }

    private static String encodeCurrentLocationForLoginRedirect(HttpServletRequest request)
    {
        String path = request.getPathInfo();
        if (!isNullOrEmpty(request.getQueryString())) {
            path += "?" + request.getQueryString();
        }
        if (path.equals("/ui") || path.equals("/ui/")) {
            return null;
        }
        return path;
    }

    private static void handleProtocolLoginRequest(Authenticator authenticator, HttpServletRequest request, HttpServletResponse response, FilterChain nextFilter)
            throws IOException, ServletException
    {
        Identity authenticatedIdentity;
        try {
            authenticatedIdentity = authenticator.authenticate(request);
        }
        catch (AuthenticationException e) {
            // authentication failed
            ServletSecurityUtils.skipRequestBody(request);

            e.getAuthenticateHeader().ifPresent(value -> response.addHeader(WWW_AUTHENTICATE, value));

            ServletSecurityUtils.sendErrorMessage(response, SC_UNAUTHORIZED, firstNonNull(e.getMessage(), "Unauthorized"));
            return;
        }

        if (redirectFormLoginToUi(request, response)) {
            return;
        }

        ServletSecurityUtils.withAuthenticatedIdentity(nextFilter, request, response, authenticatedIdentity);
    }

    public static boolean redirectFormLoginToUi(HttpServletRequest request, HttpServletResponse response)
    {
        // these paths should never be used with a protocol login, but the user might have this cached or linked, so redirect back to the main UI page.
        if (request.getPathInfo().equals(LOGIN_FORM) || request.getPathInfo().equals("/ui/login") || request.getPathInfo().equals("/ui/logout")) {
            sendRedirect(response, getRedirectLocation(request, UI_LOCATION));
            return true;
        }
        return false;
    }

    private void handleFormLoginRequest(HttpServletRequest request, HttpServletResponse response)
    {
        if (!isAuthenticationEnabled(request)) {
            sendRedirect(response, getRedirectLocation(request, DISABLED_LOCATION));
            return;
        }
        Optional<String> username = checkLoginCredentials(request);
        if (username.isPresent()) {
            response.addCookie(createAuthenticationCookie(request, username.get()));
            sendRedirectFromSuccessfulLogin(request, response, request.getParameter("redirectPath"));
            return;
        }
        sendRedirect(response, getLoginFormLocation(request));
    }

    private static void sendRedirectFromSuccessfulLogin(HttpServletRequest request, HttpServletResponse response, String redirectPath)
    {
        try {
            URI redirectUri = new URI(firstNonNull(emptyToNull(redirectPath), UI_LOCATION));
            sendRedirect(response, getRedirectLocation(request, redirectUri.getPath(), redirectUri.getQuery()));
        }
        catch (URISyntaxException ignored) {
            sendRedirect(response, UI_LOCATION);
        }
    }

    private Optional<String> checkLoginCredentials(HttpServletRequest request)
    {
        String username = emptyToNull(request.getParameter("username"));
        if (username == null) {
            return Optional.empty();
        }

        if (!request.isSecure()) {
            return Optional.of(username);
        }

        String password = emptyToNull(request.getParameter("password"));
        try {
            passwordAuthenticatorManager.getAuthenticator().createAuthenticatedPrincipal(username, password);
            return Optional.of(username);
        }
        catch (AccessDeniedException e) {
            return Optional.empty();
        }
    }

    private void handleLogoutRequest(HttpServletRequest request, HttpServletResponse response)
    {
        response.addCookie(getDeleteCookie(request));
        if (isAuthenticationEnabled(request)) {
            sendRedirect(response, getLoginFormLocation(request));
            return;
        }
        sendRedirect(response, getRedirectLocation(request, DISABLED_LOCATION));
    }

    private Optional<String> getAuthenticatedUsername(HttpServletRequest request)
    {
        Optional<Cookie> cookie = getAuthenticationCookie(request);
        if (cookie.isPresent()) {
            try {
                return Optional.of(jwtParser.apply(cookie.get().getValue()));
            }
            catch (JwtException e) {
                return Optional.empty();
            }
            catch (RuntimeException e) {
                throw new RuntimeException("Authentication error", e);
            }
        }
        return Optional.empty();
    }

    private static ServletRequest withUsername(HttpServletRequest request, String username)
    {
        requireNonNull(username, "username is null");
        BasicPrincipal principal = new BasicPrincipal(username);
        request.setAttribute(AUTHENTICATED_IDENTITY, Identity.forUser(username)
                .withPrincipal(principal)
                .build());
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return principal;
            }
        };
    }

    private Cookie createAuthenticationCookie(HttpServletRequest request, String userName)
    {
        String jwt = jwtGenerator.apply(userName);
        Cookie cookie = new Cookie(PRESTO_UI_COOKIE, jwt);
        cookie.setSecure(request.isSecure());
        cookie.setHttpOnly(true);
        cookie.setPath("/ui");
        return cookie;
    }

    private Cookie getDeleteCookie(HttpServletRequest request)
    {
        Cookie cookie = new Cookie(PRESTO_UI_COOKIE, "delete");
        cookie.setMaxAge(0);
        cookie.setSecure(request.isSecure());
        cookie.setHttpOnly(true);
        return cookie;
    }

    private static Optional<Cookie> getAuthenticationCookie(HttpServletRequest request)
    {
        return stream(firstNonNull(request.getCookies(), new Cookie[0]))
                .filter(cookie -> cookie.getName().equals(PRESTO_UI_COOKIE))
                .findFirst();
    }

    private static boolean isPublic(HttpServletRequest request)
    {
        // note login page is handled later
        String pathInfo = request.getPathInfo();
        return pathInfo.equals(DISABLED_LOCATION) ||
                pathInfo.startsWith("/ui/vendor") ||
                pathInfo.startsWith("/ui/assets");
    }

    private static void sendRedirect(HttpServletResponse response, String location)
    {
        response.setHeader(LOCATION, location);
        response.setStatus(SC_SEE_OTHER);
    }

    private static String getLoginFormLocation(HttpServletRequest request)
    {
        return getRedirectLocation(request, LOGIN_FORM);
    }

    private static String getUiLocation(HttpServletRequest request)
    {
        return getRedirectLocation(request, UI_LOCATION);
    }

    private static String getRedirectLocation(HttpServletRequest request, String path)
    {
        return getRedirectLocation(request, path, null);
    }

    static String getRedirectLocation(HttpServletRequest request, String path, String queryParameter)
    {
        HttpUriBuilder builder = uriBuilder()
                .scheme(request.getScheme())
                .host(request.getServerName())
                .port(request.getServerPort())
                .replacePath(path);
        if (queryParameter != null) {
            builder.addParameter(queryParameter);
        }
        return builder.toString();
    }

    private boolean isAuthenticationEnabled(HttpServletRequest request)
    {
        // unsecured requests support username-only authentication (no password)
        // secured requests require a password authenticator or a protocol level authenticator
        return !request.isSecure() || passwordAuthenticatorManager.isLoaded() || authenticator.isPresent();
    }

    private static String generateJwt(byte[] hmac, String username, long sessionTimeoutNanos)
    {
        return Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, hmac)
                .setSubject(username)
                .setExpiration(Date.from(ZonedDateTime.now().plusNanos(sessionTimeoutNanos).toInstant()))
                .setAudience(PRESTO_UI_AUDIENCE)
                .compact();
    }

    private static String parseJwt(byte[] hmac, String jwt)
    {
        return Jwts.parser()
                .setSigningKey(hmac)
                .requireAudience(PRESTO_UI_AUDIENCE)
                .parseClaimsJws(jwt)
                .getBody()
                .getSubject();
    }

    public static boolean redirectAllFormLoginToUi(HttpServletRequest request, HttpServletResponse response)
    {
        // these paths should never be used with a protocol login, but the user might have this cached or linked, so redirect back ot the main UI page.
        if (request.getPathInfo().equals(LOGIN_FORM) || request.getPathInfo().equals("/ui/login") || request.getPathInfo().equals("/ui/logout")) {
            sendRedirect(response, getRedirectLocation(request, UI_LOCATION));
            return true;
        }
        return false;
    }
}
