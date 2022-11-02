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

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.Key;
import java.security.SecureRandom;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class FormWebUiAuthenticationFilter
        implements WebUiAuthenticationFilter
{
    private static final String TRINO_UI_AUDIENCE = "trino-ui";
    private static final String TRINO_UI_COOKIE = "Trino-UI-Token";
    static final String TRINO_FORM_LOGIN = "Trino-Form-Login";
    static final String LOGIN_FORM = "/ui/login.html";
    static final URI LOGIN_FORM_URI = URI.create(LOGIN_FORM);
    static final String DISABLED_LOCATION = "/ui/disabled.html";
    static final URI DISABLED_LOCATION_URI = URI.create(DISABLED_LOCATION);
    public static final String UI_LOCATION = "/ui/";
    static final URI UI_LOCATION_URI = URI.create(UI_LOCATION);
    static final String UI_LOGIN = "/ui/login";
    static final String UI_LOGOUT = "/ui/logout";

    private final JwtParser jwtParser;
    private final Function<String, String> jwtGenerator;
    private final FormAuthenticator formAuthenticator;
    private final Optional<Authenticator> authenticator;

    @Inject
    public FormWebUiAuthenticationFilter(
            FormWebUiConfig config,
            FormAuthenticator formAuthenticator,
            @ForWebUi Optional<Authenticator> authenticator)
    {
        byte[] hmacBytes;
        if (config.getSharedSecret().isPresent()) {
            hmacBytes = Hashing.sha256().hashString(config.getSharedSecret().get(), UTF_8).asBytes();
        }
        else {
            hmacBytes = new byte[32];
            new SecureRandom().nextBytes(hmacBytes);
        }
        Key hmac = hmacShaKeyFor(hmacBytes);

        this.jwtParser = newJwtParserBuilder()
                .setSigningKey(hmac)
                .requireAudience(TRINO_UI_AUDIENCE)
                .build();

        long sessionTimeoutNanos = config.getSessionTimeout().roundTo(NANOSECONDS);
        this.jwtGenerator = username -> generateJwt(hmac, username, sessionTimeoutNanos);

        this.formAuthenticator = requireNonNull(formAuthenticator, "formAuthenticator is null");
        this.authenticator = requireNonNull(authenticator, "authenticator is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        String path = request.getUriInfo().getRequestUri().getPath();

        // disabled page is always visible
        if (path.equals(DISABLED_LOCATION)) {
            return;
        }

        // authenticator over a secure connection bypasses the form login
        if (authenticator.isPresent() && request.getSecurityContext().isSecure()) {
            handleProtocolLoginRequest(authenticator.get(), request);
            return;
        }

        // login and logout resource is not visible to protocol authenticators
        if ((path.equals(UI_LOGIN) && request.getMethod().equals("POST")) || path.equals(UI_LOGOUT)) {
            return;
        }

        // check if the user is already authenticated
        Optional<String> username = getAuthenticatedUsername(request);
        if (username.isPresent()) {
            // if the authenticated user is requesting the login page, send them directly to the ui
            if (path.equals(LOGIN_FORM)) {
                request.abortWith(redirectFromSuccessfulLoginResponse(request.getUriInfo().getRequestUri().getQuery()).build());
                return;
            }
            setAuthenticatedIdentity(request, username.get());
            return;
        }

        // send 401 to REST api calls and redirect to others
        if (path.startsWith("/ui/api/")) {
            sendWwwAuthenticate(request, "Unauthorized", ImmutableSet.of(TRINO_FORM_LOGIN));
            return;
        }

        if (!isAuthenticationEnabled(request.getSecurityContext().isSecure())) {
            request.abortWith(Response.seeOther(DISABLED_LOCATION_URI).build());
            return;
        }

        if (path.equals(LOGIN_FORM)) {
            return;
        }

        // redirect to login page
        request.abortWith(Response.seeOther(LOGIN_FORM_URI).build());

        request.abortWith(Response.seeOther(buildLoginFormURI(request.getUriInfo())).build());
    }

    private static URI buildLoginFormURI(UriInfo uriInfo)
    {
        UriBuilder builder = uriInfo.getRequestUriBuilder()
                .uri(LOGIN_FORM_URI);

        String path = uriInfo.getRequestUri().getPath();
        if (!isNullOrEmpty(uriInfo.getRequestUri().getQuery())) {
            path += "?" + uriInfo.getRequestUri().getQuery();
        }

        if (path.equals("/ui") || path.equals("/ui/")) {
            return builder.build();
        }

        // this is a hack - the replaceQuery method encodes the value where the uri method just copies the value
        try {
            builder.uri(new URI(null, null, null, path, null));
        }
        catch (URISyntaxException ignored) {
        }

        return builder.build();
    }

    private static void handleProtocolLoginRequest(Authenticator authenticator, ContainerRequestContext request)
    {
        Identity authenticatedIdentity;
        try {
            authenticatedIdentity = authenticator.authenticate(request);
        }
        catch (AuthenticationException e) {
            // authentication failed
            sendWwwAuthenticate(
                    request,
                    firstNonNull(e.getMessage(), "Unauthorized"),
                    e.getAuthenticateHeader().map(ImmutableSet::of).orElse(ImmutableSet.of()));
            return;
        }

        if (redirectFormLoginToUi(request)) {
            return;
        }

        setAuthenticatedIdentity(request, authenticatedIdentity);
    }

    private static boolean redirectFormLoginToUi(ContainerRequestContext request)
    {
        // these paths should never be used with a protocol login, but the user might have this cached or linked, so redirect back to the main UI page.
        String path = request.getUriInfo().getRequestUri().getPath();
        if (path.equals(LOGIN_FORM) || path.equals(UI_LOGIN) || path.equals(UI_LOGOUT)) {
            request.abortWith(Response.seeOther(UI_LOCATION_URI).build());
            return true;
        }
        return false;
    }

    public static ResponseBuilder redirectFromSuccessfulLoginResponse(String redirectPath)
    {
        URI redirectLocation = UI_LOCATION_URI;

        redirectPath = emptyToNull(redirectPath);
        if (redirectPath != null) {
            try {
                redirectLocation = new URI(redirectPath);
            }
            catch (URISyntaxException ignored) {
            }
        }

        return Response.seeOther(redirectLocation);
    }

    public Optional<NewCookie> checkLoginCredentials(String username, String password, boolean secure)
    {
        return formAuthenticator.isValidCredential(username, password, secure)
                .map(user -> createAuthenticationCookie(user, secure));
    }

    private Optional<String> getAuthenticatedUsername(ContainerRequestContext request)
    {
        Cookie cookie = request.getCookies().get(TRINO_UI_COOKIE);
        if (cookie == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(parseJwt(cookie.getValue()));
        }
        catch (JwtException e) {
            return Optional.empty();
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    private NewCookie createAuthenticationCookie(String userName, boolean secure)
    {
        String jwt = jwtGenerator.apply(userName);
        return new NewCookie(
                TRINO_UI_COOKIE,
                jwt,
                "/ui",
                null,
                Cookie.DEFAULT_VERSION,
                null,
                NewCookie.DEFAULT_MAX_AGE,
                null,
                secure,
                true);
    }

    public static NewCookie getDeleteCookie(boolean secure)
    {
        return new NewCookie(
                TRINO_UI_COOKIE,
                "delete",
                "/ui",
                null,
                Cookie.DEFAULT_VERSION,
                null,
                0,
                null,
                secure,
                true);
    }

    public boolean isPasswordAllowed(boolean secure)
    {
        return formAuthenticator.isPasswordAllowed(secure);
    }

    boolean isAuthenticationEnabled(boolean secure)
    {
        return formAuthenticator.isLoginEnabled(secure) || authenticator.isPresent();
    }

    private static String generateJwt(Key hmac, String username, long sessionTimeoutNanos)
    {
        return newJwtBuilder()
                .signWith(hmac)
                .setSubject(username)
                .setExpiration(Date.from(ZonedDateTime.now().plusNanos(sessionTimeoutNanos).toInstant()))
                .setAudience(TRINO_UI_AUDIENCE)
                .compact();
    }

    private String parseJwt(String jwt)
    {
        return jwtParser
                .parseClaimsJws(jwt)
                .getBody()
                .getSubject();
    }

    public static boolean redirectAllFormLoginToUi(ContainerRequestContext request)
    {
        // these paths should never be used with a protocol login, but the user might have this cached or linked, so redirect back ot the main UI page.
        String path = request.getUriInfo().getRequestUri().getPath();
        if (path.equals(LOGIN_FORM) || path.equals(UI_LOGIN) || path.equals(UI_LOGOUT)) {
            request.abortWith(Response.seeOther(UI_LOCATION_URI).build());
            return true;
        }
        return false;
    }
}
