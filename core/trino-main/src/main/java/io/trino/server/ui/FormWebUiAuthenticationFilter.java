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
import com.google.inject.Inject;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.trino.server.ExternalUriInfo;
import io.trino.server.ExternalUriInfo.ExternalUriBuilder;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;

import javax.crypto.SecretKey;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.Key;
import java.security.SecureRandom;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
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
    static final String DISABLED_LOCATION = "/ui/disabled.html";
    public static final String UI_LOCATION = "/ui/";
    static final String UI_LOGIN = "/ui/login";
    static final String UI_LOGOUT = "/ui/logout";

    static final String UI_PREVIEW_BASE = "/ui/preview/";

    static final String UI_PREVIEW_AUTH_INFO = UI_PREVIEW_BASE + "auth/info";
    static final String UI_PREVIEW_LOGIN_FORM = UI_PREVIEW_BASE + "auth/login";
    static final String UI_PREVIEW_LOGOUT = UI_PREVIEW_BASE + "auth/logout";

    private final JwtParser jwtParser;
    private final Function<String, String> jwtGenerator;
    private final FormAuthenticator formAuthenticator;
    private final Optional<Authenticator> authenticator;

    private static final MultipartUiCookie MULTIPART_COOKIE = new MultipartUiCookie(TRINO_UI_COOKIE, "/ui");
    private final boolean previewEnabled;

    @Inject
    public FormWebUiAuthenticationFilter(
            FormWebUiConfig config,
            FormAuthenticator formAuthenticator,
            @ForWebUi Optional<Authenticator> authenticator,
            WebUiConfig webUiConfig)
    {
        byte[] hmacBytes;
        if (config.getSharedSecret().isPresent()) {
            hmacBytes = Hashing.sha256().hashString(config.getSharedSecret().get(), UTF_8).asBytes();
        }
        else {
            hmacBytes = new byte[32];
            new SecureRandom().nextBytes(hmacBytes);
        }
        SecretKey hmac = hmacShaKeyFor(hmacBytes);

        this.jwtParser = newJwtParserBuilder()
                .verifyWith(hmac)
                .requireAudience(TRINO_UI_AUDIENCE)
                .build();

        long sessionTimeoutNanos = config.getSessionTimeout().roundTo(NANOSECONDS);
        this.jwtGenerator = username -> generateJwt(hmac, username, sessionTimeoutNanos);

        this.formAuthenticator = requireNonNull(formAuthenticator, "formAuthenticator is null");
        this.authenticator = requireNonNull(authenticator, "authenticator is null");
        this.previewEnabled = requireNonNull(webUiConfig, "webUiConfig is null").isPreviewEnabled();
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
        if (isLoginResource(path, request.getMethod())) {
            return;
        }

        // check if the user is already authenticated
        Optional<String> username = getAuthenticatedUsername(request);
        if (username.isPresent()) {
            // if the authenticated user is requesting the login page, send them directly to the ui
            if (path.equals(LOGIN_FORM)) {
                request.abortWith(redirectFromSuccessfulLoginResponse(ExternalUriInfo.from(request), request.getUriInfo().getRequestUri().getQuery()).build());
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
            request.abortWith(Response.seeOther(ExternalUriInfo.from(request).absolutePath(DISABLED_LOCATION)).build());
            return;
        }

        if (path.equals(LOGIN_FORM)) {
            return;
        }

        if (previewEnabled && path.equals(UI_PREVIEW_BASE)) {
            return;
        }

        // redirect to login page
        request.abortWith(Response.seeOther(buildLoginFormURI(request)).build());
    }

    private static URI buildLoginFormURI(ContainerRequestContext request)
    {
        ExternalUriBuilder builder = ExternalUriInfo.from(request).baseUriBuilder()
                .path(LOGIN_FORM);

        URI requestUri = request.getUriInfo().getRequestUri();
        String path = requestUri.getPath();
        if (!isNullOrEmpty(requestUri.getQuery())) {
            path += "?" + requestUri.getQuery();
        }

        if (path.equals("/ui") || path.equals("/ui/")) {
            return builder.build();
        }

        builder.rawReplaceQuery(path);

        return builder.build();
    }

    private boolean isLoginResource(String path, String method)
    {
        if (path.equals(UI_LOGIN) && method.equals("POST")) {
            return true;
        }

        if (path.equals(UI_LOGOUT)) {
            return true;
        }

        if (!previewEnabled) {
            return false;
        }

        if (path.equals(UI_PREVIEW_LOGIN_FORM) && method.equals("POST")) {
            return true;
        }

        if (path.equals(UI_PREVIEW_LOGOUT)) {
            return true;
        }

        return path.equals(UI_PREVIEW_AUTH_INFO) && method.equals("GET");
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
            request.abortWith(Response.seeOther(ExternalUriInfo.from(request).absolutePath(UI_LOCATION)).build());
            return true;
        }
        return false;
    }

    public static ResponseBuilder redirectFromSuccessfulLoginResponse(ExternalUriInfo externalUriInfo, String redirectPath)
    {
        if (!isNullOrEmpty(redirectPath)) {
            try {
                URI redirectLocation = new URI(redirectPath);
                return Response.seeOther(externalUriInfo.baseUriBuilder()
                        .path(redirectLocation.getPath())
                        .rawReplaceQuery(redirectLocation.getRawQuery())
                        .build());
            }
            catch (URISyntaxException _) {
            }
        }

        return Response.seeOther(externalUriInfo.absolutePath(UI_LOCATION));
    }

    public Optional<NewCookie[]> checkLoginCredentials(String username, String password, boolean secure)
    {
        return formAuthenticator.isValidCredential(username, password, secure)
                .map(user -> createAuthenticationCookie(user, secure));
    }

    Optional<String> getAuthenticatedUsername(ContainerRequestContext request)
    {
        try {
            return MULTIPART_COOKIE.read(request.getCookies()).map(this::parseJwt);
        }
        catch (JwtException e) {
            return Optional.empty();
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    private NewCookie[] createAuthenticationCookie(String userName, boolean secure)
    {
        return MULTIPART_COOKIE.create(jwtGenerator.apply(userName), null, secure);
    }

    public static NewCookie[] getDeleteCookies(Map<String, Cookie> existingCookies, boolean isSecure)
    {
        return MULTIPART_COOKIE.delete(existingCookies, isSecure);
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
                .subject(username)
                .expiration(Date.from(ZonedDateTime.now().plusNanos(sessionTimeoutNanos).toInstant()))
                .audience().add(TRINO_UI_AUDIENCE).and()
                .compact();
    }

    private String parseJwt(String jwt)
    {
        return jwtParser
                .parseSignedClaims(jwt)
                .getPayload()
                .getSubject();
    }

    public static boolean redirectAllFormLoginToUi(ContainerRequestContext request)
    {
        // these paths should never be used with a protocol login, but the user might have this cached or linked, so redirect back ot the main UI page.
        String path = request.getUriInfo().getRequestUri().getPath();
        if (path.equals(LOGIN_FORM) || path.equals(UI_LOGIN) || path.equals(UI_LOGOUT)) {
            request.abortWith(Response.seeOther(ExternalUriInfo.from(request).absolutePath(UI_LOCATION)).build());
            return true;
        }
        return false;
    }
}
