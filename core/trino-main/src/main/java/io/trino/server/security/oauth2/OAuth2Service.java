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
package io.trino.server.security.oauth2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.JsonResponseHandler;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.impl.DefaultClaims;
import io.trino.server.security.oauth2.OAuth2Client.OAuth2Response;
import io.trino.server.ui.OAuth2WebUiInstalled;
import io.trino.server.ui.OAuthWebUiCookie;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.security.Key;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.jsonwebtoken.Claims.AUDIENCE;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.HttpMethod.POST;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

public class OAuth2Service
{
    private static final Logger LOG = Logger.get(OAuth2Service.class);

    public static final String REDIRECT_URI = "redirect_uri";
    public static final String STATE = "state";
    public static final String NONCE = "nonce";
    public static final String OPENID_SCOPE = "openid";

    private static final String STATE_AUDIENCE_UI = "trino_oauth_ui";
    private static final String FAILURE_REPLACEMENT_TEXT = "<!-- ERROR_MESSAGE -->";
    private static final Random SECURE_RANDOM = new SecureRandom();
    public static final String HANDLER_STATE_CLAIM = "handler_state";
    private static final JsonResponseHandler<Map<String, Object>> USERINFO_RESPONSE_HANDLER = createJsonResponseHandler(mapJsonCodec(String.class, Object.class));

    private final OAuth2Client client;
    private final SigningKeyResolver signingKeyResolver;

    private final String successHtml;
    private final String failureHtml;

    private final Set<String> scopes;
    private final TemporalAmount challengeTimeout;
    private final Key stateHmac;
    private final JwtParser jwtParser;

    private final HttpClient httpClient;
    private final String issuer;
    private final String accessTokenIssuer;
    private final String clientId;
    private final Optional<URI> userinfoUri;
    private final Set<String> allowedAudiences;

    private final OAuth2TokenHandler tokenHandler;

    private final boolean webUiOAuthEnabled;

    @Inject
    public OAuth2Service(OAuth2Client client, @ForOAuth2 SigningKeyResolver signingKeyResolver, @ForOAuth2 HttpClient httpClient, OAuth2Config oauth2Config, OAuth2TokenHandler tokenHandler, Optional<OAuth2WebUiInstalled> webUiOAuthEnabled)
            throws IOException
    {
        this.client = requireNonNull(client, "client is null");
        this.signingKeyResolver = requireNonNull(signingKeyResolver, "signingKeyResolver is null");
        requireNonNull(oauth2Config, "oauth2Config is null");

        this.successHtml = Resources.toString(Resources.getResource(getClass(), "/oauth2/success.html"), UTF_8);
        this.failureHtml = Resources.toString(Resources.getResource(getClass(), "/oauth2/failure.html"), UTF_8);
        verify(failureHtml.contains(FAILURE_REPLACEMENT_TEXT), "login.html does not contain the replacement text");

        this.scopes = oauth2Config.getScopes();
        this.challengeTimeout = Duration.ofMillis(oauth2Config.getChallengeTimeout().toMillis());
        this.stateHmac = hmacShaKeyFor(oauth2Config.getStateKey()
                .map(key -> sha256().hashString(key, UTF_8).asBytes())
                .orElseGet(() -> secureRandomBytes(32)));
        this.jwtParser = newJwtParserBuilder()
                .setSigningKey(stateHmac)
                .requireAudience(STATE_AUDIENCE_UI)
                .build();

        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.issuer = oauth2Config.getIssuer();
        this.accessTokenIssuer = oauth2Config.getAccessTokenIssuer().orElse(issuer);
        this.clientId = oauth2Config.getClientId();
        this.userinfoUri = oauth2Config.getUserinfoUrl().map(url -> UriBuilder.fromUri(url).build());
        this.allowedAudiences = ImmutableSet.<String>builder()
                .addAll(oauth2Config.getAdditionalAudiences())
                .add(clientId)
                .build();

        this.tokenHandler = requireNonNull(tokenHandler, "tokenHandler is null");

        this.webUiOAuthEnabled = requireNonNull(webUiOAuthEnabled, "webUiOAuthEnabled is null").isPresent();
    }

    public Response startOAuth2Challenge(UriInfo uriInfo)
    {
        return startOAuth2Challenge(
                uriInfo.getBaseUri().resolve(CALLBACK_ENDPOINT),
                Optional.empty());
    }

    public Response startOAuth2Challenge(UriInfo uriInfo, String handlerState)
    {
        return startOAuth2Challenge(
                uriInfo.getBaseUri().resolve(CALLBACK_ENDPOINT),
                Optional.of(handlerState));
    }

    private Response startOAuth2Challenge(URI callbackUri, Optional<String> handlerState)
    {
        Instant challengeExpiration = now().plus(challengeTimeout);
        String state = newJwtBuilder()
                .signWith(stateHmac)
                .setAudience(STATE_AUDIENCE_UI)
                .claim(HANDLER_STATE_CLAIM, handlerState.orElse(null))
                .setExpiration(Date.from(challengeExpiration))
                .compact();

        Optional<String> nonce;
        // add nonce parameter to the authorization challenge only if `openid` scope is going to be requested
        // since this scope is required in order to obtain the ID token which carriers the nonce back to the server
        if (scopes.contains(OPENID_SCOPE)) {
            nonce = Optional.of(randomNonce());
        }
        else {
            nonce = Optional.empty();
        }
        Response.ResponseBuilder response = Response.seeOther(
                client.getAuthorizationUri(
                        state,
                        callbackUri,
                        nonce.map(OAuth2Service::hashNonce)));
        nonce.ifPresent(nce -> response.cookie(NonceCookie.create(nce, challengeExpiration)));
        return response.build();
    }

    public Response handleOAuth2Error(String state, String error, String errorDescription, String errorUri)
    {
        try {
            Claims stateClaims = parseState(state);
            Optional.ofNullable(stateClaims.get(HANDLER_STATE_CLAIM, String.class))
                    .ifPresent(value ->
                            tokenHandler.setTokenExchangeError(value,
                                    format("Authentication response could not be verified: error=%s, errorDescription=%s, errorUri=%s",
                                            error, errorDescription, errorDescription)));
        }
        catch (ChallengeFailedException | RuntimeException e) {
            LOG.debug(e, "Authentication response could not be verified invalid state: state=%s", state);
            return Response.status(BAD_REQUEST)
                    .entity(getInternalFailureHtml("Authentication response could not be verified"))
                    .cookie(NonceCookie.delete())
                    .build();
        }

        LOG.debug("OAuth server returned an error: error=%s, error_description=%s, error_uri=%s, state=%s", error, errorDescription, errorUri, state);
        return Response.ok()
                .entity(getCallbackErrorHtml(error))
                .cookie(NonceCookie.delete())
                .build();
    }

    public Response finishOAuth2Challenge(String state, String code, URI callbackUri, Optional<String> nonce)
    {
        Optional<String> handlerState;
        try {
            Claims stateClaims = parseState(state);
            handlerState = Optional.ofNullable(stateClaims.get(HANDLER_STATE_CLAIM, String.class));
        }
        catch (ChallengeFailedException | RuntimeException e) {
            LOG.debug(e, "Authentication response could not be verified invalid state: state=%s", state);
            return Response.status(BAD_REQUEST)
                    .entity(getInternalFailureHtml("Authentication response could not be verified"))
                    .cookie(NonceCookie.delete())
                    .build();
        }

        // Note: the Web UI may be disabled, so REST requests can not redirect to a success or error page inside of the Web UI
        try {
            // fetch access token
            OAuth2Response oauth2Response = client.getOAuth2Response(code, callbackUri);
            Claims parsedToken = validateAndParseOAuth2Response(oauth2Response, nonce).orElseThrow(() -> new ChallengeFailedException("invalid access token"));

            // determine expiration
            Instant validUntil = determineExpiration(oauth2Response.getValidUntil(), parsedToken.getExpiration());

            if (handlerState.isEmpty()) {
                return Response
                        .seeOther(URI.create(UI_LOCATION))
                        .cookie(OAuthWebUiCookie.create(oauth2Response.getAccessToken(), validUntil), NonceCookie.delete())
                        .build();
            }

            tokenHandler.setAccessToken(handlerState.get(), oauth2Response.getAccessToken());

            Response.ResponseBuilder builder = Response.ok(getSuccessHtml());
            if (webUiOAuthEnabled) {
                builder.cookie(OAuthWebUiCookie.create(oauth2Response.getAccessToken(), validUntil));
            }
            return builder.cookie(NonceCookie.delete()).build();
        }
        catch (ChallengeFailedException | RuntimeException e) {
            LOG.debug(e, "Authentication response could not be verified: state=%s", state);
            handlerState.ifPresent(value ->
                    tokenHandler.setTokenExchangeError(value, format("Authentication response could not be verified: state=%s", value)));
            return Response.status(BAD_REQUEST)
                    .cookie(NonceCookie.delete())
                    .entity(getInternalFailureHtml("Authentication response could not be verified"))
                    .build();
        }
    }

    private static Instant determineExpiration(Optional<Instant> validUntil, Date expiration)
            throws ChallengeFailedException
    {
        if (validUntil.isPresent()) {
            if (expiration != null) {
                return Ordering.natural().min(validUntil.get(), expiration.toInstant());
            }

            return validUntil.get();
        }

        if (expiration != null) {
            return expiration.toInstant();
        }

        throw new ChallengeFailedException("no valid expiration date");
    }

    private Claims parseState(String state)
            throws ChallengeFailedException
    {
        try {
            return jwtParser
                    .parseClaimsJws(state)
                    .getBody();
        }
        catch (RuntimeException e) {
            throw new ChallengeFailedException("State validation failed", e);
        }
    }

    private Optional<Claims> validateAndParseOAuth2Response(OAuth2Response oauth2Response, Optional<String> nonce)
            throws ChallengeFailedException
    {
        validateIdTokenAndNonce(oauth2Response, nonce);

        return internalConvertTokenToClaims(oauth2Response.getAccessToken());
    }

    private void validateIdTokenAndNonce(OAuth2Response oauth2Response, Optional<String> nonce)
            throws ChallengeFailedException
    {
        if (nonce.isPresent() && oauth2Response.getIdToken().isPresent()) {
            // validate ID token including nonce
            Claims claims = newJwtParserBuilder()
                    .setSigningKeyResolver(signingKeyResolver)
                    .requireIssuer(issuer)
                    .require(NONCE, hashNonce(nonce.get()))
                    .build()
                    .parseClaimsJws(oauth2Response.getIdToken().get())
                    .getBody();
            validateAudience(claims, false);
        }
        else if (nonce.isPresent() != oauth2Response.getIdToken().isPresent()) {
            // The user did not grant the requested scope `openid` thus either
            // we do not have the ID token and therefore can't validate the
            // nonce, or the ID token is present but the nonce is not.
            throw new ChallengeFailedException("Cannot validate nonce parameter");
        }
    }

    public Optional<Map<String, Object>> convertTokenToClaims(String token)
            throws ChallengeFailedException
    {
        return internalConvertTokenToClaims(token).map(claims -> claims);
    }

    private Optional<Claims> internalConvertTokenToClaims(String accessToken)
            throws ChallengeFailedException
    {
        if (userinfoUri.isPresent()) {
            // validate access token is trusted by remote userinfo endpoint
            Request request = Request.builder()
                    .setMethod(POST)
                    .addHeader(AUTHORIZATION, "Bearer " + accessToken)
                    .setUri(userinfoUri.get())
                    .build();
            try {
                Map<String, Object> userinfoClaims = httpClient.execute(request, USERINFO_RESPONSE_HANDLER);
                Claims claims = new DefaultClaims(userinfoClaims);
                validateAudience(claims, true);
                return Optional.of(claims);
            }
            catch (RuntimeException e) {
                LOG.error(e, "Received bad response from userinfo endpoint");
                return Optional.empty();
            }
        }

        // validate access token is trusted by this server
        Claims claims = newJwtParserBuilder()
                .setSigningKeyResolver(signingKeyResolver)
                .requireIssuer(accessTokenIssuer)
                .build()
                .parseClaimsJws(accessToken)
                .getBody();
        validateAudience(claims, true);
        return Optional.of(claims);
    }

    private void validateAudience(Claims claims, boolean isAccessToken)
            throws ChallengeFailedException
    {
        Set<String> validAudiences;
        Object tokenAudience = claims.get(AUDIENCE);
        if (isAccessToken) {
            // The null/empty check is for access tokens which do not contain an audience. ID tokens must have an audience.
            if (tokenAudience == null || (tokenAudience instanceof Collection && ((Collection<?>) tokenAudience).isEmpty())) {
                return;
            }
            validAudiences = allowedAudiences;
        }
        else {
            validAudiences = Set.of(clientId);
        }

        if (tokenAudience instanceof String) {
            if (!validAudiences.contains((String) tokenAudience)) {
                throw new ChallengeFailedException(format("Invalid Audience: %s. Allowed audiences: %s", tokenAudience, allowedAudiences));
            }
        }
        else if (tokenAudience instanceof Collection) {
            if (((Collection<?>) tokenAudience).stream().map(String.class::cast).noneMatch(validAudiences::contains)) {
                throw new ChallengeFailedException(format("Invalid Audience: %s. Allowed audiences: %s", tokenAudience, allowedAudiences));
            }
        }
        else {
            throw new ChallengeFailedException(format("Invalid Audience: %s", tokenAudience));
        }
    }

    public String getSuccessHtml()
    {
        return successHtml;
    }

    public String getCallbackErrorHtml(String errorCode)
    {
        return failureHtml.replace(FAILURE_REPLACEMENT_TEXT, getOAuth2ErrorMessage(errorCode));
    }

    public String getInternalFailureHtml(String errorMessage)
    {
        return failureHtml.replace(FAILURE_REPLACEMENT_TEXT, nullToEmpty(errorMessage));
    }

    private static byte[] secureRandomBytes(int count)
    {
        byte[] bytes = new byte[count];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static String getOAuth2ErrorMessage(String errorCode)
    {
        switch (errorCode) {
            case "access_denied":
                return "OAuth2 server denied the login";
            case "unauthorized_client":
                return "OAuth2 server does not allow request from this Trino server";
            case "server_error":
                return "OAuth2 server had a failure";
            case "temporarily_unavailable":
                return "OAuth2 server is temporarily unavailable";
            default:
                return "OAuth2 unknown error code: " + errorCode;
        }
    }

    private static String randomNonce()
    {
        // https://developers.login.gov/oidc/#authorization
        // min 22 chars so 24 chars obtained by encoding random 18 bytes using Base64 should be enough
        return BaseEncoding.base64Url().encode(secureRandomBytes(18));
    }

    @VisibleForTesting
    public static String hashNonce(String nonce)
    {
        return sha256()
                .hashString(nonce, UTF_8)
                .toString();
    }
}
