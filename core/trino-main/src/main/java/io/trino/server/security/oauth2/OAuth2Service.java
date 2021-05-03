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

import com.google.common.collect.Ordering;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.oauth2.OAuth2Client.AccessToken;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Date;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class OAuth2Service
{
    public static final String REDIRECT_URI = "redirect_uri";
    public static final String STATE = "state";
    public static final String NONCE = "nonce";
    public static final String OPENID_SCOPE = "openid";

    private static final String STATE_AUDIENCE_UI = "trino_oauth_ui";
    private static final String STATE_AUDIENCE_REST = "trino_oauth_rest";
    private static final String FAILURE_REPLACEMENT_TEXT = "<!-- ERROR_MESSAGE -->";
    private static final Random SECURE_RANDOM = new SecureRandom();

    private final OAuth2Client client;
    private final SigningKeyResolver signingKeyResolver;

    private final String successHtml;
    private final String failureHtml;

    private final Set<String> scopes;
    private final TemporalAmount challengeTimeout;
    private final byte[] stateHmac;

    @Inject
    public OAuth2Service(OAuth2Client client, @ForOAuth2 SigningKeyResolver signingKeyResolver, OAuth2Config oauth2Config)
            throws IOException
    {
        this.client = requireNonNull(client, "client is null");
        this.signingKeyResolver = requireNonNull(signingKeyResolver, "signingKeyResolver is null");

        this.successHtml = Resources.toString(Resources.getResource(getClass(), "/oauth2/success.html"), UTF_8);
        this.failureHtml = Resources.toString(Resources.getResource(getClass(), "/oauth2/failure.html"), UTF_8);
        verify(failureHtml.contains(FAILURE_REPLACEMENT_TEXT), "login.html does not contain the replacement text");

        requireNonNull(oauth2Config, "oauth2Config is null");
        this.scopes = oauth2Config.getScopes();
        this.challengeTimeout = Duration.ofMillis(oauth2Config.getChallengeTimeout().toMillis());
        this.stateHmac = oauth2Config.getStateKey()
                .map(key -> sha256().hashString(key, UTF_8).asBytes())
                .orElseGet(() -> secureRandomBytes(32));
    }

    public OAuthChallenge startWebUiChallenge(URI callbackUri)
    {
        Instant challengeExpiration = Instant.now().plus(challengeTimeout);
        String state = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, stateHmac)
                .setAudience(STATE_AUDIENCE_UI)
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
        return new OAuthChallenge(client.getAuthorizationUri(state, callbackUri, nonce.map(OAuth2Service::hashNonce)), challengeExpiration, nonce);
    }

    public URI startRestChallenge(URI callbackUri, UUID authId)
    {
        String state = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, stateHmac)
                .setId(authId.toString())
                .setAudience(STATE_AUDIENCE_REST)
                .setExpiration(Date.from(Instant.now().plus(challengeTimeout)))
                .compact();
        return client.getAuthorizationUri(state, callbackUri, Optional.empty());
    }

    public OAuthResult finishChallenge(Optional<UUID> authId, String code, URI callbackUri, Optional<String> nonce)
            throws ChallengeFailedException
    {
        requireNonNull(callbackUri, "callbackUri is null");
        requireNonNull(authId, "authId is null");
        requireNonNull(code, "code is null");

        // fetch access token
        AccessToken accessToken = client.getAccessToken(code, callbackUri);

        // validate access token is trusted by this server
        Claims parsedToken = Jwts.parser()
                .setSigningKeyResolver(signingKeyResolver)
                .parseClaimsJws(accessToken.getAccessToken())
                .getBody();

        validateNonce(authId, accessToken, nonce);

        // determine expiration
        Instant validUntil = accessToken.getValidUntil()
                .map(instant -> Ordering.natural().min(instant, parsedToken.getExpiration().toInstant()))
                .orElse(parsedToken.getExpiration().toInstant());

        return new OAuthResult(authId, accessToken.getAccessToken(), validUntil);
    }

    public Optional<UUID> getAuthId(String state)
            throws ChallengeFailedException
    {
        Claims stateClaims = parseState(state);
        if (STATE_AUDIENCE_UI.equals(stateClaims.getAudience())) {
            return Optional.empty();
        }
        if (STATE_AUDIENCE_REST.equals(stateClaims.getAudience())) {
            try {
                return Optional.of(UUID.fromString(stateClaims.getId()));
            }
            catch (IllegalArgumentException e) {
                throw new ChallengeFailedException("State is does not contain an auth ID");
            }
        }
        // this is very unlikely, but is a good safety check
        throw new ChallengeFailedException("Unexpected state audience");
    }

    private Claims parseState(String state)
            throws ChallengeFailedException
    {
        try {
            return Jwts.parser()
                    .setSigningKey(stateHmac)
                    .parseClaimsJws(state)
                    .getBody();
        }
        catch (RuntimeException e) {
            throw new ChallengeFailedException("State validation failed", e);
        }
    }

    public Jws<Claims> parseClaimsJws(String token)
    {
        return Jwts.parser()
                .setSigningKeyResolver(signingKeyResolver)
                .parseClaimsJws(token);
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

    private void validateNonce(Optional<UUID> authId, AccessToken accessToken, Optional<String> nonce)
            throws ChallengeFailedException
    {
        if (authId.isPresent()) {
            // authId is available only for REST API calls.
            // do not validate nonce if it's a REST challenge
            // the challenge was not started by a web browser therefore it is expected that the cookie is missing
            return;
        }

        // user did not grant the requested scope `openid` so we don't have the ID token and therefore can't validate the nonce
        // or the ID token is present but the nonce is not which means that someone is trying to obtain the token which he is not supposed to get
        if (nonce.isPresent() != accessToken.getIdToken().isPresent()) {
            throw new ChallengeFailedException("Cannot validate nonce parameter");
        }

        // validate nonce from the ID token
        nonce.ifPresent(n -> Jwts.parser()
                .setSigningKeyResolver(signingKeyResolver)
                .require(NONCE, hashNonce(n))
                .parseClaimsJws(accessToken.getIdToken().get()));
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

    private static String hashNonce(String nonce)
    {
        return Hashing.sha256()
                .hashString(nonce, StandardCharsets.UTF_8)
                .toString();
    }

    public static class OAuthChallenge
    {
        private final URI redirectUrl;
        private final Instant challengeExpiration;
        private final Optional<String> nonce;

        public OAuthChallenge(URI redirectUrl, Instant challengeExpiration, Optional<String> nonce)
        {
            this.redirectUrl = requireNonNull(redirectUrl, "redirectUrl is null");
            this.challengeExpiration = requireNonNull(challengeExpiration, "challengeExpiration is null");
            this.nonce = requireNonNull(nonce, "nonce is null");
        }

        public URI getRedirectUrl()
        {
            return redirectUrl;
        }

        public Instant getChallengeExpiration()
        {
            return challengeExpiration;
        }

        public Optional<String> getNonce()
        {
            return nonce;
        }
    }

    public static class OAuthResult
    {
        private final Optional<UUID> authId;
        private final String accessToken;
        private final Instant tokenExpiration;

        public OAuthResult(Optional<UUID> authId, String accessToken, Instant tokenExpiration)
        {
            this.authId = requireNonNull(authId, "authId is null");
            this.accessToken = requireNonNull(accessToken, "accessToken is null");
            this.tokenExpiration = requireNonNull(tokenExpiration, "tokenExpiration is null");
        }

        /**
         * Get auth ID if this is a REST client request.
         * This will be empty if the authentication request is a web UI login.
         */
        public Optional<UUID> getAuthId()
        {
            return authId;
        }

        public String getAccessToken()
        {
            return accessToken;
        }

        public Instant getTokenExpiration()
        {
            return tokenExpiration;
        }
    }
}
