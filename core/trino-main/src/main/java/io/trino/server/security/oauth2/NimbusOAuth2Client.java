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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTProcessor;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationRequest;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.openid.connect.sdk.AuthenticationRequest;
import com.nimbusds.openid.connect.sdk.Nonce;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.UserInfoRequest;
import com.nimbusds.openid.connect.sdk.UserInfoResponse;
import com.nimbusds.openid.connect.sdk.claims.AccessTokenHash;
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet;
import com.nimbusds.openid.connect.sdk.token.OIDCTokens;
import com.nimbusds.openid.connect.sdk.validators.AccessTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.InvalidHashException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.hash.Hashing.sha256;
import static com.nimbusds.oauth2.sdk.ResponseType.CODE;
import static com.nimbusds.openid.connect.sdk.OIDCScopeValue.OPENID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NimbusOAuth2Client
        implements OAuth2Client
{
    private static final Logger LOG = Logger.get(NimbusAirliftHttpClient.class);

    private final Issuer issuer;
    private final ClientID clientId;
    private final ClientSecretBasic clientAuth;
    private final Scope scope;
    private final URI authUrl;
    private final URI tokenUrl;
    private final Optional<URI> userinfoUri;
    private final Duration maxClockSkew;
    private final NimbusHttpClient httpClient;
    private final JWSKeySelector<SecurityContext> jwsKeySelector;
    private final JWTProcessor<SecurityContext> accessTokenProcessor;
    private final AuthorizationCodeFlow flow;

    @Inject
    public NimbusOAuth2Client(OAuth2Config config, NimbusHttpClient httpClient)
            throws MalformedURLException
    {
        requireNonNull(config, "config is null");
        issuer = new Issuer(config.getIssuer());
        clientId = new ClientID(config.getClientId());
        clientAuth = new ClientSecretBasic(clientId, new Secret(config.getClientSecret()));
        scope = Scope.parse(config.getScopes());

        authUrl = URI.create(requireNonNull(config.getAuthUrl(), "authUrl is null"));
        tokenUrl = URI.create(requireNonNull(config.getTokenUrl(), "tokenUrl is null"));
        userinfoUri = config.getUserinfoUrl().map(URI::create);
        maxClockSkew = config.getMaxClockSkew();

        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        jwsKeySelector = new JWSVerificationKeySelector<>(
                Stream.concat(JWSAlgorithm.Family.RSA.stream(), JWSAlgorithm.Family.EC.stream()).collect(toImmutableSet()),
                new RemoteJWKSet<>(new URL(config.getJwksUrl()), httpClient));

        DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
        processor.setJWSKeySelector(jwsKeySelector);
        Set<String> allowedAudiences = new HashSet<>(config.getAdditionalAudiences());
        allowedAudiences.add(clientId.getValue());
        allowedAudiences.add(null); // A null value in the set allows JWTs with no audience

        DefaultJWTClaimsVerifier<SecurityContext> accessTokenVerifier = new DefaultJWTClaimsVerifier<>(
                allowedAudiences,
                new JWTClaimsSet.Builder()
                        .issuer(config.getAccessTokenIssuer().orElse(config.getIssuer()))
                        .build(),
                ImmutableSet.of(requireNonNull(config.getPrincipalField(), "principalField is null")),
                ImmutableSet.of());
        accessTokenVerifier.setMaxClockSkew((int) maxClockSkew.roundTo(SECONDS));
        processor.setJWTClaimsSetVerifier(accessTokenVerifier);
        accessTokenProcessor = processor;

        flow = scope.contains(OPENID) ? new OAuth2WithOidcExtensionsCodeFlow() : new OAuth2AuthorizationCodeFlow();
    }

    @Override
    public Request createAuthorizationRequest(String state, URI callbackUri)
    {
        return flow.createAuthorizationRequest(state, callbackUri);
    }

    @Override
    public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
            throws ChallengeFailedException
    {
        return flow.getOAuth2Response(code, callbackUri, nonce);
    }

    @Override
    public Optional<Map<String, Object>> getClaims(String accessToken)
    {
        return getJWTClaimsSet(accessToken).map(JWTClaimsSet::getClaims);
    }

    private interface AuthorizationCodeFlow
    {
        Request createAuthorizationRequest(String state, URI callbackUri);

        Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                throws ChallengeFailedException;
    }

    private class OAuth2AuthorizationCodeFlow
            implements AuthorizationCodeFlow
    {
        @Override
        public Request createAuthorizationRequest(String state, URI callbackUri)
        {
            return new Request(
                    new AuthorizationRequest.Builder(CODE, clientId)
                            .redirectionURI(callbackUri)
                            .scope(scope)
                            .endpointURI(authUrl)
                            .state(new State(state))
                            .build()
                            .toURI(),
                    Optional.empty());
        }

        @Override
        public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                throws ChallengeFailedException
        {
            checkArgument(nonce.isEmpty(), "Unexpected nonce provided");
            AccessTokenResponse tokenResponse = getTokenResponse(code, callbackUri, AccessTokenResponse::parse);
            Tokens tokens = tokenResponse.toSuccessResponse().getTokens();
            AccessToken accessToken = tokens.getAccessToken();
            JWTClaimsSet claims = getJWTClaimsSet(accessToken.getValue()).orElseThrow(() -> new ChallengeFailedException("invalid access token"));
            return new Response(
                    accessToken.getValue(),
                    determineExpiration(getExpiration(accessToken), claims.getExpirationTime()),
                    Optional.empty());
        }
    }

    private class OAuth2WithOidcExtensionsCodeFlow
            implements AuthorizationCodeFlow
    {
        private final IDTokenValidator idTokenValidator;

        public OAuth2WithOidcExtensionsCodeFlow()
        {
            idTokenValidator = new IDTokenValidator(issuer, clientId, jwsKeySelector, null);
            idTokenValidator.setMaxClockSkew((int) maxClockSkew.roundTo(SECONDS));
        }

        @Override
        public Request createAuthorizationRequest(String state, URI callbackUri)
        {
            String nonce = new Nonce().getValue();
            return new Request(
                    new AuthenticationRequest.Builder(CODE, scope, clientId, callbackUri)
                            .endpointURI(authUrl)
                            .state(new State(state))
                            .nonce(new Nonce(hashNonce(nonce)))
                            .build()
                            .toURI(),
                    Optional.of(nonce));
        }

        @Override
        public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                throws ChallengeFailedException
        {
            OIDCTokenResponse tokenResponse = getTokenResponse(code, callbackUri, OIDCTokenResponse::parse);
            OIDCTokens tokens = tokenResponse.getOIDCTokens();
            validateTokens(tokens, nonce);
            AccessToken accessToken = tokens.getAccessToken();
            JWTClaimsSet claims = getJWTClaimsSet(accessToken.getValue()).orElseThrow(() -> new ChallengeFailedException("invalid access token"));
            return new Response(
                    accessToken.getValue(),
                    determineExpiration(getExpiration(accessToken), claims.getExpirationTime()),
                    Optional.ofNullable(tokens.getIDTokenString()));
        }

        private void validateTokens(OIDCTokens tokens, Optional<String> nonce)
                throws ChallengeFailedException
        {
            try {
                IDTokenClaimsSet idToken = idTokenValidator.validate(
                        tokens.getIDToken(),
                        nonce.map(this::hashNonce)
                                .map(Nonce::new)
                                .orElseThrow(() -> new ChallengeFailedException("Missing nonce")));
                AccessTokenHash accessTokenHash = idToken.getAccessTokenHash();
                if (accessTokenHash != null) {
                    AccessTokenValidator.validate(tokens.getAccessToken(), ((JWSHeader) tokens.getIDToken().getHeader()).getAlgorithm(), accessTokenHash);
                }
            }
            catch (BadJOSEException | JOSEException | InvalidHashException e) {
                throw new ChallengeFailedException("Cannot validate nonce parameter", e);
            }
        }

        private String hashNonce(String nonce)
        {
            return sha256()
                    .hashString(nonce, UTF_8)
                    .toString();
        }
    }

    private <T extends AccessTokenResponse> T getTokenResponse(String code, URI callbackUri, NimbusAirliftHttpClient.Parser<T> parser)
            throws ChallengeFailedException
    {
        T tokenResponse = httpClient.execute(new TokenRequest(tokenUrl, clientAuth, new AuthorizationCodeGrant(new AuthorizationCode(code), callbackUri)), parser);
        if (!tokenResponse.indicatesSuccess()) {
            throw new ChallengeFailedException("Error while fetching access token: " + tokenResponse.toErrorResponse().toJSONObject());
        }
        return tokenResponse;
    }

    private Optional<JWTClaimsSet> getJWTClaimsSet(String accessToken)
    {
        if (userinfoUri.isPresent()) {
            return queryUserInfo(accessToken);
        }
        return parseAccessToken(accessToken);
    }

    private Optional<JWTClaimsSet> queryUserInfo(String accessToken)
    {
        try {
            UserInfoResponse response = httpClient.execute(new UserInfoRequest(userinfoUri.get(), new BearerAccessToken(accessToken)), UserInfoResponse::parse);
            if (!response.indicatesSuccess()) {
                LOG.error("Received bad response from userinfo endpoint: " + response.toErrorResponse().getErrorObject());
                return Optional.empty();
            }
            return Optional.of(response.toSuccessResponse().getUserInfo().toJWTClaimsSet());
        }
        catch (ParseException | RuntimeException e) {
            LOG.error(e, "Received bad response from userinfo endpoint");
            return Optional.empty();
        }
    }

    private Optional<JWTClaimsSet> parseAccessToken(String accessToken)
    {
        try {
            return Optional.of(accessTokenProcessor.process(accessToken, null));
        }
        catch (java.text.ParseException | BadJOSEException | JOSEException e) {
            LOG.error(e, "Failed to parse JWT access token");
            return Optional.empty();
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

    private static Optional<Instant> getExpiration(AccessToken accessToken)
    {
        return accessToken.getLifetime() != 0 ? Optional.of(Instant.now().plusSeconds(accessToken.getLifetime())) : Optional.empty();
    }
}
