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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMappingException;
import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.Request;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOAuth2Authenticator
{
    private static final String ISSUER = "https://idp.example.com/realm";
    private static final URI BASE_URI = URI.create("https://trino.example.com/");

    // -- createIdentity --

    @Test
    public void testCreateIdentitySuccess()
            throws UserMappingException
    {
        OAuth2Authenticator authenticator = authenticator(_ -> Optional.of(ImmutableMap.of("sub", "alice@example.com")));

        Optional<Identity> identity = authenticator.createIdentity(validToken());

        assertThat(identity).isPresent();
        assertThat(identity.get().getUser()).isEqualTo("alice@example.com");
    }

    @Test
    public void testCreateIdentityExpiredToken()
            throws UserMappingException
    {
        TokenPair expired = new TokenPair("access-token", Date.from(Instant.EPOCH), Optional.empty());
        OAuth2Client claimsProvider = _ -> Optional.of(ImmutableMap.of("sub", "alice@example.com"));
        TokenPairSerializer expiredTokenSerializer = new TokenPairSerializer()
        {
            @Override
            public TokenPairSerializer.TokenPair deserialize(String token)
            {
                return expired;
            }

            @Override
            public String serialize(TokenPairSerializer.TokenPair tokenPair)
            {
                return tokenPair.accessToken();
            }
        };
        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                claimsProvider,
                config(),
                noOpTokenRefresher(),
                expiredTokenSerializer);

        Optional<Identity> identity = authenticator.createIdentity("any-token");

        assertThat(identity).isEmpty();
    }

    @Test
    public void testCreateIdentityMissingPrincipalField()
            throws UserMappingException
    {
        // claims do not contain the "sub" field
        OAuth2Authenticator authenticator = authenticator(_ -> Optional.of(ImmutableMap.of("email", "alice@example.com")));

        Optional<Identity> identity = authenticator.createIdentity(validToken());

        assertThat(identity).isEmpty();
    }

    @Test
    public void testCreateIdentityUnrecognisedToken()
            throws UserMappingException
    {
        OAuth2Authenticator authenticator = authenticator(_ -> Optional.empty());

        Optional<Identity> identity = authenticator.createIdentity(validToken());

        assertThat(identity).isEmpty();
    }

    // -- needAuthentication --

    @Test
    public void testNeedAuthenticationChallengeContainsIssuerAndScope()
    {
        OAuth2Authenticator authenticator = authenticator(_ -> Optional.empty());

        AuthenticationException exception = authenticator.needAuthentication(request(), Optional.empty(), "Unauthorized");

        String challenge = exception.getAuthenticateHeader().orElseThrow();
        assertThat(challenge).contains("x_redirect_server=");
        assertThat(challenge).contains("x_token_server=");
        assertThat(challenge).contains("x_issuer_server=\"" + ISSUER + "\"");
        assertThat(challenge).contains("scope=\"openid\"");
    }

    @Test
    public void testNeedAuthenticationChallengeMultipleScopes()
    {
        OAuth2Config config = config();
        config.setScopes(ImmutableSet.of("openid", "profile", "email"));
        OAuth2Client claimsProvider = _ -> Optional.empty();
        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                claimsProvider,
                config,
                noOpTokenRefresher(),
                TokenPairSerializer.ACCESS_TOKEN_ONLY_SERIALIZER);

        AuthenticationException exception = authenticator.needAuthentication(request(), Optional.empty(), "Unauthorized");

        String challenge = exception.getAuthenticateHeader().orElseThrow();
        // RFC 6750: scopes are space-separated within a single scope parameter
        assertThat(challenge).containsPattern("scope=\"[^\"]*openid[^\"]*\"");
        assertThat(challenge).containsPattern("scope=\"[^\"]*profile[^\"]*\"");
        assertThat(challenge).containsPattern("scope=\"[^\"]*email[^\"]*\"");
    }

    @Test
    public void testNeedAuthenticationWithExpiredTokenFallsBackToFullChallenge()
    {
        // token that fails to deserialize → falls through to full redirect challenge
        OAuth2Client claimsProvider = _ -> Optional.empty();
        TokenPairSerializer failingSerializer = new TokenPairSerializer()
        {
            @Override
            public TokenPairSerializer.TokenPair deserialize(String token)
            {
                throw new RuntimeException("cannot deserialize");
            }

            @Override
            public String serialize(TokenPairSerializer.TokenPair tokenPair)
            {
                return tokenPair.accessToken();
            }
        };
        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                claimsProvider,
                config(),
                noOpTokenRefresher(),
                failingSerializer);

        AuthenticationException exception = authenticator.needAuthentication(request(), Optional.of("garbage-token"), "Unauthorized");

        String challenge = exception.getAuthenticateHeader().orElseThrow();
        assertThat(challenge).contains("x_redirect_server=");
        assertThat(challenge).contains("x_issuer_server=\"" + ISSUER + "\"");
    }

    @Test
    public void testNeedAuthenticationWithRefreshableTokenReturnsTokenServerOnly()
    {
        TokenPair tokenPairWithRefresh = TokenPair.withAccessAndRefreshTokens(
                "access-token",
                Date.from(Instant.now().plusSeconds(3600)),
                "refresh-token");
        TokenPairSerializer serializer = new TokenPairSerializer()
        {
            @Override
            public TokenPair deserialize(String token)
            {
                return tokenPairWithRefresh;
            }

            @Override
            public String serialize(TokenPair tokenPair)
            {
                return tokenPair.accessToken();
            }
        };

        TokenRefresher tokenRefresher = new TokenRefresher(
                serializer,
                noOpTokenHandler(),
                refreshableOAuth2Client());

        OAuth2Client claimsProvider = _ -> Optional.empty();
        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                claimsProvider,
                config(),
                tokenRefresher,
                serializer);

        AuthenticationException exception = authenticator.needAuthentication(request(), Optional.of("some-token"), "Unauthorized");

        String challenge = exception.getAuthenticateHeader().orElseThrow();
        // When a refresh is possible only x_token_server is returned — no redirect or issuer
        assertThat(challenge).contains("x_token_server=");
        assertThat(challenge).doesNotContain("x_redirect_server");
        assertThat(challenge).doesNotContain("x_issuer_server");
    }

    // -- helpers --

    private static OAuth2Authenticator authenticator(OAuth2Client client)
    {
        return new OAuth2Authenticator(client, config(), noOpTokenRefresher(), TokenPairSerializer.ACCESS_TOKEN_ONLY_SERIALIZER);
    }

    private static OAuth2Config config()
    {
        return new OAuth2Config()
                .setIssuer(ISSUER)
                .setClientId("trino-client")
                .setClientSecret("secret");
    }

    private static TokenRefresher noOpTokenRefresher()
    {
        return new TokenRefresher(
                TokenPairSerializer.ACCESS_TOKEN_ONLY_SERIALIZER,
                noOpTokenHandler(),
                noOpOAuth2Client());
    }

    private static OAuth2TokenHandler noOpTokenHandler()
    {
        return new OAuth2TokenHandler()
        {
            @Override
            public void setAccessToken(String hashedState, String accessToken) {}

            @Override
            public void setTokenExchangeError(String hashedState, String errorMessage) {}
        };
    }

    private static io.trino.server.security.oauth2.OAuth2Client refreshableOAuth2Client()
    {
        return new io.trino.server.security.oauth2.OAuth2Client()
        {
            @Override
            public void load() {}

            @Override
            public io.trino.server.security.oauth2.OAuth2Client.Request createAuthorizationRequest(String state, URI callbackUri)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public io.trino.server.security.oauth2.OAuth2Client.Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Map<String, Object>> getAccessTokenClaims(String accessToken)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public io.trino.server.security.oauth2.OAuth2Client.Response refreshTokens(String refreshToken)
            {
                return new io.trino.server.security.oauth2.OAuth2Client.Response(
                        "refreshed-access-token",
                        Instant.now().plusSeconds(3600),
                        Optional.empty(),
                        Optional.empty());
            }

            @Override
            public Optional<URI> getLogoutEndpoint(Optional<String> idToken, URI callbackUrl)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static io.trino.server.security.oauth2.OAuth2Client noOpOAuth2Client()
    {
        return new io.trino.server.security.oauth2.OAuth2Client()
        {
            @Override
            public void load() {}

            @Override
            public io.trino.server.security.oauth2.OAuth2Client.Request createAuthorizationRequest(String state, URI callbackUri)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public io.trino.server.security.oauth2.OAuth2Client.Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Map<String, Object>> getAccessTokenClaims(String accessToken)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public io.trino.server.security.oauth2.OAuth2Client.Response refreshTokens(String refreshToken)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<URI> getLogoutEndpoint(Optional<String> idToken, URI callbackUrl)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns a token string that ACCESS_TOKEN_ONLY_SERIALIZER deserializes with a far-future expiry.
     */
    private static String validToken()
    {
        return TokenPairSerializer.ACCESS_TOKEN_ONLY_SERIALIZER.serialize(TokenPair.withAccessToken("access-token"));
    }

    private static ContainerRequestContext request()
    {
        return new ContainerRequestContext()
        {
            @Override
            public UriInfo getUriInfo()
            {
                return new UriInfo()
                {
                    @Override
                    public URI getBaseUri()
                    {
                        return BASE_URI;
                    }

                    @Override
                    public URI resolve(URI uri)
                    {
                        return BASE_URI.resolve(uri);
                    }

                    @Override
                    public String getPath()
                    {
                        return null;
                    }

                    @Override
                    public String getPath(boolean decode)
                    {
                        return null;
                    }

                    @Override
                    public List<PathSegment> getPathSegments()
                    {
                        return null;
                    }

                    @Override
                    public List<PathSegment> getPathSegments(boolean decode)
                    {
                        return null;
                    }

                    @Override
                    public URI getRequestUri()
                    {
                        return BASE_URI;
                    }

                    @Override
                    public UriBuilder getRequestUriBuilder()
                    {
                        return null;
                    }

                    @Override
                    public UriBuilder getBaseUriBuilder()
                    {
                        return null;
                    }

                    @Override
                    public UriBuilder getAbsolutePathBuilder()
                    {
                        return null;
                    }

                    @Override
                    public URI getAbsolutePath()
                    {
                        return null;
                    }

                    @Override
                    public MultivaluedMap<String, String> getPathParameters()
                    {
                        return null;
                    }

                    @Override
                    public MultivaluedMap<String, String> getPathParameters(boolean decode)
                    {
                        return null;
                    }

                    @Override
                    public MultivaluedMap<String, String> getQueryParameters()
                    {
                        return null;
                    }

                    @Override
                    public MultivaluedMap<String, String> getQueryParameters(boolean decode)
                    {
                        return null;
                    }

                    @Override
                    public List<String> getMatchedURIs()
                    {
                        return null;
                    }

                    @Override
                    public List<String> getMatchedURIs(boolean decode)
                    {
                        return null;
                    }

                    @Override
                    public List<Object> getMatchedResources()
                    {
                        return null;
                    }

                    @Override
                    public URI relativize(URI uri)
                    {
                        return null;
                    }

                    @Override
                    public String getMatchedResourceTemplate()
                    {
                        return null;
                    }
                };
            }

            @Override
            public Object getProperty(String name)
            {
                return null;
            }

            @Override
            public Collection<String> getPropertyNames()
            {
                return null;
            }

            @Override
            public void setProperty(String name, Object object) {}

            @Override
            public void removeProperty(String name) {}

            @Override
            public void setRequestUri(URI requestUri) {}

            @Override
            public void setRequestUri(URI baseUri, URI requestUri) {}

            @Override
            public Request getRequest()
            {
                return null;
            }

            @Override
            public String getMethod()
            {
                return "GET";
            }

            @Override
            public void setMethod(String method) {}

            @Override
            public MultivaluedMap<String, String> getHeaders()
            {
                return null;
            }

            @Override
            public String getHeaderString(String name)
            {
                return null;
            }

            @Override
            public boolean containsHeaderString(String name, String valueSeparatorRegex, Predicate<String> valuePredicate)
            {
                return false;
            }

            @Override
            public Date getDate()
            {
                return null;
            }

            @Override
            public Locale getLanguage()
            {
                return null;
            }

            @Override
            public int getLength()
            {
                return 0;
            }

            @Override
            public MediaType getMediaType()
            {
                return null;
            }

            @Override
            public List<MediaType> getAcceptableMediaTypes()
            {
                return null;
            }

            @Override
            public List<Locale> getAcceptableLanguages()
            {
                return null;
            }

            @Override
            public Map<String, Cookie> getCookies()
            {
                return null;
            }

            @Override
            public boolean hasEntity()
            {
                return false;
            }

            @Override
            public InputStream getEntityStream()
            {
                return null;
            }

            @Override
            public void setEntityStream(InputStream input) {}

            @Override
            public SecurityContext getSecurityContext()
            {
                return null;
            }

            @Override
            public void setSecurityContext(SecurityContext context) {}

            @Override
            public void abortWith(Response response) {}
        };
    }

    /**
     * Minimal OAuth2Client stub — only getAccessTokenClaims is used by OAuth2Authenticator.
     */
    @FunctionalInterface
    private interface OAuth2Client
            extends io.trino.server.security.oauth2.OAuth2Client
    {
        @Override
        Optional<Map<String, Object>> getAccessTokenClaims(String accessToken);

        @Override
        default void load() {}

        @Override
        default io.trino.server.security.oauth2.OAuth2Client.Request createAuthorizationRequest(String state, URI callbackUri)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        default io.trino.server.security.oauth2.OAuth2Client.Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        default io.trino.server.security.oauth2.OAuth2Client.Response refreshTokens(String refreshToken)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        default Optional<URI> getLogoutEndpoint(Optional<String> idToken, URI callbackUrl)
        {
            throw new UnsupportedOperationException();
        }
    }
}
