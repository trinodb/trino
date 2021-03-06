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

import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.security.oauth2.OAuth2Service.OAuthResult;
import io.trino.server.ui.OAuth2WebUiInstalled;
import io.trino.server.ui.OAuthWebUiCookie;

import javax.inject.Inject;
import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.oauth2.NonceCookie.NONCE_COOKIE;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

@Path(CALLBACK_ENDPOINT)
public class OAuth2CallbackResource
{
    private static final Logger LOG = Logger.get(OAuth2CallbackResource.class);

    public static final String CALLBACK_ENDPOINT = "/oauth2/callback";

    private final OAuth2Service service;
    private final Optional<OAuth2TokenExchange> tokenExchange;
    private final boolean webUiOAuthEnabled;

    @Inject
    public OAuth2CallbackResource(OAuth2Service service, Optional<OAuth2TokenExchange> tokenExchange, Optional<OAuth2WebUiInstalled> webUiOAuthEnabled)
    {
        this.service = requireNonNull(service, "service is null");
        this.tokenExchange = requireNonNull(tokenExchange, "tokenExchange is null");
        this.webUiOAuthEnabled = requireNonNull(webUiOAuthEnabled, "webUiOAuthEnabled is null").isPresent();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(TEXT_HTML)
    public Response callback(
            @QueryParam("state") String state,
            @QueryParam("code") String code,
            @QueryParam("error") String error,
            @QueryParam("error_description") String errorDescription,
            @QueryParam("error_uri") String errorUri,
            @CookieParam(NONCE_COOKIE) Cookie nonce,
            @Context UriInfo uriInfo)
    {
        Optional<UUID> authId;
        try {
            authId = service.getAuthId(state);
        }
        catch (ChallengeFailedException e) {
            LOG.debug(e, "Authentication response could not be verified: state=%s", state);
            return Response.ok()
                    .entity(service.getInternalFailureHtml("Authentication response could not be verified"))
                    .build();
        }

        // Note: the Web UI may be disabled, so REST requests can not redirect to a success or error page inside of the Web UI

        if (error != null) {
            LOG.debug(
                    "OAuth server returned an error: error=%s, error_description=%s, error_uri=%s, state=%s",
                    error,
                    errorDescription,
                    errorUri,
                    state);

            passErrorToTokenExchange(
                    authId,
                    "OAuth server returned an error: error=%s, error_description=%s, error_uri=%s, state=%s",
                    error,
                    errorDescription,
                    errorUri,
                    state);
            return Response.ok()
                    .entity(service.getCallbackErrorHtml(error))
                    .build();
        }

        OAuthResult result;
        try {
            result = service.finishChallenge(
                    authId,
                    code,
                    uriInfo.getBaseUri().resolve(CALLBACK_ENDPOINT),
                    NonceCookie.read(nonce));
        }
        catch (ChallengeFailedException | RuntimeException e) {
            LOG.debug(e, "Authentication response could not be verified: state=%s", state);

            passErrorToTokenExchange(authId, "Authentication response could not be verified: state=%s", state);
            return Response.ok()
                    .entity(service.getInternalFailureHtml("Authentication response could not be verified"))
                    .build();
        }

        if (authId.isEmpty()) {
            return Response
                    .seeOther(URI.create(UI_LOCATION))
                    .cookie(OAuthWebUiCookie.create(result.getAccessToken(), result.getTokenExpiration()), NonceCookie.delete())
                    .build();
        }

        if (tokenExchange.isEmpty()) {
            LOG.debug("Token exchange is not active: state=%s", state);
            return Response.ok()
                    .entity(service.getInternalFailureHtml("Client token exchange is not enabled"))
                    .build();
        }

        tokenExchange.get().setAccessToken(authId.get(), result.getAccessToken());

        ResponseBuilder builder = Response.ok(service.getSuccessHtml());
        if (webUiOAuthEnabled) {
            builder.cookie(OAuthWebUiCookie.create(result.getAccessToken(), result.getTokenExpiration()));
        }
        return builder.build();
    }

    private void passErrorToTokenExchange(Optional<UUID> authId, String format, String... args)
    {
        if (tokenExchange.isEmpty() || authId.isEmpty()) {
            return;
        }
        tokenExchange.orElseThrow().setTokenExchangeError(authId.orElseThrow(), format(format, args));
    }
}
