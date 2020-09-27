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
package io.prestosql.server.security.oauth2;

import com.github.scribejava.core.oauth2.OAuth2Error;
import io.airlift.log.Logger;
import io.prestosql.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_API_PREFIX;
import static io.prestosql.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FOUND;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path(OAUTH2_API_PREFIX)
public class OAuth2Resource
{
    private static final Logger LOG = Logger.get(OAuth2Resource.class);
    static final String OAUTH2_COOKIE = "Presto-OAuth2-Token";

    static final String OAUTH2_API_PREFIX = "/oauth2";
    static final String CALLBACK_ENDPOINT = "/callback";

    private final OAuth2Service service;

    @Inject
    public OAuth2Resource(OAuth2Service service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path(CALLBACK_ENDPOINT)
    public Response callback(
            @QueryParam("state") String state,
            @QueryParam("code") String code,
            @QueryParam("error") String error,
            @QueryParam("error_description") String errorDescription,
            @QueryParam("error_uri") String errorUri,
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext)
    {
        try {
            Challenge challenge = service.finishChallenge(
                    uriInfo.getBaseUri(),
                    State.valueOf(state),
                    Optional.ofNullable(code),
                    Optional.ofNullable(error)
                            .map(err ->
                                    new OAuth2ErrorResponse(
                                            OAuth2Error.parseFrom(error),
                                            Optional.ofNullable(errorDescription),
                                            Optional.ofNullable(errorUri).map(URI::create))));
            switch (challenge.getStatus()) {
                case SUCCEEDED:
                    Challenge.Succeeded succeeded = (Challenge.Succeeded) challenge;
                    return Response
                            .status(FOUND)
                            .header(LOCATION, UI_LOCATION)
                            .cookie(new NewCookie(
                                    OAUTH2_COOKIE,
                                    succeeded.getToken().getAccessToken(),
                                    UI_LOCATION,
                                    null,
                                    Cookie.DEFAULT_VERSION,
                                    null,
                                    NewCookie.DEFAULT_MAX_AGE,
                                    succeeded.getJwtToken().getBody().getExpiration(),
                                    securityContext.isSecure(),
                                    true))
                            .build();
                case FAILED:
                    Challenge.Failed failed = (Challenge.Failed) challenge;
                    return Response
                            .status(BAD_REQUEST)
                            .entity(failed.getError())
                            .build();
                default:
                    String message = format("Invalid challenge: state=%s status=%s", challenge.getState(), challenge.getStatus());
                    LOG.error(message);
                    return Response
                            .status(INTERNAL_SERVER_ERROR)
                            .entity(message)
                            .build();
            }
        }
        catch (ChallengeNotFoundException e) {
            String message = "Challenge not found: state=" + e.getState();
            LOG.debug(message, e);
            return Response
                    .status(NOT_FOUND)
                    .entity(message)
                    .build();
        }
        catch (IllegalArgumentException e) {
            String message = "Invalid challenge request: " + e.getMessage();
            LOG.debug(message, e);
            return Response
                    .status(BAD_REQUEST)
                    .entity(message)
                    .build();
        }
        catch (InterruptedException | ExecutionException | IOException e) {
            return Response
                    .status(INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
    }
}
