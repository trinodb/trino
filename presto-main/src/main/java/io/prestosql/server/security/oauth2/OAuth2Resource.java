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
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_API_PREFIX;
import static io.prestosql.server.security.oauth2.Status.FAILED;
import static io.prestosql.server.security.oauth2.Status.STARTED;
import static io.prestosql.server.security.oauth2.Status.SUCCEEDED;
import static io.prestosql.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FOUND;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

@Path(OAUTH2_API_PREFIX)
public class OAuth2Resource
{
    private static final Logger LOG = Logger.get(OAuth2Resource.class);
    private static final int DEPENDENCY_FAILED = 424;
    static final String OAUTH2_COOKIE = "Presto-OAuth2-Token";

    static final String OAUTH2_API_PREFIX = "/oauth2";
    static final String CALLBACK_ENDPOINT = "/callback";
    static final String TOKENS_ENDPOINT = "/tokens";

    private final OAuth2Service service;
    private final TokenPollingExecutors executors;

    @Inject
    public OAuth2Resource(OAuth2Service service, TokenPollingExecutors executors)
    {
        this.service = requireNonNull(service, "service is null");
        this.executors = requireNonNull(executors, "executors is null");
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

        return challenge.isInStatus(SUCCEEDED)
                .map(succeeded -> Response
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
                        .build())
                .or(() -> challenge.isInStatus(FAILED)
                        .map(failed -> Response
                                .status(BAD_REQUEST)
                                .entity(failed.getError())
                                .build()))
                .orElseGet(() -> {
                    String message = format("Invalid challenge: state=%s status=%s", challenge.getState(), challenge.getStatus());
                    LOG.error(message);
                    return Response
                            .status(INTERNAL_SERVER_ERROR)
                            .entity(message)
                            .build();
                });
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path(TOKENS_ENDPOINT)
    public void tokenPoll(@QueryParam("state") String state, @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(state, "state is null");
        CompletableFuture<Response> polling = service.pollForFinish(State.valueOf(state), executors.getPollingExecutor())
                .thenApply(this::processTokenPollRequest)
                .exceptionally(throwable -> {
                    if (throwable instanceof ChallengeNotFoundException) {
                        return Response.status(NOT_FOUND).build();
                    }
                    return Response.status(SERVICE_UNAVAILABLE)
                            .entity(throwable.getMessage())
                            .type(TEXT_PLAIN_TYPE)
                            .encoding("UTF-8")
                            .build();
                });
        bindAsyncResponse(asyncResponse, toListenableFuture(polling), executors.getPollingHttpRequestExecutor());
    }

    private Response processTokenPollRequest(Challenge challenge)
    {
        return challenge.isInStatus(STARTED)
                .map(started -> Response.status(ACCEPTED).build())
                .or(() -> challenge.isInStatus(SUCCEEDED)
                        .map(succeeded -> Response.ok()
                                .type(TEXT_PLAIN_TYPE)
                                .entity(succeeded.getToken().getAccessToken())
                                .build()))
                .or(() -> challenge.isInStatus(FAILED)
                        .map(failed -> Response.status(DEPENDENCY_FAILED)
                                .type(TEXT_PLAIN_TYPE)
                                .entity(failed.getError().getError())
                                .build()))
                .orElseThrow();
    }
}
