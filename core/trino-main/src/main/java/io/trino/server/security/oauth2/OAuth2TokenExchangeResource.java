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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.trino.dispatcher.DispatchExecutor;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.security.oauth2.OAuth2TokenExchange.TokenPoll;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.UUID;

import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.oauth2.OAuth2TokenExchange.MAX_POLL_TIME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

@Path(OAuth2TokenExchangeResource.TOKEN_ENDPOINT)
public class OAuth2TokenExchangeResource
{
    static final String TOKEN_ENDPOINT = "/oauth2/token/";
    private static final String JSON_PAIR_TEMPLATE = "{ \"%s\":\"%s\" }";

    private final OAuth2TokenExchange tokenExchange;
    private final ListeningExecutorService responseExecutor;

    @Inject
    public OAuth2TokenExchangeResource(OAuth2TokenExchange tokenExchange, DispatchExecutor executor)
    {
        this.tokenExchange = requireNonNull(tokenExchange, "tokenExchange is null");
        this.responseExecutor = requireNonNull(executor, "responseExecutor is null").getExecutor();
    }

    @ResourceSecurity(PUBLIC)
    @Path("{authId}")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public void getAuthenticationToken(@PathParam("authId") UUID authId, @Suspended AsyncResponse asyncResponse, @Context HttpServletRequest request)
    {
        if (authId == null) {
            throw new BadRequestException();
        }

        // Do not drop the response from the cache in case the response fails, and
        // the client retries the request, which would result in a hang.  Response
        // will timeout eventually.
        // todo if we are concerned about the temp memory usage, we can have clients acknowledge receipt of the key with the DELETE call below
        ListenableFuture<TokenPoll> tokenFuture = tokenExchange.getTokenPoll(authId);
        ListenableFuture<Response> responseFuture = Futures.transform(tokenFuture, OAuth2TokenExchangeResource::toResponse, responseExecutor);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor)
                .withTimeout(MAX_POLL_TIME, pendingResponse(request));
    }

    private static Response toResponse(TokenPoll poll)
    {
        return poll.getError()
                .map(error -> Response.ok(format(JSON_PAIR_TEMPLATE, "error", error), APPLICATION_JSON_TYPE).build())
                .orElseGet(() -> poll.getToken()
                        .map(token -> Response.ok(format(JSON_PAIR_TEMPLATE, "token", token), APPLICATION_JSON_TYPE).build())
                        .orElseThrow());
    }

    private static Response pendingResponse(HttpServletRequest request)
    {
        return Response.ok(format(JSON_PAIR_TEMPLATE, "nextUri", request.getRequestURL()), APPLICATION_JSON_TYPE).build();
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("{authId}")
    public void deleteAuthenticationToken(@PathParam("authId") UUID authId)
    {
        if (authId == null) {
            throw new BadRequestException();
        }

        tokenExchange.dropToken(authId);
    }

    public static String getTokenUri(UUID authId)
    {
        return TOKEN_ENDPOINT + authId;
    }
}
