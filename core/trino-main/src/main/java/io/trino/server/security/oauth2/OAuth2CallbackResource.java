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

import javax.inject.Inject;
import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.oauth2.NonceCookie.NONCE_COOKIE;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_HTML;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path(CALLBACK_ENDPOINT)
public class OAuth2CallbackResource
{
    private static final Logger LOG = Logger.get(OAuth2CallbackResource.class);

    public static final String CALLBACK_ENDPOINT = "/oauth2/callback";

    private final OAuth2Service service;

    @Inject
    public OAuth2CallbackResource(OAuth2Service service)
    {
        this.service = requireNonNull(service, "service is null");
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
        if (error != null) {
            return service.handleOAuth2Error(state, error, errorDescription, errorUri);
        }

        try {
            requireNonNull(state, "state is null");
            requireNonNull(code, "code is null");
            return service.finishOAuth2Challenge(state, code, uriInfo.getBaseUri().resolve(CALLBACK_ENDPOINT), NonceCookie.read(nonce));
        }
        catch (RuntimeException e) {
            LOG.debug(e, "Authentication response could not be verified: state=%s", state);
            return Response.status(BAD_REQUEST)
                    .cookie(NonceCookie.delete())
                    .entity(service.getInternalFailureHtml("Authentication response could not be verified"))
                    .build();
        }
    }
}
