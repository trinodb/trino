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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.server.ExternalUriInfo;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.CookieParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.Response;

import java.net.URLEncoder;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.oauth2.NonceCookie.NONCE_COOKIE;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static jakarta.ws.rs.core.MediaType.TEXT_HTML;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Path(CALLBACK_ENDPOINT)
@ResourceSecurity(PUBLIC)
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

    @GET
    @Produces(TEXT_HTML)
    public Response callback(
            @QueryParam("state") String state,
            @QueryParam("code") String code,
            @QueryParam("error") String error,
            @QueryParam("error_description") String errorDescription,
            @QueryParam("error_uri") String errorUri,
            @CookieParam(NONCE_COOKIE) Cookie nonce,
            @BeanParam ExternalUriInfo externalUriInfo)
    {
        if (error != null) {
            return service.handleOAuth2Error(state, URLEncoder.encode(error, UTF_8), errorDescription, errorUri);
        }

        try {
            requireNonNull(state, "state is null");
            requireNonNull(code, "code is null");
            return service.finishOAuth2Challenge(state, code, externalUriInfo, NonceCookie.read(nonce));
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
