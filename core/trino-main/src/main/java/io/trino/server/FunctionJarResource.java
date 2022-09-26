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
package io.trino.server;

import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.trino.metadata.FunctionJarDynamicManager;
import io.trino.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/jar")
public class FunctionJarResource
{
    private final FunctionJarDynamicManager functionJarDynamicManager;

    @Inject
    public FunctionJarResource(FunctionJarDynamicManager functionJarDynamicManager)
    {
        this.functionJarDynamicManager = requireNonNull(functionJarDynamicManager, "functionManager is null");
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("{jarUrl}/{notExists}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public boolean createOrUpdateJar(
            @PathParam("jarUrl") String jarUrl,
            @PathParam("notExists") boolean notExists,
            @Suspended AsyncResponse asyncResponse)
            throws UnsupportedEncodingException
    {
        requireNonNull(jarUrl, "jarUrl is null");
        String fullUrl = URLDecoder.decode(jarUrl, "UTF-8");
        try {
            functionJarDynamicManager.addJar(fullUrl, notExists);
        }
        catch (Exception e) {
            return asyncResponse.resume(Response.ok().entity(e).build());
        }

        return asyncResponse.resume(Response.ok().entity(jarUrl).build());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @DELETE
    @Path("{jarUrl}/{exists}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public boolean deleteTask(
            @PathParam("jarUrl") String jarUrl,
            @PathParam("exists") boolean exists,
            @Suspended AsyncResponse asyncResponse)
            throws UnsupportedEncodingException
    {
        requireNonNull(jarUrl, "jarUrl is null");
        String fullUrl = URLDecoder.decode(jarUrl, "UTF-8");
        try {
            functionJarDynamicManager.dropJar(fullUrl, exists);
        }
        catch (Exception e) {
            return asyncResponse.resume(Response.ok().entity(e).build());
        }

        return asyncResponse.resume(Response.ok().entity(jarUrl).build());
    }

    public static class StreamingJsonResponseHandler
            implements ResponseHandler<String, RuntimeException>
    {
        private final StringResponseHandler handler = createStringResponseHandler();

        @Override
        public String handleException(Request request, Exception exception)
        {
            throw new RuntimeException("Request to worker failed", exception);
        }

        @Override
        public String handle(Request request, io.airlift.http.client.Response response)
        {
            if (!TEXT_PLAIN.equals(response.getHeader(CONTENT_TYPE))) {
                throw new RuntimeException("Response received was not of type " + TEXT_PLAIN + ", response detail:" + response + "request detail:" + request.getUri().toString());
            }
            return handler.handle(request, response).getBody();
        }
    }
}
