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
package io.presto.eventlog;

import io.airlift.log.Logger;
import io.prestosql.spi.QueryId;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

@Path("/ui/api/eventlog")
public class EventLogResource
{
    private static final Logger logger = Logger.get(EventLogResource.class);
    private final EventLogProcessor eventLogProcessor;

    @Inject
    public EventLogResource(EventLogProcessor eventLogProcessor)
    {
        this.eventLogProcessor = requireNonNull(eventLogProcessor, "eventLogProcessor is null");
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        requireNonNull(queryId, "queryId is null");

        if (eventLogProcessor.isEnableEventLog()) {
            try {
                String eventLog = eventLogProcessor.readQueryInfo(queryId.getId());
                return Response.ok(eventLog).build();
            }
            catch (IOException e) {
                logger.error(e);
            }
        }
        return Response.status(Response.Status.GONE).build();
    }
}
