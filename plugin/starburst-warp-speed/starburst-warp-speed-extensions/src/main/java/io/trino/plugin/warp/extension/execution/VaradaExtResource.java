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
package io.trino.plugin.warp.extension.execution;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.connectors.ConnectorTaskExecutor;
import io.trino.spi.TrinoException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;

@Singleton
@Path("v1/ext/")
public class VaradaExtResource
{
    private static final Logger logger = Logger.get(VaradaExtResource.class);

    private final ConnectorTaskExecutor connectorTaskExecutor;

    @Inject
    public VaradaExtResource(ConnectorTaskExecutor connectorTaskExecutor)
    {
        this.connectorTaskExecutor = requireNonNull(connectorTaskExecutor);
    }

    @POST
    @Path("{catalog}/{task}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response executeConnectorTask(@PathParam("catalog") String catalogName, @PathParam("task") String taskName, String taskJsonData)
    {
        try {
            return Response.ok(connectorTaskExecutor.executeTask(taskName, taskJsonData, HttpMethod.POST)).header("Access-Control-Allow-Origin", "*").build();
        }
        catch (TrinoException pe) {
            logger.warn(pe, "failed to execute connector task %s", taskJsonData);
            throw pe;
        }
        catch (Exception e) {
            logger.warn(e, "failed to execute connector task %s", taskJsonData);
            throw new RuntimeException(String.format("failed to execute connector task %s", taskJsonData), e);
        }
    }

    @OPTIONS
    @Path("{catalog}/{task}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response options(@PathParam("catalog") String catalogName, @PathParam("task") String taskName, String taskJsonData)
    {
        return Response.ok().header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "POST")
                .header("Access-Control-Allow-Headers", "*")
                .build();
    }
}
