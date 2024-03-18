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
package io.trino.plugin.warp.extension.execution.initworker;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static io.trino.plugin.warp.extension.execution.initworker.CollectNodeInfoTask.TASK_NAME;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker
@Path(TASK_NAME)
//@Api(value = "Node-Info", tags = "Node-Info")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CollectNodeInfoTask
        implements TaskResource
{
    public static final String TASK_NAME = "collect-node-info";
    private static final Logger logger = Logger.get(CollectNodeInfoTask.class);
    private final NativeConfiguration nativeConfiguration;

    @Inject
    public CollectNodeInfoTask(NativeConfiguration nativeConfiguration)
    {
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    //@ApiOperation(value = "collectNodeInfo", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public CollectNodeInfoResult collectNodeInfo()
    {
        CollectNodeInfoResult result = new CollectNodeInfoResult(nativeConfiguration.getTaskMaxWorkerThreads());
        logger.debug("finished result=%s", result);
        return result;
    }
}
