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
package io.trino.plugin.warp.extension.execution.upgrade;

import com.google.inject.Inject;
import io.trino.plugin.varada.api.ClusterVersion;
import io.trino.plugin.varada.dispatcher.warmup.WarmUtils;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.NodeManager;
import io.varada.tools.util.Version;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path(GetVersionTask.TASK_NAME)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class GetVersionTask
        implements TaskResource
{
    public static final String TASK_NAME = "get-version";

    private final NodeManager nodeManager;

    @Inject
    public GetVersionTask(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager);
    }

    @GET
    public ClusterVersion getVersion()
    {
        String prestoVersion = nodeManager.getCurrentNode().getVersion();
        String varadaVersion = Version.getInstance().getVersion();
        int fastWarmingVersion = WarmUtils.FAST_WARMING_VERSION;
        return new ClusterVersion(prestoVersion, varadaVersion, fastWarmingVersion);
    }
}
