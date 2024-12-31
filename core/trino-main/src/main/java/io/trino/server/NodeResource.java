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

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.trino.failuredetector.HeartbeatFailureDetector;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

import java.util.Collection;

import static com.google.common.base.Predicates.in;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;

@Path("/v1/node")
@ResourceSecurity(MANAGEMENT_READ)
public class NodeResource
{
    private final HeartbeatFailureDetector failureDetector;

    @Inject
    public NodeResource(HeartbeatFailureDetector failureDetector)
    {
        this.failureDetector = failureDetector;
    }

    @GET
    public Collection<HeartbeatFailureDetector.Stats> getNodeStats()
    {
        return failureDetector.getStats().values();
    }

    @GET
    @Path("failed")
    public Collection<HeartbeatFailureDetector.Stats> getFailed()
    {
        return Maps.filterKeys(failureDetector.getStats(), in(failureDetector.getFailed())).values();
    }
}
