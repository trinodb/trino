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
package io.trino.plugin.warp.extension.execution.callhome;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Singleton
@Path(CallHomeResource.CALL_HOME_PATH)
////@Api(value = "Call Home", tags = "Call Home")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@TaskResourceMarker
public class CallHomeResource
        implements TaskResource
{
    public static final String CALL_HOME_PATH = "call-home";

    private final CallHomeService callHomeService;

    @Inject
    public CallHomeResource(CallHomeService callHomeService)
    {
        this.callHomeService = requireNonNull(callHomeService);
    }

    @POST
    @Audit
    ////@ApiOperation(value = "call-home", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map<String, Object> triggerCallHome(CallHomeData callHomeData)
    {
        Optional<Integer> optionalResult = callHomeService.triggerCallHome(
                callHomeData.storePath(),
                callHomeData.collectThreadDumps(),
                callHomeData.waitToFinish());

        return Map.of("numberOfUploaded", optionalResult.orElse(0));
    }
}
