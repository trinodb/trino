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
package io.trino.plugin.warp.extension.execution.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.util.json.SliceSerializer;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

@TaskResourceMarker(coordinator = false)
//@Api(value = "Row Group Dump", tags = "Row Group")
@Consumes(MediaType.APPLICATION_JSON)
@Path("")
@Produces(MediaType.APPLICATION_JSON)
public class RowGroupDataDumpTask
        implements TaskResource
{
    public static final String CACHED_ROW_GROUP = "cached-row-groups";
    public static final String CACHED_SHARED_ROW_GROUP = "cached-shared-row-groups";
    private static final Logger logger = Logger.get(RowGroupDataDumpTask.class);
    private final RowGroupDataService rowGroupDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public RowGroupDataDumpTask(RowGroupDataService rowGroupDataService)
    {
        this.rowGroupDataService = rowGroupDataService;
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(Slice.class, new SliceSerializer());
        objectMapper.registerModules(simpleModule);
    }

    @Audit
    @GET
    @Path(CACHED_ROW_GROUP)
    //@ApiOperation(value = "dump row groups", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public String dumpRowGroupDatas()
    {
        try {
            List<RowGroupData> all = rowGroupDataService.getAll();
            return objectMapper.writeValueAsString(all);
        }
        catch (Exception e) {
            logger.error(e, "error when getting all RowGroupData");
        }
        return null;
    }

    @GET
    @Path(CACHED_SHARED_ROW_GROUP)
    //@ApiOperation(value = "dump shared row groups", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public String dumpSharedRowGroupDatas()
    {
        try {
            List<RowGroupData> all = rowGroupDataService.getAll();
            return objectMapper.writeValueAsString(all);
        }
        catch (Exception e) {
            logger.error(e, "error when getting all shared RowGroupData");
        }
        return null;
    }
}
