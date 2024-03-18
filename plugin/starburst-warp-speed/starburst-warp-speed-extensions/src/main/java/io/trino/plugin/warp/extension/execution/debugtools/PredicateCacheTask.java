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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.trino.plugin.varada.juffer.PredicateBufferPoolType;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.warp.extension.execution.debugtools.PredicateCacheTask.PREDICATES_DUMP;

@TaskResourceMarker(coordinator = false)
@Path(PREDICATES_DUMP)
//@Api(value = "Predicates", tags = "predicates")
public class PredicateCacheTask
        implements TaskResource
{
    public static final String PREDICATES_DUMP = "predicates";

    private final PredicatesCacheService predicatesCacheService;

    @Inject
    public PredicateCacheTask(PredicatesCacheService predicatesCacheService)
    {
        this.predicatesCacheService = predicatesCacheService;
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    //@ApiOperation(value = "predicates", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public PredicateCacheDump dumpPredicateCache()
    {
        Map<PredicateBufferPoolType, Map<Integer, PredicateCacheData>> pool = predicatesCacheService.getPredicateCachePool();
        Map<String, BufferPoolDump> bufferPoolDumpMap = new HashMap<>();
        for (Map.Entry<PredicateBufferPoolType, Map<Integer, PredicateCacheData>> poolTypeEntry : pool.entrySet()) {
            Map<Integer, PredicateCacheData> handles = poolTypeEntry.getValue();
            int totalElements = handles.size();
            long usedElements = handles.values().stream().filter(PredicateCacheData::isUsed).count();
            bufferPoolDumpMap.put(poolTypeEntry.getKey().name(), new BufferPoolDump(totalElements, usedElements));
        }
        return new PredicateCacheDump(bufferPoolDumpMap);
    }

    public record PredicateCacheDump(Map<String, BufferPoolDump> bufferPoolDumpMap)
    {
        @JsonCreator
        public PredicateCacheDump(@JsonProperty("bufferPoolDumpMap") Map<String, BufferPoolDump> bufferPoolDumpMap)
        {
            this.bufferPoolDumpMap = bufferPoolDumpMap;
        }
    }

    public record BufferPoolDump(int totalElements, long usedElements)
    {
        @JsonCreator
        public BufferPoolDump(
                @JsonProperty("totalElements") int totalElements,
                @JsonProperty("usedElements") long usedElements)
        {
            this.totalElements = totalElements;
            this.usedElements = usedElements;
        }
    }
}
