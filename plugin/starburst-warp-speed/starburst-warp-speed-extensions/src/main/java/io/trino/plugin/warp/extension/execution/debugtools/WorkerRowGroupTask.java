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

import com.google.inject.Inject;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.execution.debugtools.DebugToolResult;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.warp.extension.execution.debugtools.WorkerRowGroupTask.WORKER_ROW_GROUP_PATH;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(coordinator = false)
@Path(WORKER_ROW_GROUP_PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WorkerRowGroupTask
        implements TaskResource
{
    public static final String WORKER_ROW_GROUP_PATH = "worker-row-group";
    public static final String WORKER_ROW_GROUP_COUNT_TASK_NAME = "worker-row-group-count";
    public static final String WORKER_ROW_GROUP_COUNT_WITH_FILES_TASK_NAME = "worker-row-group-count-files";
    public static final String WORKER_ROW_GROUP_RESET_TASK_NAME = "worker-row-group-reset";
    public static final String WORKER_ROW_GROUP_INVALIDATE_TASK_NAME = "worker-row-group-invalidate";
    public static final String WORKER_ROW_GROUP_COLLECT_TASK_NAME = "worker-row-group-collect";

    private final RowGroupDataService rowGroupDataService;
    private final DictionaryCacheService dictionaryCacheService;

    @Inject
    public WorkerRowGroupTask(RowGroupDataService rowGroupDataService,
            DictionaryCacheService dictionaryCacheService)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
    }

    @Path(WORKER_ROW_GROUP_COUNT_TASK_NAME)
    @GET
    public WorkerRowGroupCountResult count()
    {
        return innerCount(false);
    }

    @GET
    @Path(WORKER_ROW_GROUP_COUNT_WITH_FILES_TASK_NAME)
    public WorkerRowGroupCountResult countWithFiles()
    {
        return innerCount(true);
    }

    private WorkerRowGroupCountResult innerCount(boolean isRowGroupFilePath)
    {
        Set<String> columnNames = new HashSet<>();
        Set<String> rowGroupFilePathSet = new HashSet<>();
        AtomicInteger count = new AtomicInteger();
        Map<String, Integer> colNameToCount = new HashMap<>();

        rowGroupDataService.getAll()
                .forEach(rowGroupData -> rowGroupData.getWarmUpElements().stream()
                        .filter(WarmUpElement::isHot)
                        .forEach(warmUpElement -> {
                            RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
                            VaradaColumn varadaColumn = warmUpElement.getVaradaColumn();
                            String name;
                            if (varadaColumn instanceof TransformedColumn transformedColumn) {
                                name = transformedColumn.getTransformedName();
                            }
                            else {
                                name = varadaColumn.getName();
                            }
                            String key = rowGroupKey.schema() + "." + rowGroupKey.table() + "." + name + "." + warmUpElement.getWarmUpType();
                            columnNames.add(key);
                            count.incrementAndGet();
                            colNameToCount.putIfAbsent(key, 0);
                            colNameToCount.put(key, colNameToCount.get(key) + 1);
                            if (isRowGroupFilePath) {
                                rowGroupFilePathSet.add(rowGroupKey.filePath());
                            }
                        }));
        return new WorkerRowGroupCountResult(count.get(), columnNames, colNameToCount, rowGroupFilePathSet);
    }

    @POST
    @Path(WORKER_ROW_GROUP_RESET_TASK_NAME)
    //HACK HACK HACK
    public void reset()
    {
        rowGroupDataService.deleteAll();
        dictionaryCacheService.reset();
    }

    @POST
    @Path(WORKER_ROW_GROUP_INVALIDATE_TASK_NAME)
    //HACK HACK HACK
    public void invalidate()
    {
        rowGroupDataService.invalidateAll();
    }

    @POST
    @Path(WORKER_ROW_GROUP_COLLECT_TASK_NAME)
    public List<DebugToolResult> collect(RowGroupCollectData rowGroupCollectData)
    {
        return List.of();
    }
}
