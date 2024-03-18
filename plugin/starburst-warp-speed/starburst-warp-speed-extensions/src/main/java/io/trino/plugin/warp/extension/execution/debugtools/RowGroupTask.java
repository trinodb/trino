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
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.Node;
import io.varada.annotation.Audit;
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

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.plugin.varada.execution.VaradaClient.VOID_RESULTS_CODEC;
import static io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask.ROW_GROUP_PATH;
import static io.trino.plugin.warp.extension.execution.debugtools.WorkerRowGroupTask.WORKER_ROW_GROUP_COLLECT_TASK_NAME;
import static io.trino.plugin.warp.extension.execution.debugtools.WorkerRowGroupTask.WORKER_ROW_GROUP_PATH;
import static io.trino.plugin.warp.extension.execution.debugtools.WorkerRowGroupTask.WORKER_ROW_GROUP_RESET_TASK_NAME;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path(ROW_GROUP_PATH)
//@Api(value = "Row Group", tags = "Row Group")
public class RowGroupTask
        implements TaskResource
{
    public static final String ROW_GROUP_PATH = "row-group";
    public static final String ROW_GROUP_COUNT_TASK_NAME = "row-group-count";
    public static final String ROW_GROUP_COUNT_WITH_FILES_TASK_NAME = "row-group-count-files";
    public static final String ROW_GROUP_RESET_TASK_NAME = "row-group-reset";
    public static final String ROW_GROUP_COLLECT_TASK_NAME = "row-group-collect";
    private static final JsonCodec<WorkerRowGroupCountResult> workerRowGroupCountResultJsonCoded = JsonCodec.jsonCodec(WorkerRowGroupCountResult.class);
    private static final JsonCodec<RowGroupCollectData> workerRowGroupCollectJsonCodec = JsonCodec.jsonCodec(RowGroupCollectData.class);
    private static final JsonCodec<List> workerRowGroupCollectResultJsonCoded = JsonCodec.jsonCodec(List.class);

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final VaradaClient varadaClient;

    @Inject
    public RowGroupTask(CoordinatorNodeManager coordinatorNodeManager,
            VaradaClient varadaClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.varadaClient = requireNonNull(varadaClient);
    }

    @GET
    @Path(ROW_GROUP_COUNT_TASK_NAME)
    @Audit
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    //@ApiOperation(value = "count", nickname = "rowGroupCount", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public RowGroupCountResult count()
    {
        return innerCount(false);
    }

    @GET
    @Path(ROW_GROUP_COUNT_WITH_FILES_TASK_NAME)
    @Audit
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    //@ApiOperation(value = "count-with-files", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public RowGroupCountResult countWithFiles()
    {
        return innerCount(true);
    }

    @SuppressWarnings("deprecation")
    private RowGroupCountResult innerCount(boolean isRowGroupFilePath)
    {
        Map<String, Long> deviceGroupsRowGroupsCount = new HashMap<>();
        Map<String, Map<String, Integer>> allRes = new HashMap<>();
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        Set<String> columnNames = new HashSet<>();
        Map<String, Set<String>> rowGroupFilePathSet = new HashMap<>();
        workerNodes.forEach(workerNode -> {
            HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
            uriBuilder.appendPath(WORKER_ROW_GROUP_PATH);
            uriBuilder.appendPath(isRowGroupFilePath ? WorkerRowGroupTask.WORKER_ROW_GROUP_COUNT_WITH_FILES_TASK_NAME : WorkerRowGroupTask.WORKER_ROW_GROUP_COUNT_TASK_NAME);

            Request request = prepareGet()
                    .setUri(uriBuilder.build())
                    .setHeader("Content-Type", "application/json")
                    .build();
            WorkerRowGroupCountResult nodeResult = varadaClient.sendWithRetry(request, createFullJsonResponseHandler(workerRowGroupCountResultJsonCoded));
            long nodeCount = nodeResult.count();
            columnNames.addAll(nodeResult.warmupColumnNames());
            allRes.put(workerNode.getNodeIdentifier(), nodeResult.warmupColumnCount());
            if (isRowGroupFilePath) {
                rowGroupFilePathSet.put(UriUtils.getHttpUri(workerNode).toString(), nodeResult.rowGroupFilePathSet());
            }
            deviceGroupsRowGroupsCount.put(UriUtils.getHttpUri(workerNode).toString(), nodeCount);
        });
        return new RowGroupCountResult(deviceGroupsRowGroupsCount, columnNames, mergeResults(allRes), rowGroupFilePathSet);
    }

    private Map<String, Integer> mergeResults(Map<String, Map<String, Integer>> allRes)
    {
        Map<String, Integer> res = new HashMap<>();
        allRes.forEach((workerIp, workerResMap) ->
                workerResMap.forEach((k, v) -> res.put(String.format("%s:%s", workerIp, k), v)));
        return res;
    }

    @SuppressWarnings("deprecation")
    @POST
    @Path(ROW_GROUP_RESET_TASK_NAME)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Audit
    //@ApiOperation(value = "reset", nickname = "rowGroupReset", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public void reset()
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        workerNodes.forEach(workerNode -> {
            HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
            uriBuilder.appendPath(WORKER_ROW_GROUP_PATH).appendPath(WORKER_ROW_GROUP_RESET_TASK_NAME);

            Request request = preparePost()
                    .setUri(uriBuilder.build())
                    .setHeader("Content-Type", "application/json")
                    .build();
            varadaClient.sendWithRetry(request, createFullJsonResponseHandler(VOID_RESULTS_CODEC));
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked", "deprecation"})
    @GET
    @Path(ROW_GROUP_COLLECT_TASK_NAME)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Audit
    //@ApiOperation(value = "collect", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map collect(RowGroupCollectData rowGroupCollectData)
    {
        Map ret = new HashMap();
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        workerNodes.forEach(node -> {
            HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node));
            uriBuilder.appendPath(WORKER_ROW_GROUP_PATH).appendPath(WORKER_ROW_GROUP_COLLECT_TASK_NAME);
            Request request = preparePost()
                    .setUri(uriBuilder.build())
                    .setBodyGenerator(jsonBodyGenerator(workerRowGroupCollectJsonCodec, rowGroupCollectData))
                    .setHeader("Content-Type", "application/json")
                    .build();
            List nodeResult = varadaClient.sendWithRetry(request, createFullJsonResponseHandler(workerRowGroupCollectResultJsonCoded));
            ret.put(UriUtils.getHttpUri(node).getHost(), nodeResult);
        });
        return ret;
    }
}
