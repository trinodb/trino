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
package io.trino.plugin.warp.extension.execution.debugtools.dictionary;

import com.google.inject.Inject;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dictionary.DebugDictionaryKey;
import io.trino.plugin.varada.dictionary.DebugDictionaryMetadata;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.Node;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.plugin.warp.extension.execution.debugtools.dictionary.WorkerDictionaryCountTask.WORKER_DICTIONARY_GROUP_PATH;
import static io.trino.plugin.warp.extension.execution.debugtools.dictionary.WorkerDictionaryCountTask.WORKER_DICTIONARY_RESET_TASK_NAME;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path(DictionaryTask.DICTIONARY_PATH)
//@Api(value = "Dictionary", tags = "Dictionary")
public class DictionaryTask
        implements TaskResource
{
    public static final String DICTIONARY_PATH = "dictionary";
    public static final String DICTIONARY_COUNT_TASK_NAME = "dictionary-count";
    public static final String DICTIONARY_USAGE_TASK_NAME = "dictionary-usage";
    public static final String DICTIONARY_RESET_MEMORY_TASK_NAME = "dictionary-memory-reset";
    public static final String DICTIONARY_COUNT_AGGREGATED_TASK_NAME = "dictionary-count-aggregated";
    public static final String DICTIONARY_GET_CONFIGURATION = "dictionary-get-configuration";
    public static final String DICTIONARY_SET_CONFIGURATION_AND_RESET_CACHE = "set-configuration-reset-cache";
    public static final String DICTIONARY_GET_CACHE_KEYS = "dictionary-get-cache-keys";
    private static final JsonCodec<WorkerDictionaryCountResult> workerDictionaryCountResultJsonCoded = JsonCodec.jsonCodec(WorkerDictionaryCountResult.class);
    private static final JsonCodec<DictionaryConfigurationResult> workerDictionaryConfigurationResultJsonCoded = JsonCodec.jsonCodec(DictionaryConfigurationResult.class);
    private static final JsonCodec<DictionaryConfigurationRequest> workerDictionaryConfigurationRequestJsonCoded = JsonCodec.jsonCodec(DictionaryConfigurationRequest.class);
    private static final JsonCodec<Map<String, Object>> mapJsonCoded = JsonCodec.mapJsonCodec(String.class, Object.class);

    private final CoordinatorNodeManager coordinatorNodeManager;
    private final DictionaryConfiguration dictionaryConfiguration;
    private final VaradaClient varadaClient;

    @Inject
    public DictionaryTask(
            CoordinatorNodeManager coordinatorNodeManager,
            DictionaryConfiguration dictionaryConfiguration,
            VaradaClient varadaClient)
    {
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.dictionaryConfiguration = requireNonNull(dictionaryConfiguration);
        this.varadaClient = requireNonNull(varadaClient);
    }

    @POST
    @Path(DICTIONARY_RESET_MEMORY_TASK_NAME)
    //@ApiOperation(value = "reset", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public int resetMemoryDictionaries()
    {
        return resetMemoryDictionaries(new DictionaryConfigurationRequest(dictionaryConfiguration.getMaxDictionaryTotalCacheWeight(), dictionaryConfiguration.getDictionaryCacheConcurrencyLevel()));
    }

    @POST
    @Path(DICTIONARY_COUNT_TASK_NAME)
    //@ApiOperation(value = "count", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public DictionaryCountResult count()
    {
        return innerCount(false);
    }

    @GET
    @Path(DICTIONARY_GET_CONFIGURATION)
    //@ApiOperation(value = "get dictionary configuration", nickname = "dictionaryGetConfiguration", extensions = {
//            @Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map<String, DictionaryConfigurationResult> getDictionaryConfiguration()
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        return workerNodes.stream()
                .collect(Collectors.toMap(Node::getNodeIdentifier, workerNode -> (DictionaryConfigurationResult) getWorkerResult(workerNode, WorkerDictionaryCountTask.WORKER_DICTIONARY_GET_CONFIGURATION, workerDictionaryConfigurationResultJsonCoded)));
    }

    @POST
    @Path(DICTIONARY_SET_CONFIGURATION_AND_RESET_CACHE)
    //@ApiOperation(value = "set dictionary configuration and reset cache", nickname = "dictionarySetConfiguration", extensions = {
//            @Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public int setDictionaryConfiguration(DictionaryConfigurationRequest dictionaryConfiguration)
    {
        long confTotalWeight = dictionaryConfiguration.getMaxDictionaryTotalCacheWeight() > -1 ?
                dictionaryConfiguration.getMaxDictionaryTotalCacheWeight() :
                this.dictionaryConfiguration.getMaxDictionaryTotalCacheWeight();
        int confConcurrency = dictionaryConfiguration.getConcurrency() > -1 ?
                dictionaryConfiguration.getConcurrency() :
                this.dictionaryConfiguration.getDictionaryCacheConcurrencyLevel();
        return resetMemoryDictionaries(new DictionaryConfigurationRequest(confTotalWeight, confConcurrency));
    }

    @GET
    @Path(DICTIONARY_GET_CACHE_KEYS)
    //@ApiOperation(value = "get dictionary cached values", nickname = "dictionaryGetCachedKeys", extensions = {
//            @Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map<String, Map<String, Integer>> getDictionaryCachedKeys()
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        return workerNodes.stream()
                .collect(Collectors.toMap(Node::getNodeIdentifier, workerNode -> (Map<String, Integer>) getWorkerResult(workerNode, WorkerDictionaryCountTask.WORKER_DICTIONARY_GET_CACHED_KEYS, mapJsonCoded)));
    }

    @POST
    @Path(DICTIONARY_COUNT_AGGREGATED_TASK_NAME)
    //@ApiOperation(value = "count aggregate", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public DictionaryCountResult countAggregated()
    {
        return innerCount(true);
    }

    private int resetMemoryDictionaries(DictionaryConfigurationRequest dictionaryConfiguration)
    {
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        AtomicInteger totalRemoved = new AtomicInteger();
        workerNodes.forEach(workerNode -> {
            Integer res = (Integer) postWorkerResult(workerNode,
                    WORKER_DICTIONARY_RESET_TASK_NAME,
                    workerDictionaryConfigurationRequestJsonCoded,
                    dictionaryConfiguration,
                    JsonCodec.jsonCodec(Integer.class));
            totalRemoved.addAndGet(res);
        });
        return totalRemoved.get();
    }

    private DictionaryCountResult innerCount(boolean aggregated)
    {
        List<WorkerDictionaryCountResult> workerDictionaryCountResultList = new ArrayList<>();
        Map<DebugDictionaryKey, DebugDictionaryMetadata> dictionaryNameToDictionaryMetadata = new HashMap<>();
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        workerNodes.forEach(workerNode -> {
            WorkerDictionaryCountResult nodeResult = (WorkerDictionaryCountResult) postWorkerResult(workerNode,
                    WorkerDictionaryCountTask.WORKER_DICTIONARY_PATH,
                    null,
                    null,
                    workerDictionaryCountResultJsonCoded);
            if (aggregated) {
                nodeResult.getDictionaryMetadataList().forEach(dictionaryMetadata ->
                        dictionaryNameToDictionaryMetadata.merge(dictionaryMetadata.dictionaryKey(),
                                dictionaryMetadata,
                                (v1, v2) -> new DebugDictionaryMetadata(v1.dictionaryKey(), v1.dictionarySize() + v2.dictionarySize(), v1.failedWriteCount() + v2.failedWriteCount())));
            }
            else {
                workerDictionaryCountResultList.add(nodeResult);
            }
        });
        DictionaryCountResult result;
        if (aggregated) {
            WorkerDictionaryCountResult aggregated1 = new WorkerDictionaryCountResult(new ArrayList<>(dictionaryNameToDictionaryMetadata.values()), null);
            result = new DictionaryCountResult(List.of(aggregated1));
        }
        else {
            result = new DictionaryCountResult(workerDictionaryCountResultList);
        }
        return result;
    }

    @GET
    @Path(DICTIONARY_USAGE_TASK_NAME)
    //@ApiOperation(value = "count", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public long sumUsage()
    {
        AtomicInteger totalUsage = new AtomicInteger();
        List<Node> workerNodes = coordinatorNodeManager.getWorkerNodes();
        workerNodes.forEach(workerNode -> {
            HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
            uriBuilder.appendPath(WORKER_DICTIONARY_GROUP_PATH);
            uriBuilder.appendPath(WorkerDictionaryCountTask.WORKER_DICTIONARY_USAGE_PATH);

            Request request = prepareGet()
                    .setUri(uriBuilder.build())
                    .setHeader("Content-Type", "application/json")
                    .build();
            int workerUsedPages = varadaClient.sendWithRetry(request, createFullJsonResponseHandler(JsonCodec.jsonCodec(Integer.class)));
            totalUsage.addAndGet(workerUsedPages);
        });
        return totalUsage.get();
    }

    private Object postWorkerResult(Node workerNode, String workerTaskPath, JsonCodec jsonCodecRequest, Object input, JsonCodec jsonCodecResponse)
    {
        HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
        uriBuilder.appendPath(WORKER_DICTIONARY_GROUP_PATH);
        uriBuilder.appendPath(workerTaskPath);

        Request.Builder builder = preparePost()
                .setUri(uriBuilder.build())
                .setHeader("Content-Type", "application/json");
        if (input != null && jsonCodecRequest != null) {
            builder.setBodyGenerator(jsonBodyGenerator(jsonCodecRequest, input));
        }
        return varadaClient.sendWithRetry(builder.build(), createFullJsonResponseHandler(jsonCodecResponse));
    }

    private Object getWorkerResult(Node workerNode, String workerTaskPath, JsonCodec jsonCodecResponse)
    {
        HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
        uriBuilder.appendPath(WORKER_DICTIONARY_GROUP_PATH);
        uriBuilder.appendPath(workerTaskPath);

        Request.Builder builder = prepareGet()
                .setUri(uriBuilder.build())
                .setHeader("Content-Type", "application/json");
        return varadaClient.sendWithRetry(builder.build(), createFullJsonResponseHandler(jsonCodecResponse));
    }
}
