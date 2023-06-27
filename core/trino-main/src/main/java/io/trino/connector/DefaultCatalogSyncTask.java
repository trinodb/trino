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
package io.trino.connector;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.metadata.ForNodeManager;
import io.trino.server.InternalCommunicationConfig;
import io.trino.transaction.TransactionManager;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;

public class DefaultCatalogSyncTask
        implements CatalogSyncTask
{
    private static final Logger log = Logger.get(DefaultCatalogSyncTask.class);
    private static final JsonCodec<List<CatalogProperties>> CATALOG_PROPERTIES_CODEC = listJsonCodec(CatalogProperties.class);

    private final TransactionManager transactionManager;
    private final CoordinatorDynamicCatalogManager catalogManager;
    private final NodeInfo nodeInfo;
    private final ServiceSelector selector;
    private final HttpClient httpClient;

    private final boolean httpsRequired;

    @Inject
    public DefaultCatalogSyncTask(
            TransactionManager transactionManager,
            CoordinatorDynamicCatalogManager catalogManager,
            NodeInfo nodeInfo,
            @ServiceType("trino") ServiceSelector selector,
            @ForNodeManager HttpClient httpClient,
            InternalCommunicationConfig internalCommunicationConfig)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.selector = requireNonNull(selector, "selector is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.httpsRequired = internalCommunicationConfig.isHttpsRequired();
    }

    @Override
    public void syncCatalogs(List<CatalogProperties> catalogsInCoordinator)
    {
        syncWorkerCatalogs(catalogsInCoordinator);
    }

    @VisibleForTesting
    void syncWorkerCatalogs(List<CatalogProperties> catalogsInCoordinator)
    {
        Set<ServiceDescriptor> online = selector.selectAllServices().stream()
                .filter(descriptor -> !nodeInfo.getNodeId().equals(descriptor.getNodeId()))
                .collect(toImmutableSet());
        log.info("syncWorkerCatalogs online: " + online);

        // send message to workers to trigger sync
        for (ServiceDescriptor service : online) {
            URI uri = getHttpUri(service);
            if (uri == null) {
                continue;
            }
            uri = uriBuilderFrom(uri).appendPath("/v1/task/syncCatalogs").build();
            Request request = preparePost()
                    .setUri(uri)
                    .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .setBodyGenerator(jsonBodyGenerator(CATALOG_PROPERTIES_CODEC, catalogsInCoordinator))
                    .build();
            httpClient.executeAsync(request, new ResponseHandler<>()
            {
                @Override
                public Exception handleException(Request request, Exception exception)
                {
                    log.error(exception, "Error syncing catalogs with request: %s", request);
                    return exception;
                }

                @Override
                public Object handle(Request request, Response response)
                {
                    log.debug("Synced catalogs on server: %s", request.getUri());
                    return null;
                }
            });
        }
    }

    private URI getHttpUri(ServiceDescriptor descriptor)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            try {
                return new URI(url);
            }
            catch (URISyntaxException ignored) {
            }
        }
        return null;
    }
}
