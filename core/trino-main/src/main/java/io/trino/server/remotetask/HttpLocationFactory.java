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
package io.trino.server.remotetask;

import com.google.inject.Inject;
import io.airlift.http.server.HttpServerInfo;
import io.trino.execution.LocationFactory;
import io.trino.execution.TaskId;
import io.trino.node.InternalNode;
import io.trino.server.InternalCommunicationConfig;
import io.trino.spi.QueryId;

import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

public class HttpLocationFactory
        implements LocationFactory
{
    private final InternalNode currentNode;
    private final URI baseUri;

    @Inject
    public HttpLocationFactory(InternalNode currentNode, HttpServerInfo httpServerInfo, InternalCommunicationConfig config)
    {
        this(currentNode, config.isHttpsRequired() ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri());
    }

    public HttpLocationFactory(InternalNode currentNode, URI baseUri)
    {
        this.currentNode = currentNode;
        this.baseUri = requireNonNull(baseUri, "baseUri is null");
    }

    @Override
    public URI createQueryLocation(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        return uriBuilderFrom(baseUri)
                .appendPath("/v1/query")
                .appendPath(queryId.toString())
                .build();
    }

    @Override
    public URI createLocalTaskLocation(TaskId taskId)
    {
        return createTaskLocation(currentNode, taskId);
    }

    @Override
    public URI createTaskLocation(InternalNode node, TaskId taskId)
    {
        requireNonNull(node, "node is null");
        requireNonNull(taskId, "taskId is null");
        return uriBuilderFrom(node.getInternalUri())
                .appendPath("/v1/task")
                .appendPath(taskId.toString())
                .build();
    }

    @Override
    public URI createMemoryInfoLocation(InternalNode node)
    {
        requireNonNull(node, "node is null");
        return uriBuilderFrom(node.getInternalUri())
                .appendPath("/v1/memory").build();
    }
}
