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

import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.trino.execution.SqlTask;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.operator.ForExchange;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpStatus.PRECONDITION_FAILED;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.server.InternalHeaders.TRINO_NODE_INSTANCE_ID_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestTaskResource
{
    private TestingTrinoServer server;
    private HttpClient httpClient;

    @BeforeAll
    void setup()
    {
        server = TestingTrinoServer.create();
        // internal client, authenticated for the internal-only task resource
        httpClient = server.getInstance(Key.get(HttpClient.class, ForExchange.class));
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        server.close();
        server = null;
        httpClient = null;
    }

    @Test
    void testRequestToDifferentNodeInstance()
    {
        TaskId taskId = new TaskId(new StageId("different_node_instance", 0), 0, 0);

        StatusResponse response = getTaskStatus(taskId, Optional.of("other-" + server.getCurrentNode().getInstanceId()));
        assertThat(response.getStatusCode()).isEqualTo(PRECONDITION_FAILED.code());
        // the caller learns the current instance, so it can redirect a task which never reached the previous one
        assertThat(response.getHeader(TRINO_NODE_INSTANCE_ID_HEADER)).contains(server.getCurrentNode().getInstanceId());
        // the task must not be created on demand
        assertThat(server.getTaskManager().getAllTasks())
                .extracting(SqlTask::getTaskId)
                .doesNotContain(taskId);
    }

    @Test
    void testRequestToSameNodeInstance()
    {
        TaskId taskId = new TaskId(new StageId("same_node_instance", 0), 0, 0);

        assertThat(getTaskStatus(taskId, Optional.of(server.getCurrentNode().getInstanceId())).getStatusCode())
                .isEqualTo(OK.code());
        assertThat(server.getTaskManager().getAllTasks())
                .extracting(SqlTask::getTaskId)
                .contains(taskId);
    }

    @Test
    void testRequestWithoutNodeInstance()
    {
        TaskId taskId = new TaskId(new StageId("no_node_instance", 0), 0, 0);

        assertThat(getTaskStatus(taskId, Optional.empty()).getStatusCode())
                .isEqualTo(OK.code());
        assertThat(server.getTaskManager().getAllTasks())
                .extracting(SqlTask::getTaskId)
                .contains(taskId);
    }

    @Test
    void testResultsRequestToDifferentNodeInstance()
    {
        TaskId taskId = new TaskId(new StageId("results_different_node_instance", 0), 0, 0);

        assertThat(get(taskId, "results/0/0", Optional.of("other-" + server.getCurrentNode().getInstanceId())).getStatusCode())
                .isEqualTo(PRECONDITION_FAILED.code());
        // the task must not be created on demand
        assertThat(server.getTaskManager().getAllTasks())
                .extracting(SqlTask::getTaskId)
                .doesNotContain(taskId);
    }

    private StatusResponse getTaskStatus(TaskId taskId, Optional<String> nodeInstanceId)
    {
        return get(taskId, "status", nodeInstanceId);
    }

    private StatusResponse get(TaskId taskId, String path, Optional<String> nodeInstanceId)
    {
        Request.Builder request = prepareGet()
                .setUri(uriBuilderFrom(server.getBaseUrl()).appendPath("/v1/task").appendPath(taskId.toString()).appendPath(path).build());
        nodeInstanceId.ifPresent(instanceId -> request.setHeader(TRINO_NODE_INSTANCE_ID_HEADER, instanceId));
        return httpClient.execute(request.build(), createStatusResponseHandler());
    }
}
