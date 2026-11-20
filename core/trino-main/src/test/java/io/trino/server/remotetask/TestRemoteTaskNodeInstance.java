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

import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Optional;

import static io.airlift.http.client.HttpStatus.SERVICE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoteTaskNodeInstance
{
    private static final TaskId TASK_ID = new TaskId(new StageId("query", 1), 0, 0);
    private static final URI TASK_URI = URI.create("http://worker:8080/v1/task/query.1.0.0");

    @Test
    public void testRedirectWhenTaskUpdateNeverDelivered()
    {
        RemoteTaskNodeInstance nodeInstance = new RemoteTaskNodeInstance(TASK_ID, "old");
        // the worker was down
        nodeInstance.taskUpdateFailed(new UncheckedIOException("Failed to connect", new ConnectException("Connection refused")));
        // the restarted worker was still starting
        nodeInstance.taskUpdateFailed(new RetryableResponseException(SERVICE_UNAVAILABLE, TASK_URI));
        // the restarted worker rejected the task update
        nodeInstance.taskUpdateFailed(new NodeInstanceMismatchException(TASK_URI, Optional.of("new")));

        // other requests wait for the task update to decide
        assertThat(nodeInstance.canRetryRejectedRequest("old")).isTrue();

        assertThat(nodeInstance.redirectTaskUpdate(Optional.of("new"))).isTrue();
        assertThat(nodeInstance.nodeInstanceId()).isEqualTo("new");
        // requests sent before the redirect are retried
        assertThat(nodeInstance.canRetryRejectedRequest("old")).isTrue();
    }

    @Test
    public void testNoRedirectAfterTaskUpdateSucceeded()
    {
        RemoteTaskNodeInstance nodeInstance = new RemoteTaskNodeInstance(TASK_ID, "old");
        nodeInstance.taskUpdateSucceeded();

        assertThat(nodeInstance.canRetryRejectedRequest("old")).isFalse();
        assertThat(nodeInstance.redirectTaskUpdate(Optional.of("new"))).isFalse();
        assertThat(nodeInstance.nodeInstanceId()).isEqualTo("old");
    }

    @Test
    public void testNoRedirectWhenTaskUpdatePossiblyDelivered()
    {
        assertNoRedirectAfterFailure(new SocketTimeoutException("Read timed out"));
        assertNoRedirectAfterFailure(new RuntimeException("connection reset"));
    }

    @Test
    public void testNoRedirectWhenActualInstanceUnknown()
    {
        RemoteTaskNodeInstance nodeInstance = new RemoteTaskNodeInstance(TASK_ID, "old");

        assertThat(nodeInstance.redirectTaskUpdate(Optional.empty())).isFalse();
        assertThat(nodeInstance.nodeInstanceId()).isEqualTo("old");
    }

    private static void assertNoRedirectAfterFailure(Throwable failure)
    {
        RemoteTaskNodeInstance nodeInstance = new RemoteTaskNodeInstance(TASK_ID, "old");
        nodeInstance.taskUpdateFailed(failure);

        assertThat(nodeInstance.canRetryRejectedRequest("old")).isFalse();
        assertThat(nodeInstance.redirectTaskUpdate(Optional.of("new"))).isFalse();
        assertThat(nodeInstance.nodeInstanceId()).isEqualTo("old");
    }
}
