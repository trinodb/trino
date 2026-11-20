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

import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Optional;

import static io.trino.spi.HostAddress.fromUri;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.trino.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * A request was rejected, because it was addressed to a server instance which is no longer there.
 */
public class NodeInstanceMismatchException
        extends TrinoException
{
    private final Optional<String> actualNodeInstanceId;

    public NodeInstanceMismatchException(URI uri, Optional<String> actualNodeInstanceId)
    {
        super(REMOTE_TASK_MISMATCH, "%s (%s)".formatted(REMOTE_TASK_MISMATCH_ERROR, fromUri(uri)));
        this.actualNodeInstanceId = requireNonNull(actualNodeInstanceId, "actualNodeInstanceId is null");
    }

    /**
     * The instance of the server which rejected the request, if it reported it.
     */
    public Optional<String> actualNodeInstanceId()
    {
        return actualNodeInstanceId;
    }
}
