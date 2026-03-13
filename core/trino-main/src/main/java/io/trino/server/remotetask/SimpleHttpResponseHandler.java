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

import com.google.common.util.concurrent.FutureCallback;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.JsonResponse;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.concurrent.RejectedExecutionException;

import static io.trino.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SimpleHttpResponseHandler<T>
        implements FutureCallback<JsonResponse<T>>
{
    private final SimpleHttpResponseCallback<T> callback;

    private final URI uri;
    private final RemoteTaskStats stats;

    public SimpleHttpResponseHandler(SimpleHttpResponseCallback<T> callback, URI uri, RemoteTaskStats stats)
    {
        this.callback = callback;
        this.uri = uri;
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void onSuccess(JsonResponse<T> response)
    {
        if (response.statusCode() == HttpStatus.SERVICE_UNAVAILABLE.code()) {
            onFailure(new ServiceUnavailableException(uri));
            return;
        }

        if (response.exception().isPresent() && response.exception().stream().anyMatch(RejectedExecutionException.class::isInstance)) {
            callback.fatal(new TrinoException(REMOTE_TASK_ERROR, format("Unexpected response from %s", uri), response.exception().orElseThrow()));
            return;
        }

        switch (response) {
            case JsonResponse.JsonValue<T> jsonResponse -> {
                try {
                    if (jsonResponse.statusCode() == HttpStatus.OK.code()) {
                        stats.updateSuccess();
                        stats.responseSize(jsonResponse.bytesRead());
                        callback.success(response.jsonValue());
                    }
                    else {
                        callback.fatal(new TrinoException(REMOTE_TASK_ERROR, format("Unexpected response from %s", uri)));
                    }
                }
                catch (Throwable t) {
                    // this should never happen
                    callback.fatal(t);
                }
            }
            case JsonResponse.Exception<T> exceptionResponse -> {
                // Something is broken in the server or the client, so fail the task immediately (includes 500 errors)
                Throwable cause = exceptionResponse.throwable();
                if (cause == null) {
                    if (exceptionResponse.statusCode() == HttpStatus.OK.code()) {
                        cause = new TrinoException(REMOTE_TASK_ERROR, format("Expected response from %s is empty", uri));
                    }
                    else {
                        cause = new TrinoException(REMOTE_TASK_ERROR, format("Expected response code from %s to be %s, but was %s%n",
                                uri,
                                HttpStatus.OK.code(),
                                exceptionResponse.statusCode()));
                    }
                }
                else {
                    cause = new TrinoException(REMOTE_TASK_ERROR, format("Unexpected response from %s", uri), cause);
                }
                callback.failed(cause);
            }
            case JsonResponse.NonJsonBytes<T> nonJsonResponse -> callback.fatal(new TrinoException(REMOTE_TASK_ERROR, format("Expected response code from %s to be %s, but was %s%n%s",
                    uri,
                    HttpStatus.OK.code(),
                    nonJsonResponse.statusCode(),
                    nonJsonResponse.stringValue())));
        }
    }

    @Override
    public void onFailure(Throwable t)
    {
        stats.updateFailure();
        callback.failed(t);
    }

    private static class ServiceUnavailableException
            extends RuntimeException
    {
        public ServiceUnavailableException(URI uri)
        {
            super("Server returned SERVICE_UNAVAILABLE: " + uri);
        }
    }
}
