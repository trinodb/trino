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

import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimpleHttpResponseHandler
{
    private static final URI TASK_URI = URI.create("http://worker:8080/v1/task/test");

    @Test
    public void testSuccess()
    {
        RecordingCallback callback = new RecordingCallback();
        handler(callback).onSuccess(new JsonResponse<>(200, ImmutableListMultimap.of(), jsonCodec(String.class), "\"value\"".getBytes(UTF_8)));

        assertThat(callback.value).isEqualTo("value");
        assertThat(callback.failure).isNull();
        assertThat(callback.fatal).isNull();
    }

    @Test
    public void testServiceUnavailableIsRetried()
    {
        RecordingCallback callback = new RecordingCallback();
        handler(callback).onSuccess(new JsonResponse<>(503, ImmutableListMultimap.of(), new byte[0]));

        assertThat(callback.value).isNull();
        assertThat(callback.failure).isInstanceOf(RetryableResponseException.class);
        assertThat(callback.fatal).isNull();
    }

    @Test
    public void testUnexpectedStatusIsFatal()
    {
        RecordingCallback callback = new RecordingCallback();
        handler(callback).onSuccess(new JsonResponse<>(500, ImmutableListMultimap.of(), "HTTP ERROR 500".getBytes(UTF_8)));

        assertThat(callback.value).isNull();
        assertThat(callback.failure).isNull();
        assertThat(callback.fatal)
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(REMOTE_TASK_ERROR.toErrorCode()))
                .hasMessageContaining("but was 500");
    }

    @Test
    public void testEmptyOkResponseIsFatal()
    {
        RecordingCallback callback = new RecordingCallback();
        handler(callback).onSuccess(new JsonResponse<>(200, ImmutableListMultimap.of(), new byte[0]));

        assertThat(callback.value).isNull();
        assertThat(callback.failure).isNull();
        assertThat(callback.fatal)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("is empty");
    }

    @Test
    public void testTransportFailureIsRetried()
    {
        RecordingCallback callback = new RecordingCallback();
        RuntimeException cause = new RuntimeException("connection reset");
        handler(callback).onFailure(cause);

        assertThat(callback.value).isNull();
        assertThat(callback.failure).isSameAs(cause);
        assertThat(callback.fatal).isNull();
    }

    private static SimpleHttpResponseHandler<String> handler(RecordingCallback callback)
    {
        return new SimpleHttpResponseHandler<>(callback, TASK_URI, new RemoteTaskStats());
    }

    private static class RecordingCallback
            implements SimpleHttpResponseCallback<String>
    {
        private String value;
        private Throwable failure;
        private Throwable fatal;

        @Override
        public void success(String value)
        {
            this.value = value;
        }

        @Override
        public void failed(Throwable cause)
        {
            this.failure = cause;
        }

        @Override
        public void fatal(Throwable cause)
        {
            this.fatal = cause;
        }
    }
}
