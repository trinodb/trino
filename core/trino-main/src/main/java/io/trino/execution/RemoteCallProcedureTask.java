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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.node.InternalNode;
import io.trino.server.ForCallProcedure;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.util.Objects.requireNonNull;

public class RemoteCallProcedureTask
{
    private final HttpClient httpClient;
    private final JsonCodec<CallProcedureRequest> requestCodec;
    private final JsonCodec<CallProcedureResponse> responseCodec;

    @Inject
    public RemoteCallProcedureTask(
            @ForCallProcedure HttpClient httpClient,
            JsonCodec<CallProcedureRequest> requestCodec,
            JsonCodec<CallProcedureResponse> responseCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.requestCodec = requireNonNull(requestCodec, "requestCodec is null");
        this.responseCodec = requireNonNull(responseCodec, "responseCodec is null");
    }

    public ListenableFuture<JsonResponse<CallProcedureResponse>> call(InternalNode node, CallProcedureRequest callProcedureRequest)
    {
        requireNonNull(node, "node is null");
        requireNonNull(callProcedureRequest, "callProcedureRequest is null");

        Request request = preparePost()
                .setUri(uriBuilderFrom(node.getInternalUri()).appendPath("/v1/callProcedure").build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(requestCodec, callProcedureRequest))
                .build();

        return httpClient.executeAsync(request, createFullJsonResponseHandler(responseCodec));
    }
}
