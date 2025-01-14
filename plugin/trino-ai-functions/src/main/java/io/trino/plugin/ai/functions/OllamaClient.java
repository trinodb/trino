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
package io.trino.plugin.ai.functions;

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;

import java.net.URI;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.ai.functions.AiErrorCode.AI_ERROR;
import static java.util.Objects.requireNonNull;

public class OllamaClient
        extends AbstractAiClient
{
    private static final JsonCodec<GenerateRequest> GENERATE_REQUEST_CODEC = jsonCodec(GenerateRequest.class);
    private static final JsonCodec<GenerateResponse> GENERATE_RESPONSE_CODEC = jsonCodec(GenerateResponse.class);

    private final HttpClient httpClient;
    private final URI endpoint;

    @Inject
    public OllamaClient(@ForAiClient HttpClient httpClient, OllamaConfig ollamaConfig, AiConfig aiConfig)
    {
        super(aiConfig);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.endpoint = ollamaConfig.getEndpoint();
    }

    @Override
    protected String generateCompletion(String model, String prompt)
    {
        URI uri = uriBuilderFrom(endpoint)
                .appendPath("/api/generate")
                .build();

        GenerateRequest.Options options = new GenerateRequest.Options(0);
        GenerateRequest body = new GenerateRequest(model, prompt, false, options);

        Request request = preparePost()
                .setUri(uri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(GENERATE_REQUEST_CODEC, body))
                .build();

        GenerateResponse response;
        try {
            response = httpClient.execute(request, createJsonResponseHandler(GENERATE_RESPONSE_CODEC));
        }
        catch (RuntimeException e) {
            throw new TrinoException(AI_ERROR, "Failed to execute AI request", e);
        }

        return response.response();
    }

    public record GenerateRequest(String model, String prompt, boolean stream, Options options)
    {
        public record Options(int seed) {}
    }

    public record GenerateResponse(String response) {}
}
