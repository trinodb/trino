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

import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.List;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.ai.functions.AiErrorCode.AI_ERROR;
import static java.util.Objects.requireNonNull;

public class OpenAiClient
        extends AbstractAiClient
{
    private static final JsonCodec<GenerateRequest> GENERATE_REQUEST_CODEC = jsonCodec(GenerateRequest.class);
    private static final JsonCodec<GenerateResponse> GENERATE_RESPONSE_CODEC = jsonCodec(GenerateResponse.class);

    private final HttpClient httpClient;
    private final URI endpoint;
    private final String apiKey;

    @Inject
    public OpenAiClient(@ForAiClient HttpClient httpClient, OpenAiConfig openAiConfig, AiConfig aiConfig)
    {
        super(aiConfig);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.endpoint = openAiConfig.getEndpoint();
        this.apiKey = openAiConfig.getApiKey();
    }

    @Override
    protected String generateCompletion(String model, String prompt)
    {
        URI uri = uriBuilderFrom(endpoint)
                .appendPath("/v1/chat/completions")
                .build();

        GenerateRequest.Message messages = new GenerateRequest.Message("user", prompt);
        GenerateRequest body = new GenerateRequest(model, List.of(messages), 0);

        Request request = preparePost()
                .setUri(uri)
                .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
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

        if (response.choices().isEmpty()) {
            throw new TrinoException(AI_ERROR, "No response from AI model");
        }
        GenerateResponse.Choice message = response.choices().getFirst();

        if (message.message().refusal() != null) {
            throw new TrinoException(AI_ERROR, "AI model refused to generate response: " + message.message().refusal());
        }

        return message.message().content();
    }

    public record GenerateRequest(String model, List<Message> messages, int seed)
    {
        public record Message(String role, String content) {}
    }

    public record GenerateResponse(List<Choice> choices)
    {
        public record Choice(Message message)
        {
            public record Message(String content, String refusal) {}
        }
    }
}
