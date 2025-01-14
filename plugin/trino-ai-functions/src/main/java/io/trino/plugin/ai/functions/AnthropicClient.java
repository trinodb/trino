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

import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
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
import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_ID;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_SYSTEM;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiOperationNameIncubatingValues.CHAT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiSystemIncubatingValues.ANTHROPIC;
import static io.trino.plugin.ai.functions.AiErrorCode.AI_ERROR;
import static java.util.Objects.requireNonNull;

public class AnthropicClient
        extends AbstractAiClient
{
    private static final JsonCodec<MessageRequest> MESSAGE_REQUEST_CODEC = jsonCodec(MessageRequest.class);
    private static final JsonCodec<MessageResponse> MESSAGE_RESPONSE_CODEC = jsonCodec(MessageResponse.class);

    private final HttpClient httpClient;
    private final Tracer tracer;
    private final URI endpoint;
    private final String apiKey;

    @Inject
    public AnthropicClient(@ForAiClient HttpClient httpClient, Tracer tracer, AnthropicConfig anthropicConfig, AiConfig aiConfig)
    {
        super(aiConfig);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.endpoint = anthropicConfig.getEndpoint();
        this.apiKey = anthropicConfig.getApiKey();
    }

    @Override
    protected String generateCompletion(String model, String prompt)
    {
        URI uri = uriBuilderFrom(endpoint)
                .appendPath("/v1/messages")
                .build();

        MessageRequest.Message messages = new MessageRequest.Message("user", prompt);
        MessageRequest body = new MessageRequest(model, 4096, List.of(messages));

        Request request = preparePost()
                .setUri(uri)
                .setHeader("X-Api-Key", apiKey)
                .setHeader("Anthropic-Version", "2023-06-01")
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(MESSAGE_REQUEST_CODEC, body))
                .build();

        Span span = tracer.spanBuilder(CHAT + " " + model)
                .setAttribute(GEN_AI_OPERATION_NAME, CHAT)
                .setAttribute(GEN_AI_SYSTEM, ANTHROPIC)
                .setAttribute(GEN_AI_REQUEST_MODEL, model)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        MessageResponse response;
        try (var _ = span.makeCurrent()) {
            response = httpClient.execute(request, createJsonResponseHandler(MESSAGE_RESPONSE_CODEC));
            span.setAttribute(GEN_AI_RESPONSE_ID, response.id());
            span.setAttribute(GEN_AI_RESPONSE_MODEL, response.model());
            span.setAttribute(GEN_AI_USAGE_INPUT_TOKENS, response.usage().inputTokens());
            span.setAttribute(GEN_AI_USAGE_OUTPUT_TOKENS, response.usage().outputTokens());
        }
        catch (RuntimeException e) {
            span.setStatus(ERROR, e.getMessage());
            span.recordException(e);
            throw new TrinoException(AI_ERROR, "Failed to execute AI request", e);
        }
        finally {
            span.end();
        }

        if (response.content().isEmpty()) {
            throw new TrinoException(AI_ERROR, "No response from AI model");
        }
        return response.content().getFirst().text();
    }

    @JsonNaming(SnakeCaseStrategy.class)
    public record MessageRequest(String model, int maxTokens, List<Message> messages)
    {
        public record Message(String role, String content) {}
    }

    public record MessageResponse(String id, String model, List<Content> content, Usage usage)
    {
        public record Content(String text) {}

        @JsonNaming(SnakeCaseStrategy.class)
        public record Usage(int inputTokens, int outputTokens) {}
    }
}
