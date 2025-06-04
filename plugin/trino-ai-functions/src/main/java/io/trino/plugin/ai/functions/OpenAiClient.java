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
import com.google.common.net.HttpHeaders;
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
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPENAI_RESPONSE_SERVICE_TIER;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_SEED;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_ID;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_SYSTEM;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiOperationNameIncubatingValues.CHAT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiSystemIncubatingValues.OPENAI;
import static io.trino.plugin.ai.functions.AiErrorCode.AI_ERROR;
import static java.util.Objects.requireNonNull;

public class OpenAiClient
        extends AbstractAiClient
{
    private static final JsonCodec<ChatRequest> CHAT_REQUEST_CODEC = jsonCodec(ChatRequest.class);
    private static final JsonCodec<ChatResponse> CHAT_RESPONSE_CODEC = jsonCodec(ChatResponse.class);

    private final HttpClient httpClient;
    private final Tracer tracer;
    private final URI endpoint;
    private final String apiKey;

    @Inject
    public OpenAiClient(@ForAiClient HttpClient httpClient, Tracer tracer, OpenAiConfig openAiConfig, AiConfig aiConfig)
    {
        super(aiConfig);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.endpoint = openAiConfig.getEndpoint();
        this.apiKey = openAiConfig.getApiKey();
    }

    @Override
    protected String generateCompletion(String model, String prompt)
    {
        URI uri = uriBuilderFrom(endpoint)
                .appendPath("/v1/chat/completions")
                .build();

        ChatRequest.Message messages = new ChatRequest.Message("user", prompt);
        ChatRequest body = new ChatRequest(model, List.of(messages), 0);

        Request request = preparePost()
                .setUri(uri)
                .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(CHAT_REQUEST_CODEC, body))
                .build();

        Span span = tracer.spanBuilder(CHAT + " " + model)
                .setAttribute(GEN_AI_OPERATION_NAME, CHAT)
                .setAttribute(GEN_AI_SYSTEM, OPENAI)
                .setAttribute(GEN_AI_REQUEST_MODEL, model)
                .setAttribute(GEN_AI_REQUEST_SEED, body.seed())
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        ChatResponse response;
        try (var _ = span.makeCurrent()) {
            response = httpClient.execute(request, createJsonResponseHandler(CHAT_RESPONSE_CODEC));
            span.setAttribute(GEN_AI_RESPONSE_ID, response.id());
            span.setAttribute(GEN_AI_RESPONSE_MODEL, response.model());
            span.setAttribute(GEN_AI_OPENAI_RESPONSE_SERVICE_TIER, response.serviceTier());
            span.setAttribute(GEN_AI_OPENAI_RESPONSE_SYSTEM_FINGERPRINT, response.systemFingerprint());
            span.setAttribute(GEN_AI_USAGE_INPUT_TOKENS, response.usage().promptTokens());
            span.setAttribute(GEN_AI_USAGE_OUTPUT_TOKENS, response.usage().completionTokens());
        }
        catch (RuntimeException e) {
            span.setStatus(ERROR, e.getMessage());
            span.recordException(e);
            throw new TrinoException(AI_ERROR, "Request to AI provider at %s for model %s failed".formatted(uri, model), e);
        }
        finally {
            span.end();
        }

        if (response.choices().isEmpty()) {
            throw new TrinoException(AI_ERROR, "No response from AI provider at %s for model %s".formatted(uri, model));
        }
        ChatResponse.Choice message = response.choices().getFirst();

        if (message.message().refusal() != null) {
            throw new TrinoException(AI_ERROR, "AI provider at %s for model %s refused to generate response: %s".formatted(uri, model, message.message().refusal()));
        }

        return message.message().content();
    }

    public record ChatRequest(String model, List<Message> messages, int seed)
    {
        public record Message(String role, String content) {}
    }

    @JsonNaming(SnakeCaseStrategy.class)
    public record ChatResponse(
            String id,
            String model,
            List<Choice> choices,
            Usage usage,
            String serviceTier,
            String systemFingerprint)
    {
        public record Choice(Message message)
        {
            public record Message(String content, String refusal) {}
        }

        @JsonNaming(SnakeCaseStrategy.class)
        public record Usage(int promptTokens, int completionTokens) {}
    }
}
