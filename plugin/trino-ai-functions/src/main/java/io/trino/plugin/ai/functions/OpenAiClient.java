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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.OAuth2Token;
import io.trino.spi.security.OAuth2TokenExchanger;
import io.trino.spi.security.TokenExchangeRequest;

import java.net.URI;
import java.util.List;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.trino.plugin.ai.functions.AiErrorCode.AI_ERROR;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_OPERATION_NAME;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_PROVIDER_NAME;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_REQUEST_MODEL;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_REQUEST_SEED;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_RESPONSE_ID;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_RESPONSE_MODEL;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.trino.plugin.ai.functions.GenAiAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.trino.plugin.ai.functions.GenAiAttributes.OPENAI_RESPONSE_SERVICE_TIER;
import static io.trino.plugin.ai.functions.GenAiAttributes.OPENAI_RESPONSE_SYSTEM_FINGERPRINT;
import static io.trino.plugin.ai.functions.GenAiAttributes.OPERATION_NAME_CHAT;
import static io.trino.plugin.ai.functions.GenAiAttributes.PROVIDER_NAME_OPENAI;
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
    private final boolean useOauth2TokenExchange;
    private final List<String> oauth2TokenExchangeAudience;
    private final List<String> oauth2TokenExchangeScope;
    private final OAuth2TokenExchanger oAuth2TokenExchanger;

    @Inject
    public OpenAiClient(@ForAiClient HttpClient httpClient, Tracer tracer, OpenAiConfig openAiConfig, AiConfig aiConfig, OAuth2TokenExchanger oAuth2TokenExchanger)
    {
        super(aiConfig);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.endpoint = openAiConfig.getEndpoint();
        this.useOauth2TokenExchange = openAiConfig.isUseOauth2TokenExchange();
        this.oauth2TokenExchangeAudience = openAiConfig.getOauth2TokenExchangeAudience();
        this.oauth2TokenExchangeScope = openAiConfig.getOauth2TokenExchangeScope();
        this.oAuth2TokenExchanger = requireNonNull(oAuth2TokenExchanger, "oAuth2TokenExchanger is null");
        if (!useOauth2TokenExchange) {
            requireNonNull(openAiConfig.getApiKey(), "apiKey is null; set ai.openai.api-key or use ai.openai.oauth2-token-exchange=true");
        }
        this.apiKey = openAiConfig.getApiKey();
    }

    @Override
    protected boolean usesPerUserCredentials()
    {
        return useOauth2TokenExchange;
    }

    @Override
    protected String generateCompletion(ConnectorSession session, String model, String prompt)
    {
        URI uri = uriBuilderFrom(endpoint)
                .appendPath("/v1/chat/completions")
                .build();

        ChatRequest.Message messages = new ChatRequest.Message("user", prompt);
        ChatRequest body = new ChatRequest(model, List.of(messages), 0);

        String bearerToken = resolveBearerToken(session);
        Request request = preparePost()
                .setUri(uri)
                .setHeader(AUTHORIZATION, "Bearer " + bearerToken)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(CHAT_REQUEST_CODEC, body))
                .build();

        Span span = tracer.spanBuilder(OPERATION_NAME_CHAT + " " + model)
                .setAttribute(GEN_AI_OPERATION_NAME, OPERATION_NAME_CHAT)
                .setAttribute(GEN_AI_PROVIDER_NAME, PROVIDER_NAME_OPENAI)
                .setAttribute(GEN_AI_REQUEST_MODEL, model)
                .setAttribute(GEN_AI_REQUEST_SEED, body.seed())
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        ChatResponse response;
        try (var _ = span.makeCurrent()) {
            response = httpClient.execute(request, createJsonResponseHandler(CHAT_RESPONSE_CODEC));
            span.setAttribute(GEN_AI_RESPONSE_ID, response.id());
            span.setAttribute(GEN_AI_RESPONSE_MODEL, response.model());
            span.setAttribute(OPENAI_RESPONSE_SERVICE_TIER, response.serviceTier());
            span.setAttribute(OPENAI_RESPONSE_SYSTEM_FINGERPRINT, response.systemFingerprint());
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

    private String resolveBearerToken(ConnectorSession session)
    {
        if (!useOauth2TokenExchange) {
            return apiKey;
        }
        OAuth2Token token = oAuth2TokenExchanger.exchangeToken(
                        session.getIdentity(),
                        new TokenExchangeRequest(oauth2TokenExchangeAudience, oauth2TokenExchangeScope))
                .orElseThrow(() -> new TrinoException(AI_ERROR, "Failed to exchange OAuth2 token for AI provider access"));
        return token.accessToken();
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
