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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Maps.filterValues;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.ai.functions.AiErrorCode.AI_ERROR;
import static java.util.Objects.requireNonNullElse;

public abstract class AbstractAiClient
        implements AiClient
{
    protected static final JsonCodec<List<String>> LIST_CODEC = listJsonCodec(String.class);
    protected static final JsonCodec<Map<String, String>> MAP_CODEC = mapJsonCodec(String.class, String.class);
    protected static final JsonCodec<String> STRING_CODEC = jsonCodec(String.class);

    protected final String analyzeSentimentModel;
    protected final String classifyModel;
    protected final String extractModel;
    protected final String fixGrammarModel;
    protected final String generateModel;
    protected final String maskModel;
    protected final String translateModel;

    private final Cache<String, String> completionCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));

    protected AbstractAiClient(AiConfig config)
    {
        this.analyzeSentimentModel = requireNonNullElse(config.getAnalyzeSentimentModel(), config.getModel());
        this.classifyModel = requireNonNullElse(config.getClassifyModel(), config.getModel());
        this.extractModel = requireNonNullElse(config.getExtractModel(), config.getModel());
        this.fixGrammarModel = requireNonNullElse(config.getFixGrammarModel(), config.getModel());
        this.generateModel = requireNonNullElse(config.getGenerateModel(), config.getModel());
        this.maskModel = requireNonNullElse(config.getMaskModel(), config.getModel());
        this.translateModel = requireNonNullElse(config.getTranslateModel(), config.getModel());
    }

    @Override
    public String analyzeSentiment(String text)
    {
        String prompt =
                """
                Classify the text below into one of the following labels: [positive, negative, neutral, mixed]
                Output only the label.
                =====
                %s
                """.formatted(text);

        String response = completion(analyzeSentimentModel, prompt);
        return response.toLowerCase(Locale.ROOT);
    }

    @Override
    public String classify(String text, List<String> labels)
    {
        String prompt =
                """
                Classify the text below into one of the following JSON encoded labels: %s
                Output the label as a JSON string (not a JSON object).
                Output only the label.
                =====
                %s
                """.formatted(LIST_CODEC.toJson(labels), text);

        String response = completion(classifyModel, prompt);
        try {
            return STRING_CODEC.fromJson(response);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(AI_ERROR, "Failed to parse AI response", e);
        }
    }

    @Override
    public Map<String, String> extract(String text, List<String> labels)
    {
        String prompt =
                """
                Extract a value for each of the JSON encoded labels from the text below.
                For each label, only extract a single value.
                Labels: %s
                Output the extracted values as a JSON object.
                Output only the JSON.
                Do not output a code block for the JSON.
                =====
                %s
                """.formatted(LIST_CODEC.toJson(labels), text);

        String response = completion(extractModel, prompt);
        try {
            return filterValues(MAP_CODEC.fromJson(response), Objects::nonNull);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(AI_ERROR, "Failed to parse AI response", e);
        }
    }

    @Override
    public String fixGrammar(String text)
    {
        String prompt =
                """
                Fix the grammar in the text below.
                Output only the text.
                =====
                %s
                """.formatted(text);

        return completion(fixGrammarModel, prompt);
    }

    @Override
    public String generate(String prompt)
    {
        return completion(generateModel, prompt);
    }

    @Override
    public String mask(String text, List<String> labels)
    {
        String prompt =
                """
                Mask the values for each of the JSON encoded labels in the text below.
                Labels: %s
                Replace the values with the text "[MASKED]".
                Output only the masked text.
                Do not output anything else.
                =====
                %s
                """.formatted(LIST_CODEC.toJson(labels), text);

        return completion(maskModel, prompt);
    }

    @Override
    public String translate(String text, String language)
    {
        String prompt =
                """
                Translate the text below to the language specified.
                The language is encoded as a JSON string.
                Output only the translated text.
                Language: %s
                =====
                %s
                """.formatted(STRING_CODEC.toJson(language), text);

        return completion(translateModel, prompt);
    }

    private String completion(String model, String prompt)
    {
        try {
            String key = model + "\0" + prompt;
            return completionCache.get(key, () -> generateCompletion(model, prompt));
        }
        catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TrinoException ex) {
                throw ex;
            }
            throw e;
        }
    }

    protected abstract String generateCompletion(String model, String prompt);
}
