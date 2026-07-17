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

import io.opentelemetry.api.common.AttributeKey;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

/**
 * GenAI semantic convention attribute keys and values.
 * <p>
 * These mirror the conventions that used to live in {@code io.opentelemetry.semconv.incubating}
 * before they were moved out of that artifact to the
 * <a href="https://github.com/open-telemetry/semantic-conventions-genai">OpenTelemetry GenAI
 * semantic conventions repository</a>, which does not yet publish a Maven artifact. They are
 * defined locally so we no longer depend on the deprecated constants while keeping the emitted
 * attribute names unchanged.
 */
final class GenAiAttributes
{
    private GenAiAttributes() {}

    public static final AttributeKey<String> GEN_AI_OPERATION_NAME = stringKey("gen_ai.operation.name");
    public static final AttributeKey<String> GEN_AI_PROVIDER_NAME = stringKey("gen_ai.provider.name");
    public static final AttributeKey<String> GEN_AI_REQUEST_MODEL = stringKey("gen_ai.request.model");
    public static final AttributeKey<Long> GEN_AI_REQUEST_SEED = longKey("gen_ai.request.seed");
    public static final AttributeKey<String> GEN_AI_RESPONSE_ID = stringKey("gen_ai.response.id");
    public static final AttributeKey<String> GEN_AI_RESPONSE_MODEL = stringKey("gen_ai.response.model");
    public static final AttributeKey<Long> GEN_AI_USAGE_INPUT_TOKENS = longKey("gen_ai.usage.input_tokens");
    public static final AttributeKey<Long> GEN_AI_USAGE_OUTPUT_TOKENS = longKey("gen_ai.usage.output_tokens");

    public static final AttributeKey<String> OPENAI_RESPONSE_SERVICE_TIER = stringKey("openai.response.service_tier");
    public static final AttributeKey<String> OPENAI_RESPONSE_SYSTEM_FINGERPRINT = stringKey("openai.response.system_fingerprint");

    public static final String OPERATION_NAME_CHAT = "chat";
    public static final String PROVIDER_NAME_ANTHROPIC = "anthropic";
    public static final String PROVIDER_NAME_OPENAI = "openai";
}
