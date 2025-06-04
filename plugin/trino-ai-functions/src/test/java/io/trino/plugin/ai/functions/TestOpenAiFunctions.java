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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.specto.hoverfly.junit5.api.HoverflyConfig;
import io.specto.hoverfly.junit5.api.HoverflySimulate;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@HoverflySimulate(config = @HoverflyConfig(disableTlsVerification = true))
public class TestOpenAiFunctions
        extends AbstractTestAiFunctions
{
    @Override
    protected Map<String, String> getProperties(HostAndPort hoverflyAddress)
    {
        return ImmutableMap.<String, String>builder()
                .put("ai.provider", "openai")
                .put("ai.model", "gpt-4o-mini")
                .put("ai.openai.api-key", "test")
                .put("ai.http-client.http-proxy", hoverflyAddress.toString())
                .put("ai.http-client.trust-store-path", "src/test/resources/hoverfly/hoverfly.pem")
                .buildOrThrow();
    }

    @Test
    @Override
    public void testGen()
    {
        // run the trace test first as later invocations will use the cache
        assertions.execute("SELECT ai_gen('What is the capital of France?')");

        assertThat(assertions.getQueryRunner().getSpans())
                .filteredOn(span -> span.getName().equals("chat gpt-4o-mini"))
                .hasSize(1).first()
                .extracting(SpanData::getAttributes, ATTRIBUTES)
                .containsEntry("gen_ai.operation.name", "chat")
                .containsEntry("gen_ai.system", "openai")
                .containsEntry("gen_ai.request.model", "gpt-4o-mini")
                .containsEntry("gen_ai.request.seed", 0)
                .containsEntry("gen_ai.response.id", "chatcmpl-AyyfeFHICsf6xiGSzc9w2t0nhqmZd")
                .containsEntry("gen_ai.response.model", "gpt-4o-mini-2024-07-18")
                .containsEntry("gen_ai.openai.response.service_tier", "default")
                .containsEntry("gen_ai.openai.response.system_fingerprint", "fp_72ed7ab54c")
                .containsEntry("gen_ai.usage.input_tokens", 14)
                .containsEntry("gen_ai.usage.output_tokens", 8);

        super.testGen();
    }
}
