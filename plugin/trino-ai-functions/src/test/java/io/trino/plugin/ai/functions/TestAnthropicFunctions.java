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

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

@HoverflySimulate(config = @HoverflyConfig(disableTlsVerification = true))
public class TestAnthropicFunctions
        extends AbstractTestAiFunctions
{
    @Override
    protected Map<String, String> getProperties(HostAndPort hoverflyAddress)
    {
        return ImmutableMap.<String, String>builder()
                .put("ai.provider", "anthropic")
                .put("ai.model", "claude-3-5-sonnet-latest")
                .put("ai.anthropic.api-key", "test")
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
                .filteredOn(span -> span.getName().equals("chat claude-3-5-sonnet-latest"))
                .hasSize(1).first()
                .extracting(SpanData::getAttributes, ATTRIBUTES)
                .containsEntry("gen_ai.operation.name", "chat")
                .containsEntry("gen_ai.provider.name", "anthropic")
                .containsEntry("gen_ai.request.model", "claude-3-5-sonnet-latest")
                .containsEntry("gen_ai.response.id", "msg_014mmUArhqd4VHpeHVuPKCVe")
                .containsEntry("gen_ai.response.model", "claude-3-5-sonnet-20241022")
                .containsEntry("gen_ai.usage.input_tokens", 14)
                .containsEntry("gen_ai.usage.output_tokens", 10);

        super.testGen();
    }

    @Test
    @Override
    public void testPrompt()
    {
        // Anthropic prompt test uses claude model
        assertThat(assertions.function("ai_prompt", "'What is 2+2?'", "'claude-3-5-sonnet-latest'", "0.0e0"))
                .hasType(VARCHAR)
                .isEqualTo("2 + 2 equals 4.");
    }

    @Test
    @Override
    public void testPromptCacheRespectsTemperature()
    {
        // Same prompt and model, different temperatures should be separate cache entries
        assertThat(assertions.function("ai_prompt", "'test cache'", "'claude-3-5-sonnet-latest'", "0.0e0"))
                .hasType(VARCHAR);
        assertThat(assertions.function("ai_prompt", "'test cache'", "'claude-3-5-sonnet-latest'", "0.5e0"))
                .hasType(VARCHAR);
        // Both should execute successfully, proving they're separate cache entries
    }

    @Test
    @Override
    public void testPromptInvalidTemperatureLow()
    {
        assertThat(assertions.query("SELECT ai_prompt('test', 'claude-3-5-sonnet-latest', -0.1e0)"))
                .failure()
                .hasMessageContaining("temperature must be between 0.0 and 1.0 for Anthropic");
    }

    @Test
    @Override
    public void testPromptInvalidTemperatureHigh()
    {
        // Anthropic's max temperature is 1.0, so 1.1 should fail
        assertThat(assertions.query("SELECT ai_prompt('test', 'claude-3-5-sonnet-latest', 1.1e0)"))
                .failure()
                .hasMessageContaining("temperature must be between 0.0 and 1.0 for Anthropic");
    }
}
