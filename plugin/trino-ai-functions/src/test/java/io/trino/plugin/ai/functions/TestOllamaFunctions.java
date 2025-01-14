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
import io.specto.hoverfly.junit5.api.HoverflySimulate;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

@HoverflySimulate
public class TestOllamaFunctions
        extends AbstractTestAiFunctions
{
    @Override
    protected Map<String, String> getProperties(HostAndPort hoverflyAddress)
    {
        return ImmutableMap.<String, String>builder()
                .put("ai.provider", "ollama")
                .put("ai.model", "llama3.3")
                .put("ai.ollama.endpoint", "http://localhost:11434")
                .put("ai.http-client.http-proxy", hoverflyAddress.toString())
                .buildOrThrow();
    }

    @Test
    public void testSimilarity()
    {
        assertThat(assertions.function("ai_similarity", "'The sky is blue'", "'The sky is blue'"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);

        assertThat(assertions.function("ai_similarity", "'The sky is blue'", "'The sky is green'"))
                .hasType(DOUBLE)
                .isEqualTo(0.73);

        assertThat(assertions.function("ai_similarity", "'The sky is blue'", "'The car is red'"))
                .hasType(DOUBLE)
                .isEqualTo(0.23);

        assertThat(assertions.function("ai_similarity", "'I hate pizza'", "'We drink water'"))
                .hasType(DOUBLE)
                .isEqualTo(0.17);

        assertThat(assertions.function("ai_similarity", "'What is that?'", "'Go home now'"))
                .hasType(DOUBLE)
                .isEqualTo(0.18);
    }
}
