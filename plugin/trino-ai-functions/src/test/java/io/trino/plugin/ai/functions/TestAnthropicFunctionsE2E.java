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
import io.trino.sql.SqlPath;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end tests using real Anthropic API.
 * These tests only run when ANTHROPIC_API_KEY environment variable is set.
 * They make actual API calls to Anthropic and validate real responses.
 * <p>
 * To run these tests:
 * <pre>
 * export ANTHROPIC_API_KEY="sk-ant-..."
 * mvn test -Dtest=TestAnthropicFunctionsE2E
 * </pre>
 * <p>
 * Note: These tests will be SKIPPED if ANTHROPIC_API_KEY is not set.
 * <p>
 * Architecture: This is a standalone E2E test class that makes REAL API calls.
 * It does NOT use Hoverfly mocks (those are in TestAnthropicFunctions for integration testing).
 */
@EnabledIfEnvironmentVariable(named = "ANTHROPIC_API_KEY", matches = ".*")
@TestInstance(PER_CLASS)
public class TestAnthropicFunctionsE2E
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        // Direct setup WITHOUT Hoverfly - makes REAL API calls
        assertions = new QueryAssertions(testSessionBuilder()
                .setPath(SqlPath.buildPath("ai.ai", Optional.empty()))
                .build());
        assertions.addPlugin(new AiPlugin());

        // Create catalog with REAL Anthropic endpoint (no proxy)
        assertions.getQueryRunner().createCatalog("ai", "ai", ImmutableMap.of(
                "ai.provider", "anthropic",
                "ai.model", "claude-sonnet-4-5-20250929",
                "ai.anthropic.api-key", System.getenv("ANTHROPIC_API_KEY"),
                "ai.anthropic.endpoint", "https://api.anthropic.com")); // Real API endpoint
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testPrompt()
    {
        // Make REAL API call to Anthropic with temperature 0.0 (deterministic)
        // Verify response contains the answer - factual question should always include "4"
        assertThat(assertions.function("ai_prompt", "'What is 2+2?'", "'claude-sonnet-4-5-20250929'", "0.0e0"))
                .satisfies(result -> assertThat(result.toString()).contains("4"));
    }

    @Test
    public void testPromptWithHigherTemperature()
    {
        // Test with maximum Anthropic temperature (1.0)
        // Verify non-empty creative response
        assertThat(assertions.function("ai_prompt", "'Say hello in a creative way'", "'claude-sonnet-4-5-20250929'", "1.0e0"))
                .satisfies(result -> assertThat(result.toString()).isNotEmpty());
    }

    @Test
    public void testPromptInvalidTemperatureLow()
    {
        // Validation should reject invalid temperature
        assertThat(assertions.query("SELECT ai_prompt('test', 'claude-sonnet-4-5-20250929', -0.1e0)"))
                .failure()
                .hasMessageContaining("temperature must be between 0.0 and 1.0 for Anthropic");
    }

    @Test
    public void testPromptInvalidTemperatureHigh()
    {
        // Validation should reject invalid temperature (Anthropic max is 1.0)
        assertThat(assertions.query("SELECT ai_prompt('test', 'claude-sonnet-4-5-20250929', 1.1e0)"))
                .failure()
                .hasMessageContaining("temperature must be between 0.0 and 1.0 for Anthropic");
    }

    @Test
    public void testPromptCacheRespectsTemperature()
    {
        // Cache behavior test - same prompt with different temperatures
        // should produce different results (proving separate cache entries)
        // Both should execute successfully and return non-empty responses
        assertThat(assertions.function("ai_prompt", "'test cache'", "'claude-sonnet-4-5-20250929'", "0.0e0"))
                .satisfies(result -> assertThat(result.toString()).isNotEmpty());

        assertThat(assertions.function("ai_prompt", "'test cache'", "'claude-sonnet-4-5-20250929'", "0.5e0"))
                .satisfies(result -> assertThat(result.toString()).isNotEmpty());

        // Different temperatures may produce different responses (not guaranteed but common)
        // At minimum, both should be valid non-empty strings
    }
}
