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
package io.trino.cli;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAgentMode
{
    private static final Map<String, String> NO_ENVIRONMENT = ImmutableMap.of();
    private static final Map<String, String> CLAUDE_CODE = ImmutableMap.of("CLAUDECODE", "1");

    @Test
    public void testDisabledWithoutAgentEnvironment()
    {
        assertThat(resolve(Optional.empty(), NO_ENVIRONMENT, false, false)).isEmpty();
        assertThat(resolve(Optional.empty(), ImmutableMap.of("CLAUDECODE", ""), false, false)).isEmpty();
        assertThat(resolve(Optional.empty(), ImmutableMap.of("UNRELATED", "1"), false, false)).isEmpty();
    }

    @Test
    public void testDetectedFromAgentEnvironment()
    {
        for (String variable : new String[] {"AGENT", "AI_AGENT", "CLAUDECODE", "CODEX_CI", "CODEX_SANDBOX", "CODEX_THREAD_ID", "COPILOT_CLI", "CURSOR_AGENT", "GEMINI_CLI", "GROK_AGENT", "MUSE_SESSION_ID", "MUSE_THREAD_ID"}) {
            Optional<AgentMode> mode = resolve(Optional.empty(), ImmutableMap.of(variable, "1"), false, false);

            assertThat(mode).as(variable).isPresent();
            assertThat(mode.orElseThrow().notice())
                    .contains("agent mode on (detected " + variable + "); use --no-agent or TRINO_AGENT_MODE=0 to disable");
        }
    }

    @Test
    public void testNotDetectedOnTerminal()
    {
        assertThat(resolve(Optional.empty(), CLAUDE_CODE, true, false)).isEmpty();
    }

    @Test
    public void testTerminalIsOnlyCheckedWhenAgentIsDetected()
    {
        BooleanSupplier unexpected = () -> {
            throw new AssertionError("standard output must not be checked");
        };
        assertThat(AgentMode.resolve(Optional.of(true), CLAUDE_CODE, unexpected, false)).isPresent();
        assertThat(AgentMode.resolve(Optional.of(false), CLAUDE_CODE, unexpected, false)).isEmpty();
        assertThat(AgentMode.resolve(Optional.empty(), ImmutableMap.of("TRINO_AGENT_MODE", "1"), unexpected, false)).isPresent();
        assertThat(AgentMode.resolve(Optional.empty(), NO_ENVIRONMENT, unexpected, false)).isEmpty();
        assertThat(AgentMode.resolve(Optional.empty(), CLAUDE_CODE, unexpected, true)).isEmpty();
    }

    @Test
    public void testNotDetectedWithOutputFormat()
    {
        assertThat(resolve(Optional.empty(), CLAUDE_CODE, false, true)).isEmpty();
    }

    @Test
    public void testFlagOverridesEverything()
    {
        assertThat(resolve(Optional.of(true), NO_ENVIRONMENT, true, true)).isPresent();
        assertThat(resolve(Optional.of(true), ImmutableMap.of("TRINO_AGENT_MODE", "0"), false, false)).isPresent();
        assertThat(resolve(Optional.of(false), CLAUDE_CODE, false, false)).isEmpty();
        assertThat(resolve(Optional.of(false), ImmutableMap.of("TRINO_AGENT_MODE", "1"), false, false)).isEmpty();
    }

    @Test
    public void testEnvironmentVariableOverridesDetection()
    {
        assertThat(resolve(Optional.empty(), ImmutableMap.of("TRINO_AGENT_MODE", "1"), true, true)).isPresent();
        assertThat(resolve(Optional.empty(), ImmutableMap.of("TRINO_AGENT_MODE", "true"), false, false)).isPresent();
        assertThat(resolve(Optional.empty(), ImmutableMap.of("TRINO_AGENT_MODE", "0", "CLAUDECODE", "1"), false, false)).isEmpty();
        assertThat(resolve(Optional.empty(), ImmutableMap.of("TRINO_AGENT_MODE", "false", "CLAUDECODE", "1"), false, false)).isEmpty();
    }

    @Test
    public void testNoNoticeWhenEnabledExplicitly()
    {
        assertThat(resolve(Optional.of(true), CLAUDE_CODE, false, false).orElseThrow().notice()).isEmpty();
        assertThat(resolve(Optional.empty(), ImmutableMap.of("TRINO_AGENT_MODE", "1", "CLAUDECODE", "1"), false, false).orElseThrow().notice()).isEmpty();
    }

    @Test
    public void testInvalidEnvironmentVariable()
    {
        assertThatThrownBy(() -> resolve(Optional.empty(), ImmutableMap.of("TRINO_AGENT_MODE", "maybe"), false, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TRINO_AGENT_MODE must be 1 or 0: maybe");
    }

    private static Optional<AgentMode> resolve(Optional<Boolean> flag, Map<String, String> environment, boolean standardOutputIsTerminal, boolean outputFormatSpecified)
    {
        return AgentMode.resolve(flag, environment, () -> standardOutputIsTerminal, outputFormatSpecified);
    }
}
