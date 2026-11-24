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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Agent mode makes the CLI suitable for programs, such as AI agents, that read its output
 * through a pipe.
 */
public final class AgentMode
{
    public static final String ENVIRONMENT_VARIABLE = "TRINO_AGENT_MODE";

    private static final List<String> AGENT_ENVIRONMENT_VARIABLES = ImmutableList.of(
            "AGENT",
            "AI_AGENT",
            "CLAUDECODE",
            "CODEX_CI",
            "CODEX_SANDBOX",
            "CODEX_THREAD_ID",
            "COPILOT_CLI",
            "CURSOR_AGENT",
            "GEMINI_CLI",
            "GROK_AGENT",
            "MUSE_SESSION_ID",
            "MUSE_THREAD_ID");

    private final Optional<String> detectedVariable;

    private AgentMode(Optional<String> detectedVariable)
    {
        this.detectedVariable = requireNonNull(detectedVariable, "detectedVariable is null");
    }

    /**
     * Decides whether agent mode is on. The {@code --agent} and {@code --no-agent} options take
     * precedence over {@link #ENVIRONMENT_VARIABLE}, which takes precedence over detecting
     * an agent from the environment.
     *
     * @param flag the value of {@code --agent} or {@code --no-agent}, if specified
     * @param environment the process environment
     * @param standardOutputIsTerminal whether the output is shown to a person in a terminal, asked only when an agent is detected
     * @param outputFormatSpecified whether the user chose an output format on the command line
     */
    public static Optional<AgentMode> resolve(Optional<Boolean> flag, Map<String, String> environment, BooleanSupplier standardOutputIsTerminal, boolean outputFormatSpecified)
    {
        if (flag.isPresent()) {
            return enabled(flag.get(), Optional.empty());
        }

        Optional<Boolean> configured = parse(environment.get(ENVIRONMENT_VARIABLE));
        if (configured.isPresent()) {
            return enabled(configured.get(), Optional.empty());
        }

        if (outputFormatSpecified) {
            return Optional.empty();
        }
        Optional<String> variable = AGENT_ENVIRONMENT_VARIABLES.stream()
                .filter(name -> !isNullOrBlank(environment.get(name)))
                .findFirst();
        if (variable.isEmpty() || standardOutputIsTerminal.getAsBoolean()) {
            return Optional.empty();
        }
        return variable.map(name -> new AgentMode(Optional.of(name)));
    }

    /**
     * A message for the user when agent mode was turned on by detection, not by a setting.
     */
    public Optional<String> notice()
    {
        return detectedVariable.map(variable -> format(
                "agent mode on (detected %s); use --no-agent or %s=0 to disable",
                variable,
                ENVIRONMENT_VARIABLE));
    }

    private static Optional<AgentMode> enabled(boolean enabled, Optional<String> detectedVariable)
    {
        if (!enabled) {
            return Optional.empty();
        }
        return Optional.of(new AgentMode(detectedVariable));
    }

    private static Optional<Boolean> parse(String value)
    {
        if (isNullOrBlank(value)) {
            return Optional.empty();
        }
        switch (value.trim().toLowerCase(ENGLISH)) {
            case "1":
            case "true":
                return Optional.of(true);
            case "0":
            case "false":
                return Optional.of(false);
        }
        throw new IllegalArgumentException(format("%s must be 1 or 0: %s", ENVIRONMENT_VARIABLE, value));
    }

    private static boolean isNullOrBlank(String value)
    {
        return value == null || value.isBlank();
    }
}
