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

import io.trino.client.ErrorLocation;
import io.trino.client.QueryError;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static io.trino.cli.Trino.createCommandLine;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAgentErrors
{
    @Test
    public void testQueryFailure()
    {
        QueryError error = new QueryError(
                "line 1:8: Column 'x' cannot be resolved",
                "42000",
                47,
                "COLUMN_NOT_FOUND",
                "USER_ERROR",
                new ErrorLocation(1, 8),
                null);

        assertThat(AgentErrors.queryFailure("q1", error)).isEqualTo("" +
                "{\"type\":\"error\",\"query_id\":\"q1\",\"error_name\":\"COLUMN_NOT_FOUND\",\"error_type\":\"USER_ERROR\"," +
                "\"error_code\":47,\"sql_state\":\"42000\",\"message\":\"line 1:8: Column 'x' cannot be resolved\"," +
                "\"location\":{\"line\":1,\"column\":8}}");
    }

    @Test
    public void testQueryFailureWithoutOptionalFields()
    {
        QueryError error = new QueryError("failed", null, 65536, null, null, null, null);

        assertThat(AgentErrors.queryFailure("q1", error)).isEqualTo("{\"type\":\"error\",\"query_id\":\"q1\",\"error_code\":65536,\"message\":\"failed\"}");
    }

    @Test
    public void testMessageIsOneLine()
    {
        String json = AgentErrors.message("first \"line\"\nsecond\tline\\");

        assertThat(json).isEqualTo("{\"type\":\"error\",\"message\":\"first \\\"line\\\"\\nsecond\\tline\\\\\"}");
        assertThat(json).doesNotContain("\n");
    }

    @Test
    public void testQueryMessage()
    {
        assertThat(AgentErrors.queryMessage("q1", "Query aborted by user"))
                .isEqualTo("{\"type\":\"error\",\"query_id\":\"q1\",\"message\":\"Query aborted by user\"}");
    }

    @Test
    public void testException()
    {
        assertThat(AgentErrors.exception(new RuntimeException("boom"))).isEqualTo("{\"type\":\"error\",\"message\":\"boom\"}");
        assertThat(AgentErrors.exception(new IllegalStateException())).isEqualTo("{\"type\":\"error\",\"message\":\"java.lang.IllegalStateException\"}");
    }

    @Test
    public void testCliErrorsAreJsonInAgentMode()
    {
        assertThat(runWithConflictingOptions("--agent"))
                .isEqualTo("{\"type\":\"error\",\"message\":\"both --execute and --file specified\"}\n");
    }

    @Test
    public void testCliErrorsArePlainTextWithoutAgentMode()
    {
        assertThat(runWithConflictingOptions("--no-agent"))
                .contains("both --execute and --file specified")
                .doesNotContain("\"type\"");
    }

    private static String runWithConflictingOptions(String agentOption)
    {
        // the error is written to the writer of the command line, and not to the process wide System.err
        StringWriter err = new StringWriter();
        int exitCode = createCommandLine(new Console())
                .setDefaultValueProvider(null)
                .setErr(new PrintWriter(err))
                .execute(agentOption, "--execute=select 1", "--file=query.sql");
        assertThat(exitCode).isEqualTo(1);
        return err.toString();
    }
}
