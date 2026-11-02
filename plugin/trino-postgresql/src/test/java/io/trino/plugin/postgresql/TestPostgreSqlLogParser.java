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
package io.trino.plugin.postgresql;

import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPostgreSqlLogParser
{
    @Test
    public void testCancellationWithInterleavedLogEntries()
    {
        TestingPostgreSqlServer.PostgreSqlLogParser logParser = new TestingPostgreSqlServer.PostgreSqlLogParser();

        assertThat(logParser.parse(logEntry(123, "ERROR:  canceling statement due to user request"))).isEmpty();
        assertThat(logParser.parse(logEntry(456, "LOG:  execute <unnamed>: SELECT 1")))
                .contains(new RemoteDatabaseEvent("SELECT 1", RUNNING));
        assertThat(logParser.parse(logEntry(123, "LOG:  duration: 1000.000 ms"))).isEmpty();
        assertThat(logParser.parse(logEntry(123, "STATEMENT:  SELECT pg_sleep(60)")))
                .contains(new RemoteDatabaseEvent("SELECT pg_sleep(60)", CANCELLED));
    }

    private static String logEntry(int backendProcessId, String message)
    {
        return "2026-08-21 12:34:56.789 UTC [%s] %s".formatted(backendProcessId, message);
    }
}
