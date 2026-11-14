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
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.TypedQueryData;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAgentProgressPrinter
{
    @Test
    public void testFormat()
    {
        assertThat(AgentProgressPrinter.describe(status("RUNNING", OptionalDouble.of(42.7), 12_999, 1_234_567, 34_567_890)))
                .isEqualTo("progress: query_id=q1 state=RUNNING progress=42% elapsed=12s rows=1234567 bytes=34567890");
    }

    @Test
    public void testFormatWithoutProgressPercentage()
    {
        assertThat(AgentProgressPrinter.describe(status("QUEUED", OptionalDouble.empty(), 0, 0, 0)))
                .isEqualTo("progress: query_id=q1 state=QUEUED elapsed=0s rows=0 bytes=0");
    }

    @Test
    public void testProgressIsNeverComplete()
    {
        assertThat(AgentProgressPrinter.describe(status("RUNNING", OptionalDouble.of(100), 1000, 1, 1)))
                .contains("progress=99%");
    }

    @Test
    public void testPrintsPeriodicallyUntilClosed()
            throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        AtomicInteger calls = new AtomicInteger();
        try (AgentProgressPrinter printer = new AgentProgressPrinter(
                () -> {
                    calls.incrementAndGet();
                    return status("RUNNING", OptionalDouble.of(10), 1000, calls.get(), 0);
                },
                new PrintStream(bytes, true, UTF_8),
                10)) {
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (bytes.toString(UTF_8).lines().count() < 3 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
        }

        String output = bytes.toString(UTF_8);
        assertThat(output.lines()).hasSizeGreaterThanOrEqualTo(3);
        assertThat(output.lines()).allMatch(line -> line.startsWith("progress: query_id=q1 state=RUNNING progress=10% elapsed=1s rows="));

        // nothing is printed after close
        int linesAtClose = bytes.toString(UTF_8).split("\n").length;
        Thread.sleep(100);
        assertThat(bytes.toString(UTF_8).split("\n")).hasSize(linesAtClose);
    }

    @Test
    public void testFailureToReadStatusDoesNotStopPrinting()
            throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        AtomicInteger calls = new AtomicInteger();
        try (AgentProgressPrinter printer = new AgentProgressPrinter(
                () -> {
                    if (calls.incrementAndGet() == 1) {
                        throw new IllegalStateException("not available yet");
                    }
                    return status("RUNNING", OptionalDouble.empty(), 0, 0, 0);
                },
                new PrintStream(bytes, true, UTF_8),
                10)) {
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (bytes.size() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
        }

        assertThat(bytes.toString(UTF_8)).startsWith("progress: query_id=q1 state=RUNNING");
    }

    private static QueryResults status(String state, OptionalDouble progress, long elapsedMillis, long rows, long bytes)
    {
        return new QueryResults(
                "q1",
                URI.create("http://localhost/query.html?q1"),
                null,
                null,
                ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT))),
                TypedQueryData.of(ImmutableList.of()),
                StatementStats.builder()
                        .setState(state)
                        .setProgressPercentage(progress)
                        .setRunningPercentage(OptionalDouble.empty())
                        .setElapsedTimeMillis(elapsedMillis)
                        .setProcessedRows(rows)
                        .setProcessedBytes(bytes)
                        .build(),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());
    }
}
