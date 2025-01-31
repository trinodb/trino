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
package io.trino.plugin.loki;

import io.github.jeschkies.loki.client.LokiClient;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static java.lang.String.format;

final class TestLokiIntegration
        extends AbstractTestQueryFramework
{
    private LokiClient client;

    private static final DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter timestampFormatterAtEasternTime = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss'-05:00'").withZone(ZoneId.of("US/Eastern"));
    private static final DateTimeFormatter isoTimestampFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingLokiServer server = closeAfterClass(new TestingLokiServer());
        this.client = server.createLokiClient();
        return LokiQueryRunner.builder(server).build();
    }

    @Test
    void testLogsQuery()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        client.pushLogLine("line 1", end.minus(Duration.ofMinutes(10)), ImmutableMap.of("test", "logs_query"));
        client.pushLogLine("line 2", end.minus(Duration.ofMinutes(5)), ImmutableMap.of("test", "logs_query"));
        client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "logs_query"));
        client.flush();

        assertQuery(format("""
                        SELECT value FROM
                        TABLE(system.query_range(
                         '{test="logs_query"}',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES ('line 1')");

        assertQuery(format("""
                        SELECT value FROM
                        TABLE(system.query_range(
                         '{test="logs_query"}',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatterAtEasternTime.format(start), timestampFormatterAtEasternTime.format(end)),
                "VALUES ('line 1')");
    }

    @Test
    void testMetricsQuery()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        client.pushLogLine("line 1", end.minus(Duration.ofMinutes(3)), ImmutableMap.of("test", "metrics_query"));
        client.pushLogLine("line 2", end.minus(Duration.ofMinutes(2)), ImmutableMap.of("test", "metrics_query"));
        client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "metrics_query"));
        client.flush();
        assertQuery(format("""
                        SELECT value FROM
                        TABLE(system.query_range(
                         'count_over_time({test="metrics_query"}[5m])',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES (1.0)");
    }

    @Test
    void testLabels()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        client.pushLogLine("line 1", end.minus(Duration.ofMinutes(3)), ImmutableMap.of("test", "labels"));
        client.pushLogLine("line 2", end.minus(Duration.ofMinutes(2)), ImmutableMap.of("test", "labels"));
        client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "labels"));
        client.flush();
        assertQuery(format("""
                        SELECT labels['test'] FROM
                        TABLE(system.query_range(
                         'count_over_time({test="labels"}[5m])',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES ('labels')");
    }

    @Test
    void testLabelsComplex()
            throws Exception
    {
        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        client.pushLogLine("line 1", end.minus(Duration.ofMinutes(3)), ImmutableMap.of("test", "labels_complex", "service", "one"));
        client.pushLogLine("line 2", end.minus(Duration.ofMinutes(2)), ImmutableMap.of("test", "labels_complex", "service", "two"));
        client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "labels_complex", "service", "one"));
        client.flush();
        assertQuery(format("""
                        SELECT labels['service'], COUNT(*) FROM
                        TABLE(system.query_range(
                          '{test="labels_complex"}',
                          TIMESTAMP '%s',
                          TIMESTAMP '%s'
                        ))
                        GROUP BY labels['service']
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES ('one', 2.0), ('two', 1.0)");
    }

    @Test
    void testSelectTimestampLogsQuery()
            throws Exception
    {
        Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofHours(12));
        Instant end = start.plus(Duration.ofHours(4));
        Instant firstLineTimestamp = start.truncatedTo(ChronoUnit.MILLIS);

        client.pushLogLine("line 1", firstLineTimestamp, ImmutableMap.of("test", "select_timestamp_query"));
        client.pushLogLine("line 2", firstLineTimestamp.plus(Duration.ofHours(1)), ImmutableMap.of("test", "select_timestamp_query"));
        client.pushLogLine("line 3", firstLineTimestamp.plus(Duration.ofHours(2)), ImmutableMap.of("test", "select_timestamp_query"));
        client.flush();
        assertQuery(format("""
                        SELECT
                          -- H2 does not support TIMESTAMP WITH TIME ZONE so cast to VARCHAR
                          to_iso8601(timestamp), value
                        FROM
                        TABLE(system.query_range(
                         '{test="select_timestamp_query"}',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s'
                        ))
                        ORDER BY timestamp
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                format("VALUES ('%s', 'line 1')", isoTimestampFormatter.format(firstLineTimestamp)));
    }

    @Test
    void testTimestampMetricsQuery()
            throws Exception
    {
        Instant start = Instant.now().truncatedTo(ChronoUnit.HOURS).minus(Duration.ofHours(4));
        Instant end = start.plus(Duration.ofHours(3));

        this.client.pushLogLine("line 1", start.plus(Duration.ofMinutes(4)), ImmutableMap.of("test", "timestamp_metrics_query"));
        this.client.pushLogLine("line 2", start.plus(Duration.ofHours(2)), ImmutableMap.of("test", "timestamp_metrics_query"));
        this.client.pushLogLine("line 3", start.plus(Duration.ofHours(3)), ImmutableMap.of("test", "timestamp_metrics_query"));
        this.client.flush();
        assertQuery(format("""
                        SELECT to_iso8601(timestamp), value FROM
                        TABLE(system.query_range(
                         'count_over_time({test="timestamp_metrics_query"}[5m])',
                         TIMESTAMP '%s',
                         TIMESTAMP '%s',
                         300
                        ))
                        LIMIT 1
                        """, timestampFormatter.format(start), timestampFormatter.format(end)),
                "VALUES ('%s', 1.0)".formatted(isoTimestampFormatter.format(start.plus(Duration.ofMinutes(5)))));
    }

    @Test
    void testSelectFromTableFails()
    {
        assertQueryFails("SELECT * FROM default", "Loki connector does not support querying tables directly. Use the TABLE function instead.");
    }

    @Test
    void testQueryRangeInvalidArguments()
    {
        assertQueryFails(
                """
                SELECT to_iso8601(timestamp), value FROM
                TABLE(system.query_range(
                 'count_over_time({test="timestamp_metrics_query"}[5m])',
                 TIMESTAMP '2012-08-08',
                 TIMESTAMP '2012-08-09',
                 -300
                ))
                LIMIT 1
                """,
                "step must be positive");
        assertQueryFails(
                """
                SELECT to_iso8601(timestamp), value FROM
                TABLE(system.query_range(
                 'count_over_time({test="timestamp_metrics_query"}[5m])',
                 TIMESTAMP '2012-08-08',
                 TIMESTAMP '2012-08-09',
                 NULL
                ))
                LIMIT 1
                """,
                "step must be positive");
    }
}
