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
package io.trino.tests.product.loki;

import com.google.common.collect.ImmutableMap;
import io.github.jeschkies.loki.client.LokiClient;
import io.github.jeschkies.loki.client.LokiClientConfig;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.time.ZoneOffset.UTC;

@ProductTest
@RequiresEnvironment(LokiEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Loki
@TestGroup.ProfileSpecificTests
class TestLoki
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS'Z'").withZone(UTC);

    @Test
    void testQueryRange(LokiEnvironment env)
            throws Exception
    {
        LokiClient client = new LokiClient(new LokiClientConfig(URI.create(env.getLokiHttpUrl()), Duration.ofSeconds(10)));

        Instant start = Instant.now().minus(Duration.ofHours(3));
        Instant end = start.plus(Duration.ofHours(2));

        client.pushLogLine("line 1", end.minus(Duration.ofMinutes(10)), ImmutableMap.of("test", "logs_query"));
        client.pushLogLine("line 2", end.minus(Duration.ofMinutes(5)), ImmutableMap.of("test", "logs_query"));
        client.pushLogLine("line 3", end.minus(Duration.ofMinutes(1)), ImmutableMap.of("test", "logs_query"));
        client.flush();

        assertThat(env.executeTrino("SELECT value FROM TABLE(loki.system.query_range(" +
                "'{test=\"logs_query\"}'," +
                "TIMESTAMP '" + TIMESTAMP_FORMATTER.format(start) + "'," +
                "TIMESTAMP '" + TIMESTAMP_FORMATTER.format(end) + "'))" +
                "LIMIT 1"))
                .containsOnly(row("line 1"));
    }
}
