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
package io.trino.plugin.eventlistener.logger;

import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;

public class TestLoggerEventListener
{
    @Test
    public void testQueryCreatedEventLogged()
    {
        LoggerEventListener listener = new LoggerEventListener(new SimpleJsonCodec<>(), new SimpleJsonCodec<>(), new LoggerEventListenerConfig().setLogCreated(true));
        QueryCreatedEvent event = new QueryCreatedEvent(Instant.now(), createContext(), createMetadata("QUEUED"));
        assertThatCode(() -> listener.queryCreated(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCompletedEventLogged()
    {
        LoggerEventListener listener = new LoggerEventListener(new SimpleJsonCodec<>(), new SimpleJsonCodec<>(), new LoggerEventListenerConfig().setLogCompleted(true));
        QueryCompletedEvent event = new QueryCompletedEvent(
                createMetadata("FINISHED"),
                createStatistics(),
                createContext(),
                new QueryIOMetadata(List.of(), Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                List.of(new TrinoWarning(new WarningCode(1, "WARN"), "warn")),
                Instant.now(),
                Instant.now(),
                Instant.now());
        assertThatCode(() -> listener.queryCompleted(event)).doesNotThrowAnyException();
    }

    private static QueryContext createContext()
    {
        return new QueryContext(
                "user",
                "user",
                Set.of(),
                Optional.empty(),
                Set.of(),
                Set.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Set.of(),
                Set.of(),
                Optional.empty(),
                "UTC",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of(),
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
                "127.0.0.1",
                "test",
                "test",
                Optional.empty(),
                "TASK");
    }

    private static QueryMetadata createMetadata(String state)
    {
        return new QueryMetadata(
                "query-id",
                Optional.empty(),
                Optional.empty(),
                "SELECT 1",
                Optional.empty(),
                Optional.empty(),
                state,
                List.of(),
                List.of(),
                URI.create("http://localhost/query-id"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static QueryStatistics createStatistics()
    {
        return new QueryStatistics(
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                List.of(),
                0,
                true,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                Optional.empty());
    }

    private static class SimpleJsonCodec<T>
            implements JsonCodec<T>
    {
        @Override
        public String toJson(T object)
        {
            return "{\"query\":\"SELECT * FROM very_long_table_name\"}";
        }

        @Override
        public T fromJson(String json)
        {
            return null;
        }

        @Override
        public T fromBytes(byte[] json)
        {
            return null;
        }
    }
}
