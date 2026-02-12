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
package io.trino.plugin.eventlistener.querylog;

import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryExecutionEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestQueryLogEventListener
{
    private static class TestQueryMetadata
            implements QueryCreatedEvent.QueryMetadata
    {
        private final String queryState;

        TestQueryMetadata(String queryState)
        {
            this.queryState = queryState;
        }

        @Override
        public String getQueryId()
        {
            return "test-id";
        }

        @Override
        public String getQueryState()
        {
            return queryState;
        }

        @Override
        public Optional<String> getUpdateType()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getQueryType()
        {
            return Optional.empty();
        }

        @Override
        public Instant getCreateTime()
        {
            return Instant.now();
        }

        @Override
        public Optional<Instant> getExecutionStartTime()
        {
            return Optional.empty();
        }

        @Override
        public Optional<Instant> getEndTime()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getUser()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getCatalog()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getSchema()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getRemoteClientAddress()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getClientInfo()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getClientTags()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getSessionProperties()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getPreparedQuery()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getQuery()
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getPlan()
        {
            return Optional.empty();
        }
    }

    private static class TestQueryCreatedEvent
            implements QueryCreatedEvent
    {
        private final QueryMetadata metadata;

        TestQueryCreatedEvent(QueryMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public QueryMetadata getMetadata()
        {
            return metadata;
        }
    }

    private static class TestQueryCompletedEvent
            implements QueryCompletedEvent
    {
        private final QueryMetadata metadata;
        private final Optional<QueryFailure> failure;

        TestQueryCompletedEvent(QueryMetadata metadata, Optional<QueryFailure> failure)
        {
            this.metadata = metadata;
            this.failure = failure;
        }

        @Override
        public QueryMetadata getMetadata()
        {
            return metadata;
        }

        @Override
        public Optional<QueryFailure> getFailure()
        {
            return failure;
        }

        @Override
        public long getExecutionStartTime()
        {
            return 0;
        }

        @Override
        public long getExecutionEndTime()
        {
            return 0;
        }
    }

    private static class TestQueryExecutionEvent
            implements QueryExecutionEvent
    {
        private final QueryMetadata metadata;

        TestQueryExecutionEvent(QueryMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public QueryMetadata getMetadata()
        {
            return metadata;
        }

        @Override
        public RuntimeStats getRuntimeStats()
        {
            return null;
        }
    }

    private static class SimpleJsonCodec<T>
            implements JsonCodec<T>
    {
        @Override
        public String toJson(T object)
        {
            return "{\"id\":\"test\"}";
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

    @Test
    public void testQueryCreatedEventLogged()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCreated(true);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED"));

        assertThatCode(() -> listener.queryCreated(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCreatedEventIgnoredByFilter()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCreated(true);
        config.setIgnoredQueryStates(Set.of("CREATED"));

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED"));

        assertThatCode(() -> listener.queryCreated(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCreatedEventDisabled()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCreated(false);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED"));
        assertThatCode(() -> listener.queryCreated(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCompletedEventLogged()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCompleted(true);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCompletedEvent event = new TestQueryCompletedEvent(
                new TestQueryMetadata("FINISHED"),
                Optional.empty());

        assertThatCode(() -> listener.queryCompleted(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCompletedEventDisabled()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCompleted(false);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCompletedEvent event = new TestQueryCompletedEvent(
                new TestQueryMetadata("FINISHED"),
                Optional.empty());
        assertThatCode(() -> listener.queryCompleted(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryExecutedEventLogged()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogExecuted(true);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryExecutionEvent event = new TestQueryExecutionEvent(
                new TestQueryMetadata("RUNNING"));

        assertThatCode(() -> listener.queryExecuted(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryExecutedEventDisabled()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogExecuted(false);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryExecutionEvent event = new TestQueryExecutionEvent(
                new TestQueryMetadata("RUNNING"));
        assertThatCode(() -> listener.queryExecuted(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCreatedWithAllConfigurations()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCreated(true);
        config.setLogCompleted(true);
        config.setLogExecuted(true);
        config.setExcludedFields(Set.of("user", "password"));
        config.setMaxFieldSize(DataSize.of(8, KILOBYTE));
        config.setTruncatedFields(Set.of("query", "plan"));
        config.setTruncationSizeLimit(DataSize.of(2, KILOBYTE));
        config.setIgnoredQueryStates(Set.of());
        config.setIgnoredUpdateTypes(Set.of());
        config.setIgnoredQueryTypes(Set.of());
        config.setIgnoredFailureTypes(Set.of());

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED"));

        assertThatCode(() -> listener.queryCreated(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCreatedWithException()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new JsonCodec<QueryCreatedEvent>()
        {
            @Override
            public String toJson(QueryCreatedEvent object)
            {
                throw new RuntimeException("Serialization error");
            }

            @Override
            public QueryCreatedEvent fromJson(String json)
            {
                return null;
            }

            @Override
            public QueryCreatedEvent fromBytes(byte[] json)
            {
                return null;
            }
        };
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCreated(true);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED"));

        // Should not throw, just log error
        assertThatCode(() -> listener.queryCreated(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryCompletedWithException()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new JsonCodec<QueryCompletedEvent>()
        {
            @Override
            public String toJson(QueryCompletedEvent object)
            {
                throw new RuntimeException("Serialization error");
            }

            @Override
            public QueryCompletedEvent fromJson(String json)
            {
                return null;
            }

            @Override
            public QueryCompletedEvent fromBytes(byte[] json)
            {
                return null;
            }
        };
        JsonCodec<QueryExecutionEvent> executedCodec = new SimpleJsonCodec<>();

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogCompleted(true);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryCompletedEvent event = new TestQueryCompletedEvent(
                new TestQueryMetadata("FINISHED"),
                Optional.empty());

        // Should not throw, just log error
        assertThatCode(() -> listener.queryCompleted(event)).doesNotThrowAnyException();
    }

    @Test
    public void testQueryExecutedWithException()
    {
        JsonCodec<QueryCreatedEvent> createdCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryCompletedEvent> completedCodec = new SimpleJsonCodec<>();
        JsonCodec<QueryExecutionEvent> executedCodec = new JsonCodec<QueryExecutionEvent>()
        {
            @Override
            public String toJson(QueryExecutionEvent object)
            {
                throw new RuntimeException("Serialization error");
            }

            @Override
            public QueryExecutionEvent fromJson(String json)
            {
                return null;
            }

            @Override
            public QueryExecutionEvent fromBytes(byte[] json)
            {
                return null;
            }
        };

        QueryLogEventListenerConfig config = new QueryLogEventListenerConfig();
        config.setLogExecuted(true);

        QueryLogEventListener listener = new QueryLogEventListener(
                completedCodec,
                createdCodec,
                executedCodec,
                config);

        QueryExecutionEvent event = new TestQueryExecutionEvent(
                new TestQueryMetadata("RUNNING"));

        // Should not throw, just log error
        assertThatCode(() -> listener.queryExecuted(event)).doesNotThrowAnyException();
    }
}

