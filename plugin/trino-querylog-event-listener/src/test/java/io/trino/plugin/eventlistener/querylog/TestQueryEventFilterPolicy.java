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

import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryExecutionEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryEventFilterPolicy
{
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

    private static class TestQueryMetadata
            implements QueryMetadata
    {
        private final String queryState;
        private final Optional<String> updateType;
        private final Optional<String> queryType;

        TestQueryMetadata(String queryState, Optional<String> updateType, Optional<String> queryType)
        {
            this.queryState = queryState;
            this.updateType = updateType;
            this.queryType = queryType;
        }

        @Override
        public String getQueryId()
        {
            return "test";
        }

        @Override
        public String getQueryState()
        {
            return queryState;
        }

        @Override
        public Optional<String> getUpdateType()
        {
            return updateType;
        }

        @Override
        public Optional<String> getQueryType()
        {
            return queryType;
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

    private static class TestQueryFailure
            implements QueryFailure
    {
        private final Optional<String> failureType;

        TestQueryFailure(Optional<String> failureType)
        {
            this.failureType = failureType;
        }

        @Override
        public Optional<String> getFailureType()
        {
            return failureType;
        }

        @Override
        public String getFailureMessage()
        {
            return "test";
        }

        @Override
        public Optional<String> getFailureTaskId()
        {
            return Optional.empty();
        }
    }

    @Test
    public void testShouldLogQueryCreatedWithEmptyFilters()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of(),
                Set.of(),
                Set.of(),
                Set.of());

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED", Optional.empty(), Optional.empty()));
        assertThat(filter.shouldLogQueryCreated(event)).isTrue();
    }

    @Test
    public void testIgnoreQueryStateCreated()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of("CREATED"),
                Set.of(),
                Set.of(),
                Set.of());

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED", Optional.empty(), Optional.empty()));

        assertThat(filter.shouldLogQueryCreated(event)).isFalse();
    }

    @Test
    public void testIgnoreUpdateTypeCreated()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of(),
                Set.of("INSERT"),
                Set.of(),
                Set.of());

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED", Optional.of("INSERT"), Optional.empty()));

        assertThat(filter.shouldLogQueryCreated(event)).isFalse();
    }

    @Test
    public void testIgnoreQueryTypeCreated()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of(),
                Set.of(),
                Set.of("UTILITY"),
                Set.of());

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED", Optional.empty(), Optional.of("UTILITY")));

        assertThat(filter.shouldLogQueryCreated(event)).isFalse();
    }

    @Test
    public void testShouldLogQueryCreatedWhenNotIgnored()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of("RUNNING"),
                Set.of("DELETE"),
                Set.of("DDL"),
                Set.of());

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED", Optional.of("INSERT"), Optional.of("DML")));

        assertThat(filter.shouldLogQueryCreated(event)).isTrue();
    }

    @Test
    public void testShouldLogQueryCompletedWithFailureType()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of(),
                Set.of(),
                Set.of(),
                Set.of("USER_ERROR"));

        QueryCompletedEvent event = new TestQueryCompletedEvent(
                new TestQueryMetadata("FAILED", Optional.empty(), Optional.empty()),
                Optional.of(new TestQueryFailure(Optional.of("USER_ERROR"))));

        assertThat(filter.shouldLogQueryCompleted(event)).isFalse();
    }

    @Test
    public void testShouldLogQueryCompletedSuccessful()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of(),
                Set.of(),
                Set.of(),
                Set.of("USER_ERROR"));

        QueryCompletedEvent event = new TestQueryCompletedEvent(
                new TestQueryMetadata("FINISHED", Optional.empty(), Optional.empty()),
                Optional.empty());

        assertThat(filter.shouldLogQueryCompleted(event)).isTrue();
    }

    @Test
    public void testShouldLogQueryExecuted()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of("RUNNING"),
                Set.of(),
                Set.of(),
                Set.of());

        QueryExecutionEvent event = new TestQueryExecutionEvent(
                new TestQueryMetadata("FINISHED", Optional.empty(), Optional.empty()));

        assertThat(filter.shouldLogQueryExecuted(event)).isTrue();
    }

    @Test
    public void testShouldNotLogQueryExecutedWithIgnoredState()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of("FINISHED"),
                Set.of(),
                Set.of(),
                Set.of());

        QueryExecutionEvent event = new TestQueryExecutionEvent(
                new TestQueryMetadata("FINISHED", Optional.empty(), Optional.empty()));

        assertThat(filter.shouldLogQueryExecuted(event)).isFalse();
    }

    @Test
    public void testMultipleFiltersAppliedCreated()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of("RUNNING", "QUEUED"),
                Set.of("INSERT", "UPDATE"),
                Set.of("UTILITY", "DDL"),
                Set.of());

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED", Optional.of("SELECT"), Optional.of("DML")));

        // Should pass all filters
        assertThat(filter.shouldLogQueryCreated(event)).isTrue();
    }

    @Test
    public void testOptionalFieldsNotPresent()
    {
        QueryEventFilterPolicy filter = new QueryEventFilterPolicy(
                Set.of(),
                Set.of("INSERT"),
                Set.of("UTILITY"),
                Set.of());

        QueryCreatedEvent event = new TestQueryCreatedEvent(
                new TestQueryMetadata("CREATED", Optional.empty(), Optional.empty()));

        // Should not filter since optional fields are not present
        assertThat(filter.shouldLogQueryCreated(event)).isTrue();
    }
}
