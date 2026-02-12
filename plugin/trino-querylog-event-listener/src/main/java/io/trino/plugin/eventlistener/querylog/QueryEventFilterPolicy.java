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

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Utility class to filter query events based on configured ignore patterns.
 * <p>
 * Allows events to be skipped from logging based on:
 * - Query state (RUNNING, QUEUED, FINISHED, etc.)
 * - Update type (INSERT, UPDATE, DELETE, etc.)
 * - Query type (DML, DDL, UTILITY, EXPLAIN, etc.)
 * - Failure type (USER_ERROR, INTERNAL_ERROR, EXTERNAL, etc.)
 */
public class QueryEventFilter
{
    private final Set<String> ignoredQueryStates;
    private final Set<String> ignoredUpdateTypes;
    private final Set<String> ignoredQueryTypes;
    private final Set<String> ignoredFailureTypes;

    public QueryEventFilter(
            Set<String> ignoredQueryStates,
            Set<String> ignoredUpdateTypes,
            Set<String> ignoredQueryTypes,
            Set<String> ignoredFailureTypes)
    {
        this.ignoredQueryStates = requireNonNull(ignoredQueryStates, "ignoredQueryStates is null");
        this.ignoredUpdateTypes = requireNonNull(ignoredUpdateTypes, "ignoredUpdateTypes is null");
        this.ignoredQueryTypes = requireNonNull(ignoredQueryTypes, "ignoredQueryTypes is null");
        this.ignoredFailureTypes = requireNonNull(ignoredFailureTypes, "ignoredFailureTypes is null");
    }

    /**
     * Check if a QueryCreatedEvent should be logged (not ignored).
     * Optimized: avoids isEmpty() checks and caches metadata calls.
     */
    public boolean shouldLogQueryCreated(QueryCreatedEvent event)
    {
        QueryCreatedEvent.QueryMetadata metadata = event.getMetadata();

        // Check query state - fast path since it's required
        if (ignoredQueryStates.contains(metadata.getQueryState())) {
            return false;
        }

        // Check update type if present
        Optional<String> updateType = metadata.getUpdateType();
        if (updateType.isPresent() && ignoredUpdateTypes.contains(updateType.get())) {
            return false;
        }

        // Check query type if present
        Optional<String> queryType = metadata.getQueryType();
        if (queryType.isPresent() && ignoredQueryTypes.contains(queryType.get())) {
            return false;
        }

        return true;
    }

    /**
     * Check if a QueryCompletedEvent should be logged (not ignored).
     * Optimized: avoids isEmpty() checks and caches metadata calls.
     */
    public boolean shouldLogQueryCompleted(QueryCompletedEvent event)
    {
        QueryCompletedEvent.QueryMetadata metadata = event.getMetadata();

        // Check query state
        if (ignoredQueryStates.contains(metadata.getQueryState())) {
            return false;
        }

        // Check update type if present
        Optional<String> updateType = metadata.getUpdateType();
        if (updateType.isPresent() && ignoredUpdateTypes.contains(updateType.get())) {
            return false;
        }

        // Check query type if present
        Optional<String> queryType = metadata.getQueryType();
        if (queryType.isPresent() && ignoredQueryTypes.contains(queryType.get())) {
            return false;
        }

        // Check failure type if present
        Optional<QueryCompletedEvent.QueryFailure> failure = event.getFailure();
        if (failure.isPresent()) {
            Optional<String> failureType = failure.get().getFailureType();
            if (failureType.isPresent() && ignoredFailureTypes.contains(failureType.get())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if a QueryExecutionEvent should be logged (not ignored).
     * Optimized: avoids isEmpty() checks and caches metadata calls.
     */
    public boolean shouldLogQueryExecuted(QueryExecutionEvent event)
    {
        QueryExecutionEvent.QueryMetadata metadata = event.getMetadata();

        // Check query state
        if (ignoredQueryStates.contains(metadata.getQueryState())) {
            return false;
        }

        // Check update type if present
        Optional<String> updateType = metadata.getUpdateType();
        if (updateType.isPresent() && ignoredUpdateTypes.contains(updateType.get())) {
            return false;
        }

        // Check query type if present
        Optional<String> queryType = metadata.getQueryType();
        if (queryType.isPresent() && ignoredQueryTypes.contains(queryType.get())) {
            return false;
        }

        return true;
    }
}
