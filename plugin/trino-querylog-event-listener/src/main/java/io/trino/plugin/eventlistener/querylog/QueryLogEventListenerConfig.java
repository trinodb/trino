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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.Objects.requireNonNull;

public class QueryLogEventListenerConfig
{
    private final EnumSet<QueryLogEventType> loggedEvents = EnumSet.noneOf(QueryLogEventType.class);
    private String logFilePath = "querylog.log";
    private Set<String> excludedFields = Collections.emptySet();
    private DataSize maxFieldSize = DataSize.of(4, KILOBYTE);
    private Set<String> truncatedFields = Collections.emptySet();
    private DataSize truncationSizeLimit = DataSize.of(2, KILOBYTE);
    private Set<String> ignoredQueryStates = Collections.emptySet();
    private Set<String> ignoredUpdateTypes = Collections.emptySet();
    private Set<String> ignoredQueryTypes = Collections.emptySet();
    private Set<String> ignoredFailureTypes = Collections.emptySet();

    @ConfigDescription("Will log io.trino.spi.eventlistener.QueryCreatedEvent")
    @Config("querylog-event-listener.log-created")
    public QueryLogEventListenerConfig setLogCreated(boolean logCreated)
    {
        if (logCreated) {
            loggedEvents.add(QueryLogEventType.QUERY_CREATED);
        }
        return this;
    }

    public boolean getLogCreated()
    {
        return loggedEvents.contains(QueryLogEventType.QUERY_CREATED);
    }

    @ConfigDescription("Will log io.trino.spi.eventlistener.QueryCompletedEvent")
    @Config("querylog-event-listener.log-completed")
    public QueryLogEventListenerConfig setLogCompleted(boolean logCompleted)
    {
        if (logCompleted) {
            loggedEvents.add(QueryLogEventType.QUERY_COMPLETED);
        }
        return this;
    }

    public boolean getLogCompleted()
    {
        return loggedEvents.contains(QueryLogEventType.QUERY_COMPLETED);
    }

    @ConfigDescription("Will log io.trino.spi.eventlistener.QueryExecutionEvent")
    @Config("querylog-event-listener.log-executed")
    public QueryLogEventListenerConfig setLogExecuted(boolean logExecuted)
    {
        if (logExecuted) {
            loggedEvents.add(QueryLogEventType.QUERY_EXECUTED);
        }
        return this;
    }

    public boolean getLogExecuted()
    {
        return loggedEvents.contains(QueryLogEventType.QUERY_EXECUTED);
    }

    @ConfigDescription("Path to the log file where query events will be written")
    @Config("querylog-event-listener.log-file-path")
    public QueryLogEventListenerConfig setLogFilePath(String logFilePath)
    {
        this.logFilePath = logFilePath;
        return this;
    }

    public String getLogFilePath()
    {
        return logFilePath;
    }

    public EnumSet<QueryLogEventType> getLoggedEvents()
    {
        return loggedEvents.clone();
    }

    public Set<String> getExcludedFields()
    {
        return this.excludedFields;
    }

    @ConfigDescription("Comma-separated list of field names to be excluded from the log event (their value will be replaced with null). E.g.: 'payload,user'")
    @Config("querylog-event-listener.excluded-fields")
    public QueryLogEventListenerConfig setExcludedFields(Set<String> excludedFields)
    {
        this.excludedFields = requireNonNull(excludedFields, "excludedFields is null").stream()
                .filter(field -> !field.isBlank())
                .collect(toImmutableSet());
        return this;
    }

    public DataSize getMaxFieldSize()
    {
        return maxFieldSize;
    }

    @ConfigDescription("Maximum size for any field value in the log event. Larger values will be truncated. Default: 4KB")
    @Config("querylog-event-listener.max-field-size")
    public QueryLogEventListenerConfig setMaxFieldSize(DataSize maxFieldSize)
    {
        this.maxFieldSize = requireNonNull(maxFieldSize, "maxFieldSize is null");
        return this;
    }

    public Set<String> getTruncatedFields()
    {
        return this.truncatedFields;
    }

    @ConfigDescription("Comma-separated list of field names that should be truncated if they exceed the truncation size limit. E.g.: 'query,stageInfo,sourceCode'")
    @Config("querylog-event-listener.truncated-fields")
    public QueryLogEventListenerConfig setTruncatedFields(Set<String> truncatedFields)
    {
        this.truncatedFields = requireNonNull(truncatedFields, "truncatedFields is null").stream()
                .filter(field -> !field.isBlank())
                .collect(toImmutableSet());
        return this;
    }

    public DataSize getTruncationSizeLimit()
    {
        return truncationSizeLimit;
    }

    @ConfigDescription("Maximum size in bytes for fields specified in truncated-fields. Values exceeding this limit will be truncated with [TRUNCATED] suffix. Default: 2KB")
    @Config("querylog-event-listener.truncation-size-limit")
    public QueryLogEventListenerConfig setTruncationSizeLimit(DataSize truncationSizeLimit)
    {
        this.truncationSizeLimit = requireNonNull(truncationSizeLimit, "truncationSizeLimit is null");
        return this;
    }

    public Set<String> getIgnoredQueryStates()
    {
        return this.ignoredQueryStates;
    }

    @ConfigDescription("Comma-separated list of query states to ignore when logging. E.g.: 'RUNNING,QUEUED,WAITING'")
    @Config("querylog-event-listener.ignored-query-states")
    public QueryLogEventListenerConfig setIgnoredQueryStates(Set<String> ignoredQueryStates)
    {
        this.ignoredQueryStates = requireNonNull(ignoredQueryStates, "ignoredQueryStates is null").stream()
                .filter(state -> !state.isBlank())
                .collect(toImmutableSet());
        return this;
    }

    public Set<String> getIgnoredUpdateTypes()
    {
        return this.ignoredUpdateTypes;
    }

    @ConfigDescription("Comma-separated list of update types to ignore when logging. E.g.: 'INSERT,UPDATE,DELETE'")
    @Config("querylog-event-listener.ignored-update-types")
    public QueryLogEventListenerConfig setIgnoredUpdateTypes(Set<String> ignoredUpdateTypes)
    {
        this.ignoredUpdateTypes = requireNonNull(ignoredUpdateTypes, "ignoredUpdateTypes is null").stream()
                .filter(type -> !type.isBlank())
                .collect(toImmutableSet());
        return this;
    }

    public Set<String> getIgnoredQueryTypes()
    {
        return this.ignoredQueryTypes;
    }

    @ConfigDescription("Comma-separated list of query types to ignore when logging. E.g.: 'DML,DDL,UTILITY,EXPLAIN'")
    @Config("querylog-event-listener.ignored-query-types")
    public QueryLogEventListenerConfig setIgnoredQueryTypes(Set<String> ignoredQueryTypes)
    {
        this.ignoredQueryTypes = requireNonNull(ignoredQueryTypes, "ignoredQueryTypes is null").stream()
                .filter(type -> !type.isBlank())
                .collect(toImmutableSet());
        return this;
    }

    public Set<String> getIgnoredFailureTypes()
    {
        return this.ignoredFailureTypes;
    }

    @ConfigDescription("Comma-separated list of failure types to ignore when logging. E.g.: 'USER_ERROR,INTERNAL_ERROR'")
    @Config("querylog-event-listener.ignored-failure-types")
    public QueryLogEventListenerConfig setIgnoredFailureTypes(Set<String> ignoredFailureTypes)
    {
        this.ignoredFailureTypes = requireNonNull(ignoredFailureTypes, "ignoredFailureTypes is null").stream()
                .filter(type -> !type.isBlank())
                .collect(toImmutableSet());
        return this;
    }
}
