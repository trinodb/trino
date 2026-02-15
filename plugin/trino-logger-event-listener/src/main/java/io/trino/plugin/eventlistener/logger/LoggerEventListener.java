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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;

import static java.util.Objects.requireNonNull;

/**
 * Event listener that logs query events to a file using Airlift Logger.
 * <p>
 * The listener logs all configured event types (QUERY_CREATED, QUERY_COMPLETED)
 * as JSON to the configured log file path.
 * <p>
 * Supports:
 * - Excluding specific fields from log output
 * - Truncating large field values to prevent excessive log sizes
 */
public class LoggerEventListener
        implements EventListener
{
    private final Logger log = Logger.get(LoggerEventListener.class);

    private final JsonCodec<QueryCompletedEvent> queryCompletedEventJsonCodec;
    private final JsonCodec<QueryCreatedEvent> queryCreatedEventJsonCodec;
    private final QueryEventFieldFilter fieldFilter;
    private final QueryEventFilterPolicy filterPolicy;

    private final boolean logCreated;
    private final boolean logCompleted;

    @Inject
    public LoggerEventListener(
            JsonCodec<QueryCompletedEvent> queryCompletedEventJsonCodec,
            JsonCodec<QueryCreatedEvent> queryCreatedEventJsonCodec,
            LoggerEventListenerConfig config)
    {
        this.queryCompletedEventJsonCodec = requireNonNull(queryCompletedEventJsonCodec, "queryCompletedEventJsonCodec is null");
        this.queryCreatedEventJsonCodec = requireNonNull(queryCreatedEventJsonCodec, "queryCreatedEventJsonCodec is null");
        this.fieldFilter = new QueryEventFieldFilter(
                config.getExcludedFields(),
                config.getMaxFieldSize(),
                config.getTruncatedFields(),
                config.getTruncationSizeLimit());
        this.filterPolicy = new QueryEventFilterPolicy(
                config.getIgnoredQueryStates(),
                config.getIgnoredUpdateTypes(),
                config.getIgnoredQueryTypes(),
                config.getIgnoredFailureTypes());

        this.logCreated = config.getLogCreated();
        this.logCompleted = config.getLogCompleted();

        log.info("LoggerEventListener initialized with logCreated=%s, logCompleted=%s, excludedFields=%s, maxFieldSize=%s, truncatedFields=%s, truncationSizeLimit=%s, ignoredQueryStates=%s, ignoredUpdateTypes=%s, ignoredQueryTypes=%s, ignoredFailureTypes=%s",
                logCreated, logCompleted, config.getExcludedFields(), config.getMaxFieldSize(),
                config.getTruncatedFields(), config.getTruncationSizeLimit(), config.getIgnoredQueryStates(),
                config.getIgnoredUpdateTypes(), config.getIgnoredQueryTypes(), config.getIgnoredFailureTypes());
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (logCreated && filterPolicy.shouldLogQueryCreated(queryCreatedEvent)) {
            try {
                String json = queryCreatedEventJsonCodec.toJson(queryCreatedEvent);
                String filtered = fieldFilter.applyFiltering(json);
                log.info("QUERY_CREATED: %s", filtered);
            }
            catch (Exception e) {
                log.error(e, "Failed to log query created event");
            }
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (logCompleted && filterPolicy.shouldLogQueryCompleted(queryCompletedEvent)) {
            try {
                String json = queryCompletedEventJsonCodec.toJson(queryCompletedEvent);
                String filtered = fieldFilter.applyFiltering(json);
                log.info("QUERY_COMPLETED: %s", filtered);
            }
            catch (Exception e) {
                log.error(e, "Failed to log query completed event");
            }
        }
    }
}
