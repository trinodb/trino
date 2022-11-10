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
package io.trino.execution;

import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.EventsCollector.QueryEvents;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryFailedException;
import org.intellij.lang.annotations.Language;

import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.fail;

class EventsAwaitingQueries
{
    private final EventsCollector eventsCollector;

    private final DistributedQueryRunner queryRunner;

    EventsAwaitingQueries(EventsCollector eventsCollector, DistributedQueryRunner queryRunner)
    {
        this.eventsCollector = requireNonNull(eventsCollector, "eventsCollector is null");
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
    }

    MaterializedResultWithEvents runQueryAndWaitForEvents(@Language("SQL") String sql, Session session)
            throws Exception
    {
        return runQueryAndWaitForEvents(sql, session, Optional.empty());
    }

    MaterializedResultWithEvents runQueryAndWaitForEvents(@Language("SQL") String sql, Session session, boolean requireAnonymizedPlan)
            throws Exception
    {
        eventsCollector.setRequiresAnonymizedPlan(requireAnonymizedPlan);
        return runQueryAndWaitForEvents(sql, session, Optional.empty());
    }

    MaterializedResultWithEvents runQueryAndWaitForEvents(@Language("SQL") String sql, Session session, Optional<String> expectedExceptionRegEx)
            throws Exception
    {
        QueryId queryId = null;
        MaterializedResult result = null;
        try {
            MaterializedResultWithQueryId materializedResultWithQueryId = queryRunner.executeWithQueryId(session, sql);
            queryId = materializedResultWithQueryId.getQueryId();
            result = materializedResultWithQueryId.getResult();
        }
        catch (RuntimeException exception) {
            if (expectedExceptionRegEx.isPresent()) {
                String regex = expectedExceptionRegEx.get();
                if (!nullToEmpty(exception.getMessage()).matches(regex)) {
                    fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql), exception);
                }
            }
            else {
                throw exception;
            }
            if (exception instanceof QueryFailedException) {
                queryId = ((QueryFailedException) exception).getQueryId();
            }
        }
        if (queryId == null) {
            return null;
        }

        QueryEvents queryEvents = eventsCollector.getQueryEvents(queryId);
        queryEvents.waitForQueryCompletion(new Duration(30, SECONDS));

        // Sleep some more so extraneous, unexpected events can be recorded too.
        // This is not rock solid but improves effectiveness on detecting duplicate events.
        SECONDS.sleep(1);

        return new MaterializedResultWithEvents(result, queryEvents);
    }

    public static class MaterializedResultWithEvents
    {
        private final MaterializedResult materializedResult;
        private final QueryEvents queryEvents;

        public MaterializedResultWithEvents(MaterializedResult materializedResult, QueryEvents queryEvents)
        {
            this.materializedResult = materializedResult;
            this.queryEvents = requireNonNull(queryEvents, "queryEvents is null");
        }

        public MaterializedResult getMaterializedResult()
        {
            return materializedResult;
        }

        public QueryEvents getQueryEvents()
        {
            return queryEvents;
        }
    }
}
