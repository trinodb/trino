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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.procedure.Procedure.Argument;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static io.prestosql.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.prestosql.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class KillQueryProcedure
{
    private static final MethodHandle KILL_QUERY = methodHandle(KillQueryProcedure.class, "killQuery", String.class, String.class);

    private final Optional<DispatchManager> dispatchManager;

    @Inject
    public KillQueryProcedure(Optional<DispatchManager> dispatchManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
    }

    @UsedByGeneratedCode
    public void killQuery(String queryId, String message)
    {
        QueryId query = parseQueryId(queryId);

        try {
            checkState(dispatchManager.isPresent(), "No dispatch manager is set. kill_query procedure should be executed on coordinator.");
            DispatchQuery dispatchQuery = dispatchManager.get().getQuery(query);

            // check before killing to provide the proper error message (this is racy)
            if (dispatchQuery.isDone()) {
                throw new PrestoException(NOT_SUPPORTED, "Target query is not running: " + queryId);
            }

            dispatchQuery.fail(createKillQueryException(message));

            // verify if the query was killed (if not, we lost the race)
            checkState(dispatchQuery.isDone(), "Failure to fail the query: %s", query);
            if (!ADMINISTRATIVELY_KILLED.toErrorCode().equals(dispatchQuery.getErrorCode().orElse(null))) {
                throw new PrestoException(NOT_SUPPORTED, "Target query is not running: " + queryId);
            }
        }
        catch (NoSuchElementException e) {
            throw new PrestoException(NOT_FOUND, "Target query not found: " + queryId);
        }
    }

    public Procedure getProcedure()
    {
        return new Procedure(
                "runtime",
                "kill_query",
                ImmutableList.<Argument>builder()
                        .add(new Argument("query_id", VARCHAR))
                        .add(new Argument("message", VARCHAR))
                        .build(),
                KILL_QUERY.bindTo(this));
    }

    public static PrestoException createKillQueryException(String message)
    {
        return new PrestoException(ADMINISTRATIVELY_KILLED, "Query killed. " +
                (isNullOrEmpty(message) ? "No message provided." : "Message: " + message));
    }

    public static PrestoException createPreemptQueryException(String message)
    {
        return new PrestoException(ADMINISTRATIVELY_PREEMPTED, "Query preempted. " +
                (isNullOrEmpty(message) ? "No message provided." : "Message: " + message));
    }

    private static QueryId parseQueryId(String queryId)
    {
        try {
            return QueryId.valueOf(queryId);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, e);
        }
    }
}
