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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.FullConnectorSession;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQuery;
import io.trino.security.AccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import java.lang.invoke.MethodHandle;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.security.AccessControlUtil.checkCanKillQueryOwnedBy;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class KillQueryProcedure
{
    private static final MethodHandle KILL_QUERY = methodHandle(KillQueryProcedure.class, "killQuery", String.class, String.class, ConnectorSession.class);

    private final Optional<DispatchManager> dispatchManager;
    private final AccessControl accessControl;

    @Inject
    public KillQueryProcedure(Optional<DispatchManager> dispatchManager, AccessControl accessControl)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @UsedByGeneratedCode
    public void killQuery(String queryId, String message, ConnectorSession session)
    {
        checkProcedureArgument(queryId != null, "query_id cannot be null");

        QueryId query = parseQueryId(queryId);

        try {
            checkState(dispatchManager.isPresent(), "No dispatch manager is set. kill_query procedure should be executed on coordinator.");
            DispatchQuery dispatchQuery = dispatchManager.get().getQuery(query);

            checkCanKillQueryOwnedBy(((FullConnectorSession) session).getSession().getIdentity(), dispatchQuery.getSession().getIdentity(), accessControl);

            // check before killing to provide the proper error message (this is racy)
            if (dispatchQuery.isDone()) {
                throw new TrinoException(NOT_SUPPORTED, "Target query is not running: " + queryId);
            }

            dispatchQuery.fail(createKillQueryException(message));

            // verify if the query was killed (if not, we lost the race)
            checkState(dispatchQuery.isDone(), "Failure to fail the query: %s", query);
            if (!ADMINISTRATIVELY_KILLED.toErrorCode().equals(dispatchQuery.getErrorCode().orElse(null))) {
                throw new TrinoException(NOT_SUPPORTED, "Target query is not running: " + queryId);
            }
        }
        catch (NoSuchElementException e) {
            throw new TrinoException(NOT_FOUND, "Target query not found: " + queryId);
        }
    }

    public Procedure getProcedure()
    {
        return new Procedure(
                "runtime",
                "kill_query",
                ImmutableList.<Argument>builder()
                        .add(new Argument("QUERY_ID", VARCHAR))
                        .add(new Argument("MESSAGE", VARCHAR, false, null))
                        .build(),
                KILL_QUERY.bindTo(this));
    }

    public static TrinoException createKillQueryException(String message)
    {
        return new TrinoException(ADMINISTRATIVELY_KILLED, "Query killed. " +
                (isNullOrEmpty(message) ? "No message provided." : "Message: " + message));
    }

    public static TrinoException createPreemptQueryException(String message)
    {
        return new TrinoException(ADMINISTRATIVELY_PREEMPTED, "Query preempted. " +
                (isNullOrEmpty(message) ? "No message provided." : "Message: " + message));
    }

    private static QueryId parseQueryId(String queryId)
    {
        try {
            return QueryId.valueOf(queryId);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, e);
        }
    }
}
