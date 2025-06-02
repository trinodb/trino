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
package io.trino.jdbc;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.client.Column;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static io.trino.client.CloseableLimitingIterator.limit;
import static io.trino.jdbc.ResultUtils.resultsException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TrinoResultSet
        extends AbstractTrinoResultSet
{
    private final Statement statement;
    private final StatementClient client;
    private final String queryId;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean closeStatementOnClose;

    static TrinoResultSet create(Statement statement, StatementClient client, long maxRows, Consumer<QueryStats> progressCallback, WarningsManager warningsManager)
            throws SQLException
    {
        requireNonNull(client, "client is null");
        List<Column> columns = getColumns(client, progressCallback);
        return new TrinoResultSet(statement, client, columns, maxRows, progressCallback, warningsManager);
    }

    private TrinoResultSet(Statement statement, StatementClient client, List<Column> columns, long maxRows, Consumer<QueryStats> progressCallback, WarningsManager warningsManager)
            throws SQLException
    {
        super(
                Optional.of(requireNonNull(statement, "statement is null")),
                columns,
                limit(new AsyncResultIterator(requireNonNull(client, "client is null"), progressCallback, warningsManager, Optional.empty()), maxRows));

        this.statement = statement;
        this.client = requireNonNull(client, "client is null");
        requireNonNull(progressCallback, "progressCallback is null");

        this.queryId = client.currentStatusInfo().getId();
    }

    public String getQueryId()
    {
        return queryId;
    }

    public QueryStats getStats()
    {
        return QueryStats.create(queryId, client.getStats());
    }

    void setCloseStatementOnClose()
            throws SQLException
    {
        boolean alreadyClosed;
        synchronized (this) {
            alreadyClosed = closed;
            if (!alreadyClosed) {
                closeStatementOnClose = true;
            }
        }
        if (alreadyClosed) {
            statement.close();
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        boolean closeStatement;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            closeStatement = closeStatementOnClose;
        }

        super.close();
        client.close();
        if (closeStatement) {
            statement.close();
        }
    }

    @Override
    public synchronized boolean isClosed()
            throws SQLException
    {
        return closed;
    }

    void partialCancel()
    {
        client.cancelLeafStage();
    }

    private static List<Column> getColumns(StatementClient client, Consumer<QueryStats> progressCallback)
            throws SQLException
    {
        while (client.isRunning()) {
            QueryStatusInfo results = client.currentStatusInfo();
            progressCallback.accept(QueryStats.create(results.getId(), results.getStats()));
            List<Column> columns = results.getColumns();
            if (columns != null) {
                return columns;
            }
            client.advance();
        }

        verify(client.isFinished());
        QueryStatusInfo results = client.finalStatusInfo();
        if (results.getError() == null) {
            throw new SQLException(format("Query has no columns (#%s)", results.getId()));
        }
        throw resultsException(results);
    }
}
