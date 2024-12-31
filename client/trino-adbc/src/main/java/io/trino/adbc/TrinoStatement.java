package io.trino.adbc;

import com.google.common.collect.ImmutableMap;
import io.trino.client.*;
import io.trino.client.spooling.SegmentLoader;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.trino.client.StatementClientFactory.newStatementClient;
import static java.util.Objects.requireNonNull;


public class TrinoStatement implements AdbcStatement {

    private String query;

    private final AtomicLong maxRows = new AtomicLong();
    private final AtomicInteger queryTimeoutSeconds = new AtomicInteger();
    private final AtomicInteger fetchSize = new AtomicInteger();
    private final AtomicBoolean closeOnCompletion = new AtomicBoolean();
    private final AtomicReference<TrinoConnection> connection;
    private final Consumer<TrinoStatement> onClose;
    private final AtomicReference<StatementClient> executingClient = new AtomicReference<>();
    private final AtomicReference<QueryResult> currentResult = new AtomicReference<>();
    //private final AtomicReference<Optional<WarningsManager>> currentWarningsManager = new AtomicReference<>(Optional.empty());
    private final AtomicLong currentUpdateCount = new AtomicLong(-1);
    private final AtomicReference<String> currentUpdateType = new AtomicReference<>();
    private final AtomicReference<Optional<Consumer<QueryStats>>> progressCallback = new AtomicReference<>(Optional.empty());
    private final Consumer<QueryStats> progressConsumer = value -> progressCallback.get().ifPresent(callback -> callback.accept(value));

    public TrinoStatement(TrinoConnection connection, Consumer<TrinoStatement> onClose){
        this.connection = new AtomicReference<>(requireNonNull(connection, "connection is null"));
        this.onClose = requireNonNull(onClose, "onClose is null");
    }

    @Override
    public void setSqlQuery(String query) throws AdbcException {
        this.query = query;
    }

    @Override
    public QueryResult executeQuery() throws AdbcException {
        requireNonNull(query, "query is null");
        if (connection().shouldStartTransaction()) {
            try {
                internalExecute(connection().getStartTransactionSql());
            } catch (IOException e) {
                throw AdbcException.io(e.getMessage())
                        .withCause(e);
            }
        }
        try {
            internalExecute(query);
        } catch (IOException e) {
            throw AdbcException.io(e.getMessage())
                    .withCause(e);
        }
        return currentResult.get();
    }

    protected final void checkOpen()
            throws AdbcException
    {
        connection();
    }

    final boolean internalExecute(String sql)
            throws AdbcException, IOException {
        clearCurrentResults();
        checkOpen();

        StatementClient client = null;
        QueryResult queryResult = null;
        try {
            client = connection().startQuery(sql, getStatementSessionProperties());
            SegmentLoader segmentLoader = connection().getSegmentLoader();
            if (client.isFinished()) {
                QueryStatusInfo finalStatusInfo = client.finalStatusInfo();
                if (finalStatusInfo.getError() != null) {
                    QueryError queryError = requireNonNull(finalStatusInfo.getError(), "error is null");
                    //TODO use more specific adbc status code
                    throw new AdbcException(queryError.getMessage(), null, AdbcStatusCode.INTERNAL, queryError.getSqlState(), queryError.getErrorCode());
                }
            }
            executingClient.set(client);
            //client.currentData()
            //WarningsManager warningsManager = new WarningsManager();
            //currentWarningsManager.set(Optional.of(warningsManager));

            //queryResult = TrinoResultSet.create(this, client, maxRows.get(), progressConsumer, warningsManager);

            // check if this is a query
            ArrowReader reader  = new SpooledResultReader(ArrowUtils.root(), client, segmentLoader, progressConsumer);
            queryResult = new QueryResult(0, reader);
            if (client.currentStatusInfo().getUpdateType() == null) {
                //TODO create child allocator with useful name
                //BufferAllocator allocator = ArrowUtils.root().newChildAllocator();

                currentResult.set(queryResult);
            }

            // this is an update, not a query
            while (queryResult.getReader().loadNextBatch()) {
                // ignore rows
            }

            connection().updateSession(client);

            Long updateCount = client.finalStatusInfo().getUpdateCount();
            currentUpdateCount.set((updateCount != null) ? updateCount : 0);
            currentUpdateType.set(client.finalStatusInfo().getUpdateType());
            //warningsManager.addWarnings(client.finalStatusInfo().getWarnings());
            return false;
        }
        catch (RuntimeException e) {
            throw new AdbcException(e.getMessage(), e, AdbcStatusCode.INTERNAL, null, 0);
        } catch (IOException e) {
            throw AdbcException.io("failed to execute query").withCause(e);
        } finally {
            executingClient.set(null);
            if (currentResult.get() == null) {
                if (queryResult != null) {
                    queryResult.close();
                }
                if (client != null) {
                    client.close();
                }
            }
        }
    }

    private void clearCurrentResults()
    {
        currentResult.set(null);
        currentUpdateCount.set(-1);
        currentUpdateType.set(null);
       // currentWarningsManager.set(Optional.empty());
    }

    private Map<String, String> getStatementSessionProperties()
    {
        ImmutableMap.Builder<String, String> sessionProperties = ImmutableMap.builder();
        if (queryTimeoutSeconds.get() > 0) {
            sessionProperties.put("query_max_run_time", queryTimeoutSeconds.get() + "s");
        }
        return sessionProperties.buildOrThrow();
    }

    protected final TrinoConnection connection()
            throws AdbcException
    {
        TrinoConnection connection = this.connection.get();
        if (connection == null) {
            throw AdbcException.invalidState("Statement is closed");
        }
        if (connection.isClosed()) {
            throw AdbcException.invalidState("Connection is closed");
        }
        return connection;
    }

    @Override
    public UpdateResult executeUpdate() throws AdbcException {
        return null;
    }

    @Override
    public void prepare() throws AdbcException {

    }

    @Override
    public void close() throws Exception {

    }
}
