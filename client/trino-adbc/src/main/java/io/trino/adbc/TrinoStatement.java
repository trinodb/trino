package io.trino.adbc;

import io.trino.client.QueryStats;
import io.trino.client.StatementClient;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;

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
        StatementClient client = newStatementClient();
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
