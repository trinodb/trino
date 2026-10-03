package tech.ydb.trino;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Optional;

import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.getWriteBatchSize;

public class YdbPageSinkProvider extends JdbcPageSinkProvider {
    private final JdbcClient client;
    private final RemoteQueryModifier modifier;

    @Inject
    public YdbPageSinkProvider(JdbcClient client, RemoteQueryModifier modifier, QueryBuilder queryBuilder) {
        super(client, modifier, queryBuilder);
        this.client = client;
        this.modifier = modifier;
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorMergeTableHandle handle,
            Optional<ConnectorTableCredentials> credentials,
            ConnectorPageSinkId pageSinkId) {
        return new YdbMergeSink(session, (JdbcMergeTableHandle) handle, client, pageSinkId, modifier, getWriteBatchSize(session));
    }
}
