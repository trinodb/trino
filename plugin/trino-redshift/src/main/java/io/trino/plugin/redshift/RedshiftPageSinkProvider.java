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
package io.trino.plugin.redshift;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.TypeManager;

import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_INSERT;
import static io.trino.plugin.redshift.RedshiftSessionProperties.isBatchedInsertsCopyEnabled;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class RedshiftPageSinkProvider
        extends JdbcPageSinkProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final RedshiftConfig redshiftConfig;
    private final String trinoVersion;

    @Inject
    public RedshiftPageSinkProvider(
            JdbcClient jdbcClient,
            RemoteQueryModifier remoteQueryModifier,
            QueryBuilder queryBuilder,
            @FileSystemS3 TrinoFileSystemFactory fileSystemFactory,
            RedshiftConfig redshiftConfig,
            TypeManager typeManager,
            NodeManager nodeManager)
    {
        super(jdbcClient, remoteQueryModifier, queryBuilder);

        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.redshiftConfig = requireNonNull(redshiftConfig, "redshiftConfig is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoVersion = requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getVersion();
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        if (isBatchedInsertsCopyEnabled(session)) {
            if (session.getProperty(NON_TRANSACTIONAL_INSERT, Boolean.class)) {
                throw new TrinoException(NOT_SUPPORTED, "Batched inserts with COPY are not supported in non-transactional mode");
            }

            JdbcOutputTableHandle handle = (JdbcOutputTableHandle) outputTableHandle;
            return new RedshiftBatchedInsertsCopyPageSink(
                    session,
                    pageSinkId,
                    fileSystemFactory,
                    typeManager.getTypeOperators(),
                    RedshiftClient.sinkPrefix(session, redshiftConfig.getBatchedInsertsCopyLocation()),
                    handle.getColumnNames(),
                    handle.getColumnTypes(),
                    trinoVersion);
        }
        else {
            // Otherwise, default jdbc behavior
            return super.createPageSink(transactionHandle, session, outputTableHandle, pageSinkId);
        }
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        if (isBatchedInsertsCopyEnabled(session)) {
            return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle, pageSinkId);
        }
        else {
            // Otherwise, default jdbc behavior
            return super.createPageSink(transactionHandle, session, insertTableHandle, pageSinkId);
        }
    }
}
