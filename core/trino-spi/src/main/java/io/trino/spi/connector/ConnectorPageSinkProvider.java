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
package io.trino.spi.connector;

import io.trino.spi.TrinoException;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public interface ConnectorPageSinkProvider
{
    /**
     * @deprecated Implement {@link #createPageSink(ConnectorTransactionHandle, ConnectorSession, ConnectorOutputTableHandle, Optional, ConnectorPageSinkId)} instead.
     */
    @Deprecated(forRemoval = true)
    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating new tables");
    }

    /**
     * Creates a {@link ConnectorPageSink} for writing data to a newly created table.
     *
     * @param transactionHandle the transaction handle for this operation
     * @param session the session in which the write is being performed
     * @param outputTableHandle the handle identifying the new table being created
     * @param tableCredentials credentials for accessing the table data
     * @param pageSinkId unique identifier for this page sink instance
     * @return a page sink for writing data to the new table
     */
    default ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, session, outputTableHandle, pageSinkId);
    }

    /**
     * @deprecated Implement {@link #createPageSink(ConnectorTransactionHandle, ConnectorSession, ConnectorInsertTableHandle, Optional, ConnectorPageSinkId)}
     */
    @Deprecated(forRemoval = true)
    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support insert operations");
    }

    /**
     * Creates a {@link ConnectorPageSink} for inserting data into an existing table.
     *
     * @param transactionHandle the transaction handle for this operation
     * @param session the session in which the insert is being performed
     * @param insertTableHandle the handle identifying the table for insertion
     * @param tableCredentials credentials for accessing the table data
     * @param pageSinkId unique identifier for this page sink instance
     * @return a page sink for inserting data into the table
     */
    default ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, session, insertTableHandle, pageSinkId);
    }

    /**
     * Creates a {@link ConnectorPageSink} for executing a table procedure that writes data.
     *
     * @param transactionHandle the transaction handle for this operation
     * @param session the session in which the table execute is being performed
     * @param tableExecuteHandle the handle identifying the table execute operation
     * @param tableCredentials credentials for accessing the table data
     * @param pageSinkId unique identifier for this page sink instance
     * @return a page sink for writing data during table execute
     */
    default ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, session, tableExecuteHandle, pageSinkId);
    }

    /**
     * @deprecated Implement {@link #createPageSink(ConnectorTransactionHandle, ConnectorSession, ConnectorTableExecuteHandle, Optional, ConnectorPageSinkId)} instead.
     */
    @Deprecated(forRemoval = true)
    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        throw new IllegalArgumentException("createPageSink not supported for tableExecuteHandle");
    }

    /**
     * Creates a {@link ConnectorMergeSink} for executing SQL MERGE operations.
     *
     * @param transactionHandle the transaction handle for this operation
     * @param session the session in which the merge is being performed
     * @param mergeHandle the handle identifying the merge operation
     * @param tableCredentials credentials for accessing the table data
     * @param pageSinkId unique identifier for this page sink instance
     * @return a merge sink for executing the MERGE operation
     */
    default ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return createMergeSink(transactionHandle, session, mergeHandle, pageSinkId);
    }

    /**
     * @deprecated Implement {@link #createMergeSink(ConnectorTransactionHandle, ConnectorSession, ConnectorMergeTableHandle, Optional, ConnectorPageSinkId)} instead.
     */
    @Deprecated(forRemoval = true)
    default ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support SQL MERGE operations");
    }
}
