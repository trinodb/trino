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

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface Connector
{
    /**
     * @deprecated use {@link #beginTransaction(IsolationLevel, boolean, boolean)}
     */
    @Deprecated
    default ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Start a new transaction and return a handle for it. The engine will call
     * {@link #getMetadata} to fetch the metadata instance for the transaction.
     * The engine will later call exactly one of {@link #commit} or {@link #rollback}
     * to end the transaction, even in auto-commit mode.
     * <p>
     * If {@code true} is returned from {@link #isSingleStatementWritesOnly}, then
     * the engine will enforce that auto-commit mode is used for writes, allowing
     * connectors to execute writes immediately, rather than needing to wait
     * until the transaction is committed.
     *
     * @param isolationLevel minimum isolation level for the transaction
     * @param readOnly if the transaction is guaranteed to only read data (not write)
     * @param autoCommit if the transaction uses auto-commit mode
     */
    default ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return beginTransaction(isolationLevel, readOnly);
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     */
    default ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return getMetadata(transactionHandle);
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     * @deprecated use {@link #getMetadata(ConnectorSession, ConnectorTransactionHandle)}
     */
    @Deprecated
    default ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support tables with splits
     */
    default ConnectorSplitManager getSplitManager()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables page at a time
     */
    default ConnectorPageSourceProvider getPageSourceProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables record at a time
     */
    default ConnectorRecordSetProvider getRecordSetProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables page at a time
     */
    default ConnectorPageSinkProvider getPageSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support indexes
     */
    default ConnectorIndexProvider getIndexProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support partitioned table layouts
     */
    default ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the set of system tables provided by this connector
     */
    default Set<SystemTable> getSystemTables()
    {
        return emptySet();
    }

    /**
     * @return the set of procedures provided by this connector
     */
    default Set<Procedure> getProcedures()
    {
        return emptySet();
    }

    default Set<TableProcedureMetadata> getTableProcedures()
    {
        return emptySet();
    }

    /**
     * @return the system properties for this connector
     */
    default List<PropertyMetadata<?>> getSessionProperties()
    {
        return emptyList();
    }

    /**
     * @return the schema properties for this connector
     */
    default List<PropertyMetadata<?>> getSchemaProperties()
    {
        return emptyList();
    }

    /**
     * @return the analyze properties for this connector
     */
    default List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return emptyList();
    }

    /**
     * @return the table properties for this connector
     */
    default List<PropertyMetadata<?>> getTableProperties()
    {
        return emptyList();
    }

    /**
     * @return the materialized view properties for this connector
     */
    default List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return emptyList();
    }

    /**
     * @return the column properties for this connector
     */
    default List<PropertyMetadata<?>> getColumnProperties()
    {
        return emptyList();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not have an access control
     */
    default ConnectorAccessControl getAccessControl()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the event listeners provided by this connector
     */
    default Iterable<EventListener> getEventListeners()
    {
        return emptySet();
    }

    /**
     * Commit the transaction. Will be called at most once and will not be called if
     * {@link #rollback} is called.
     */
    default void commit(ConnectorTransactionHandle transactionHandle) {}

    /**
     * Rollback the transaction. Will be called at most once and will not be called if
     * {@link #commit} is called.
     * <p>
     * Note: calls to this method may race with calls to the ConnectorMetadata.
     */
    default void rollback(ConnectorTransactionHandle transactionHandle) {}

    /**
     * True if the connector only supports write statements in independent transactions.
     * The engine will enforce this for the connector by requiring auto-commit mode for writes.
     */
    default boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    default void shutdown() {}

    default Set<ConnectorCapabilities> getCapabilities()
    {
        return emptySet();
    }
}
