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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public interface ConnectorPageSinkProvider
{
    @Deprecated // TODO(Issue #14705): Remove
    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        throw new IllegalArgumentException("createPageSink not supported for outputTableHandle");
    }

    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, session, outputTableHandle);
    }

    @Deprecated // TODO(Issue #14705): Remove
    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        throw new IllegalArgumentException("createPageSink not supported for insertTableHandle");
    }

    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, session, insertTableHandle);
    }

    @Deprecated // TODO(Issue #14705): Remove
    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        throw new IllegalArgumentException("createPageSink not supported for tableExecuteHandle");
    }

    default ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(transactionHandle, session, tableExecuteHandle);
    }

    @Deprecated // TODO(Issue #14705): Remove
    default ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support SQL MERGE operations");
    }

    default ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        return createMergeSink(transactionHandle, session, mergeHandle);
    }
}
