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
package io.trino.plugin.iceberg;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;

/**
 * Exactly one of {@link #cursor} or {@link #pageSource} must be implemented.
 */
public interface IcebergSystemTable
{
    ConnectorTableMetadata getTableMetadata();

    Map<String, ColumnHandle> getColumnHandles();

    /**
     * Create a cursor for the data in this table.
     *
     * @param session the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     */
    default RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a page source for the data in this table.
     *
     * @param session the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     */
    default ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        throw new UnsupportedOperationException();
    }
}
