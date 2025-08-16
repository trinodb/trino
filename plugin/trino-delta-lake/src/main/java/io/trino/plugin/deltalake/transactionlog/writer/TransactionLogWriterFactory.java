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
package io.trino.plugin.deltalake.transactionlog.writer;

import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.connector.ConnectorSession;

public interface TransactionLogWriterFactory
{
    TransactionLogWriter createWriter(ConnectorSession session, DeltaLakeTableHandle tableHandle);

    TransactionLogWriter createWriter(ConnectorSession session, String tableLocation, MetadataEntry metadataEntry, ProtocolEntry protocolEntry);

    TransactionLogWriter createFileSystemWriter(ConnectorSession session, String tableLocation);

    TransactionLogWriter newWriterWithoutTransactionIsolation(ConnectorSession session, String tableLocation);
}
