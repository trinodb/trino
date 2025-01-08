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
package io.trino.plugin.paimon;

import com.google.inject.Inject;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalog;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalogFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static java.util.Objects.requireNonNull;

/**
 * Trino {@link ConnectorPageSinkProvider}.
 */
public class PaimonPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PaimonTrinoCatalogFactory paimonTrinoCatalogFactory;

    @Inject
    public PaimonPageSinkProvider(PaimonTrinoCatalogFactory paimonTrinoCatalogFactory)
    {
        this.paimonTrinoCatalogFactory =
                requireNonNull(paimonTrinoCatalogFactory, "paimonTrinoCatalogFactory is null");
    }

    private static void validataBucketMode(Table table)
    {
        BucketMode mode =
                table instanceof FileStoreTable
                        ? ((FileStoreTable) table).bucketMode()
                        : BucketMode.HASH_FIXED;
        switch (mode) {
            case HASH_FIXED:
            case BUCKET_UNAWARE:
                break;
            default:
                throw new IllegalArgumentException("Unknown bucket mode: " + mode);
        }
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink((PaimonTableHandle) outputTableHandle, session);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink((PaimonTableHandle) insertTableHandle, session);
    }

    private ConnectorPageSink createPageSink(
            PaimonTableHandle tableHandle,
            ConnectorSession session)
    {
        PaimonTrinoCatalog paimonTrinoCatalog = paimonTrinoCatalogFactory.create(session.getIdentity());
        Table table = tableHandle.tableWithDynamicOptions(paimonTrinoCatalog, session);
        validataBucketMode(table);

        return runWithContextClassLoader(
                () -> {
                    BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
                    if (PaimonSessionProperties.enableInsertOverwrite(session)) {
                        batchWriteBuilder.withOverwrite();
                    }
                    BatchTableWrite write = batchWriteBuilder.newWrite();
                    return new PaimonPageSink(write);
                },
                PaimonPageSinkProvider.class.getClassLoader());
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            ConnectorPageSinkId pageSinkId)
    {
        PaimonTrinoCatalog paimonTrinoCatalog = paimonTrinoCatalogFactory.create(session.getIdentity());
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) mergeHandle.getTableHandle();
        Table table = paimonTableHandle.tableWithDynamicOptions(paimonTrinoCatalog, session);
        return new PaimonMergeSink(
                createPageSink(paimonTableHandle, session),
                table.rowType().getFields().size());
    }
}
