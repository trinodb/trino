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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.AbstractTestHive.HiveTransaction;
import io.trino.plugin.hive.AbstractTestHive.Transaction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.AbstractTestHive.getAllSplits;
import static io.trino.plugin.hive.AbstractTestHive.getSplits;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.getTypes;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;

public class HiveFileSystemTestUtils
{
    private HiveFileSystemTestUtils() {}

    public static MaterializedResult readTable(SchemaTableName tableName, HiveTransactionManager transactionManager,
                                               HiveConfig config, ConnectorPageSourceProvider pageSourceProvider,
                                               ConnectorSplitManager splitManager)
            throws IOException
    {
        ConnectorMetadata metadata = null;
        ConnectorSession session = null;
        ConnectorSplitSource splitSource = null;

        try (Transaction transaction = newTransaction(transactionManager)) {
            metadata = transaction.getMetadata();
            session = newSession(config);

            ConnectorTableHandle table = getTableHandle(metadata, tableName, session);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, table).values());

            metadata.beginQuery(session);
            splitSource = getSplits(splitManager, transaction, session, table);

            List<Type> allTypes = getTypes(columnHandles);
            List<Type> dataTypes = getTypes(columnHandles.stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toImmutableList()));
            MaterializedResult.Builder result = MaterializedResult.resultBuilder(session, dataTypes);

            List<ConnectorSplit> splits = getAllSplits(splitSource);
            for (ConnectorSplit split : splits) {
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                        transaction.getTransactionHandle(),
                        session,
                        split,
                        table,
                        columnHandles,
                        DynamicFilter.EMPTY)) {
                    MaterializedResult pageSourceResult = materializeSourceDataStream(session, pageSource, allTypes);
                    for (MaterializedRow row : pageSourceResult.getMaterializedRows()) {
                        Object[] dataValues = IntStream.range(0, row.getFieldCount())
                                .filter(channel -> !((HiveColumnHandle) columnHandles.get(channel)).isHidden())
                                .mapToObj(row::getField)
                                .toArray();
                        result.row(dataValues);
                    }
                }
            }
            return result.build();
        }
        finally {
            cleanUpQuery(metadata, session);
            closeQuietly(splitSource);
        }
    }

    public static ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName, ConnectorSession session)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(session, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    public static ConnectorSession newSession(HiveConfig config)
    {
        return getHiveSession(config);
    }

    public static Transaction newTransaction(HiveTransactionManager transactionManager)
    {
        return new HiveTransaction(transactionManager);
    }

    public static MaterializedResult filterTable(SchemaTableName tableName,
                                                 List<ColumnHandle> projectedColumns,
                                                 HiveTransactionManager transactionManager,
                                                 HiveConfig config,
                                                 ConnectorPageSourceProvider pageSourceProvider,
                                                 ConnectorSplitManager splitManager)
            throws IOException
    {
        ConnectorMetadata metadata = null;
        ConnectorSession session = null;
        ConnectorSplitSource splitSource = null;

        try (Transaction transaction = newTransaction(transactionManager)) {
            metadata = transaction.getMetadata();
            session = newSession(config);

            ConnectorTableHandle table = getTableHandle(metadata, tableName, session);

            metadata.beginQuery(session);
            splitSource = getSplits(splitManager, transaction, session, table);

            List<Type> allTypes = getTypes(projectedColumns);
            List<Type> dataTypes = getTypes(projectedColumns.stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toImmutableList()));
            MaterializedResult.Builder result = MaterializedResult.resultBuilder(session, dataTypes);

            List<ConnectorSplit> splits = getAllSplits(splitSource);
            for (ConnectorSplit split : splits) {
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(),
                        session, split, table, projectedColumns, DynamicFilter.EMPTY)) {
                    MaterializedResult pageSourceResult = materializeSourceDataStream(session, pageSource, allTypes);
                    for (MaterializedRow row : pageSourceResult.getMaterializedRows()) {
                        Object[] dataValues = IntStream.range(0, row.getFieldCount())
                                .filter(channel -> !((HiveColumnHandle) projectedColumns.get(channel)).isHidden())
                                .mapToObj(row::getField)
                                .toArray();
                        result.row(dataValues);
                    }
                }
            }
            return result.build();
        }
        finally {
            cleanUpQuery(metadata, session);
            closeQuietly(splitSource);
        }
    }

    public static int getSplitsCount(SchemaTableName tableName,
                                     HiveTransactionManager transactionManager,
                                     HiveConfig config,
                                     ConnectorSplitManager splitManager)
    {
        ConnectorMetadata metadata = null;
        ConnectorSession session = null;
        ConnectorSplitSource splitSource = null;

        try (Transaction transaction = newTransaction(transactionManager)) {
            metadata = transaction.getMetadata();
            session = newSession(config);

            ConnectorTableHandle table = getTableHandle(metadata, tableName, session);

            metadata.beginQuery(session);
            splitSource = getSplits(splitManager, transaction, session, table);
            return getAllSplits(splitSource).size();
        }
        finally {
            cleanUpQuery(metadata, session);
            closeQuietly(splitSource);
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            if (closeable != null) {
                closeable.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static void cleanUpQuery(ConnectorMetadata metadata, ConnectorSession session)
    {
        if (metadata != null && session != null) {
            metadata.cleanupQuery(session);
        }
    }
}
