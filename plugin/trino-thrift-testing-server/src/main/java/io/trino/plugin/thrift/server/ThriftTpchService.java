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
package io.trino.plugin.thrift.server;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.json.JsonCodec;
import io.trino.plugin.thrift.api.TrinoThriftBlock;
import io.trino.plugin.thrift.api.TrinoThriftColumnMetadata;
import io.trino.plugin.thrift.api.TrinoThriftId;
import io.trino.plugin.thrift.api.TrinoThriftNullableColumnSet;
import io.trino.plugin.thrift.api.TrinoThriftNullableSchemaName;
import io.trino.plugin.thrift.api.TrinoThriftNullableTableMetadata;
import io.trino.plugin.thrift.api.TrinoThriftNullableToken;
import io.trino.plugin.thrift.api.TrinoThriftPageResult;
import io.trino.plugin.thrift.api.TrinoThriftSchemaTableName;
import io.trino.plugin.thrift.api.TrinoThriftService;
import io.trino.plugin.thrift.api.TrinoThriftServiceException;
import io.trino.plugin.thrift.api.TrinoThriftSplit;
import io.trino.plugin.thrift.api.TrinoThriftSplitBatch;
import io.trino.plugin.thrift.api.TrinoThriftTableMetadata;
import io.trino.plugin.thrift.api.TrinoThriftTupleDomain;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.thrift.api.TrinoThriftBlock.fromBlock;
import static io.trino.plugin.thrift.server.SplitInfo.normalSplit;
import static io.trino.plugin.tpch.TpchMetadata.getTrinoType;
import static io.trino.plugin.tpch.TpchRecordSet.createTpchRecordSet;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.min;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;

public class ThriftTpchService
        implements TrinoThriftService, Closeable
{
    private static final int DEFAULT_NUMBER_OF_SPLITS = 3;
    private static final List<String> SCHEMAS = ImmutableList.of("tiny", "sf1");
    protected static final JsonCodec<SplitInfo> SPLIT_INFO_CODEC = jsonCodec(SplitInfo.class);

    private final ListeningExecutorService executor = listeningDecorator(
            newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadsNamed("thrift-tpch-%s")));

    @Override
    public final List<String> listSchemaNames()
    {
        return SCHEMAS;
    }

    @Override
    public final List<TrinoThriftSchemaTableName> listTables(TrinoThriftNullableSchemaName schemaNameOrNull)
    {
        List<TrinoThriftSchemaTableName> tables = new ArrayList<>();
        for (String schemaName : getSchemaNames(schemaNameOrNull.getSchemaName())) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                tables.add(new TrinoThriftSchemaTableName(schemaName, tpchTable.getTableName()));
            }
        }
        return tables;
    }

    @Override
    public final TrinoThriftNullableTableMetadata getTableMetadata(TrinoThriftSchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        if (!SCHEMAS.contains(schemaName) || TpchTable.getTables().stream().noneMatch(table -> table.getTableName().equals(tableName))) {
            return new TrinoThriftNullableTableMetadata(null);
        }
        TpchTable<?> tpchTable = TpchTable.getTable(schemaTableName.getTableName());
        List<TrinoThriftColumnMetadata> columns = new ArrayList<>();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new TrinoThriftColumnMetadata(column.getSimplifiedColumnName(), getTypeString(column), null, false));
        }
        List<Set<String>> indexableKeys = getIndexableKeys(schemaName, tableName);
        return new TrinoThriftNullableTableMetadata(new TrinoThriftTableMetadata(schemaTableName, columns, null, !indexableKeys.isEmpty() ? indexableKeys : null));
    }

    protected List<Set<String>> getIndexableKeys(String schemaName, String tableName)
    {
        return ImmutableList.of();
    }

    @Override
    public final ListenableFuture<TrinoThriftSplitBatch> getSplits(
            TrinoThriftSchemaTableName schemaTableName,
            TrinoThriftNullableColumnSet desiredColumns,
            TrinoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            TrinoThriftNullableToken nextToken)
    {
        return executor.submit(() -> getSplitsSync(schemaTableName, maxSplitCount, nextToken));
    }

    private static TrinoThriftSplitBatch getSplitsSync(
            TrinoThriftSchemaTableName schemaTableName,
            int maxSplitCount,
            TrinoThriftNullableToken nextToken)
    {
        int totalParts = DEFAULT_NUMBER_OF_SPLITS;
        // last sent part
        int partNumber = nextToken.getToken() == null ? 0 : Ints.fromByteArray(nextToken.getToken().getId());
        int numberOfSplits = min(maxSplitCount, totalParts - partNumber);

        List<TrinoThriftSplit> splits = new ArrayList<>(numberOfSplits);
        for (int i = 0; i < numberOfSplits; i++) {
            SplitInfo splitInfo = normalSplit(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    partNumber + 1,
                    totalParts);
            splits.add(new TrinoThriftSplit(new TrinoThriftId(SPLIT_INFO_CODEC.toJsonBytes(splitInfo)), ImmutableList.of()));
            partNumber++;
        }
        TrinoThriftId newNextToken = partNumber < totalParts ? new TrinoThriftId(Ints.toByteArray(partNumber)) : null;
        return new TrinoThriftSplitBatch(splits, newNextToken);
    }

    @Override
    public final ListenableFuture<TrinoThriftSplitBatch> getIndexSplits(
            TrinoThriftSchemaTableName schemaTableName,
            List<String> indexColumnNames,
            List<String> outputColumnNames,
            TrinoThriftPageResult keys,
            TrinoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            TrinoThriftNullableToken nextToken)
    {
        return executor.submit(() -> getIndexSplitsSync(schemaTableName, indexColumnNames, keys, maxSplitCount, nextToken));
    }

    protected TrinoThriftSplitBatch getIndexSplitsSync(
            TrinoThriftSchemaTableName schemaTableName,
            List<String> indexColumnNames,
            TrinoThriftPageResult keys,
            int maxSplitCount,
            TrinoThriftNullableToken nextToken)
            throws TrinoThriftServiceException
    {
        throw new TrinoThriftServiceException("Index join is not supported", false);
    }

    @Override
    public final ListenableFuture<TrinoThriftPageResult> getRows(
            TrinoThriftId splitId,
            List<String> outputColumns,
            long maxBytes,
            TrinoThriftNullableToken nextToken)
    {
        return executor.submit(() -> getRowsSync(splitId, outputColumns, maxBytes, nextToken));
    }

    private TrinoThriftPageResult getRowsSync(
            TrinoThriftId splitId,
            List<String> outputColumns,
            long maxBytes,
            TrinoThriftNullableToken nextToken)
    {
        SplitInfo splitInfo = SPLIT_INFO_CODEC.fromJson(splitId.getId());
        checkArgument(maxBytes >= DEFAULT_MAX_PAGE_SIZE_IN_BYTES, "requested maxBytes is too small");
        ConnectorPageSource pageSource;
        if (!splitInfo.isIndexSplit()) {
            // normal scan
            pageSource = createPageSource(splitInfo, outputColumns);
        }
        else {
            // index lookup
            pageSource = createLookupPageSource(splitInfo, outputColumns);
        }
        return getRowsInternal(pageSource, splitInfo.getTableName(), outputColumns, nextToken.getToken());
    }

    protected ConnectorPageSource createLookupPageSource(SplitInfo splitInfo, List<String> outputColumnNames)
    {
        throw new UnsupportedOperationException("lookup is not supported");
    }

    @PreDestroy
    @Override
    public final void close()
    {
        executor.shutdownNow();
    }

    public static List<Type> types(String tableName, List<String> columnNames)
    {
        TpchTable<?> table = TpchTable.getTable(tableName);
        return columnNames.stream().map(name -> getTrinoType(table.getColumn(name), DecimalTypeMapping.DOUBLE)).collect(toList());
    }

    public static double schemaNameToScaleFactor(String schemaName)
    {
        switch (schemaName) {
            case "tiny":
                return 0.01;
            case "sf1":
                return 1.0;
        }
        throw new IllegalArgumentException("Schema is not setup: " + schemaName);
    }

    private static TrinoThriftPageResult getRowsInternal(ConnectorPageSource pageSource, String tableName, List<String> columnNames, @Nullable TrinoThriftId nextToken)
    {
        // very inefficient implementation as it needs to re-generate all previous results to get the next page
        int skipPages = nextToken != null ? Ints.fromByteArray(nextToken.getId()) : 0;
        skipPages(pageSource, skipPages);

        Page page = null;
        while (!pageSource.isFinished() && page == null) {
            page = pageSource.getNextPage();
            skipPages++;
        }
        TrinoThriftId newNextToken = pageSource.isFinished() ? null : new TrinoThriftId(Ints.toByteArray(skipPages));

        return toThriftPage(page, types(tableName, columnNames), newNextToken);
    }

    private static TrinoThriftPageResult toThriftPage(Page page, List<Type> columnTypes, @Nullable TrinoThriftId nextToken)
    {
        if (page == null) {
            checkState(nextToken == null, "there must be no more data when page is null");
            return new TrinoThriftPageResult(ImmutableList.of(), 0, null);
        }
        checkState(page.getChannelCount() == columnTypes.size(), "number of columns in a page doesn't match the one in requested types");
        int numberOfColumns = columnTypes.size();
        List<TrinoThriftBlock> columnBlocks = new ArrayList<>(numberOfColumns);
        for (int i = 0; i < numberOfColumns; i++) {
            columnBlocks.add(fromBlock(page.getBlock(i), columnTypes.get(i)));
        }
        return new TrinoThriftPageResult(columnBlocks, page.getPositionCount(), nextToken);
    }

    private static void skipPages(ConnectorPageSource pageSource, int skipPages)
    {
        for (int i = 0; i < skipPages; i++) {
            checkState(!pageSource.isFinished(), "pageSource is unexpectedly finished");
            pageSource.getNextPage();
        }
    }

    private static ConnectorPageSource createPageSource(SplitInfo splitInfo, List<String> columnNames)
    {
        switch (splitInfo.getTableName()) {
            case "orders":
                return createPageSource(TpchTable.ORDERS, columnNames, splitInfo);
            case "customer":
                return createPageSource(TpchTable.CUSTOMER, columnNames, splitInfo);
            case "lineitem":
                return createPageSource(TpchTable.LINE_ITEM, columnNames, splitInfo);
            case "nation":
                return createPageSource(TpchTable.NATION, columnNames, splitInfo);
            case "region":
                return createPageSource(TpchTable.REGION, columnNames, splitInfo);
            case "part":
                return createPageSource(TpchTable.PART, columnNames, splitInfo);
            default:
                throw new IllegalArgumentException("Table not setup: " + splitInfo.getTableName());
        }
    }

    private static <T extends TpchEntity> ConnectorPageSource createPageSource(TpchTable<T> table, List<String> columnNames, SplitInfo splitInfo)
    {
        List<TpchColumn<T>> columns = columnNames.stream().map(table::getColumn).collect(toList());
        return new RecordPageSource(createTpchRecordSet(
                table,
                columns,
                DecimalTypeMapping.DOUBLE,
                schemaNameToScaleFactor(splitInfo.getSchemaName()),
                splitInfo.getPartNumber(),
                splitInfo.getTotalParts(),
                TupleDomain.all()));
    }

    private static List<String> getSchemaNames(String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return SCHEMAS;
        }
        if (SCHEMAS.contains(schemaNameOrNull)) {
            return ImmutableList.of(schemaNameOrNull);
        }
        return ImmutableList.of();
    }

    private static String getTypeString(TpchColumn<?> column)
    {
        return getTrinoType(column, DecimalTypeMapping.DOUBLE).getTypeSignature().toString();
    }
}
