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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.orc.OrcFileWriter;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.PartitionAndStatementId.CODEC;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HiveUpdatablePageSource
        extends AbstractHiveAcidWriters
        implements UpdatablePageSource
{
    // The channel numbers of the child blocks in the RowBlock passed to deleteRows()
    public static final int ORIGINAL_TRANSACTION_CHANNEL = 0;
    public static final int BUCKET_CHANNEL = 1;
    public static final int ROW_ID_CHANNEL = 2;
    // Used for UPDATE operations
    public static final int ROW_CHANNEL = 3;
    public static final int ACID_ROW_STRUCT_COLUMN_ID = 6;

    private final String partitionName;
    private final ConnectorPageSource hivePageSource;
    private final AcidOperation updateKind;
    private final Block hiveRowTypeNullsBlock;
    private final long writeId;
    private final Optional<List<Integer>> dependencyChannels;

    private long maxWriteId;
    private long rowCount;
    private long insertRowCounter;
    private long initialRowId;
    private long maxNumberOfRowsPerSplit;

    private boolean closed;

    public HiveUpdatablePageSource(
            HiveTableHandle hiveTableHandle,
            String partitionName,
            int statementId,
            ConnectorPageSource hivePageSource,
            TypeManager typeManager,
            OptionalInt bucketNumber,
            Path bucketPath,
            boolean originalFile,
            OrcFileWriterFactory orcFileWriterFactory,
            Configuration configuration,
            ConnectorSession session,
            HiveType hiveRowType,
            List<HiveColumnHandle> dependencyColumns,
            AcidOperation updateKind,
            long initialRowId,
            long maxNumberOfRowsPerSplit)
    {
        super(hiveTableHandle.getTransaction(), statementId, bucketNumber, bucketPath, originalFile, orcFileWriterFactory, configuration, session, hiveRowType, updateKind);
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.hivePageSource = requireNonNull(hivePageSource, "hivePageSource is null");
        this.updateKind = requireNonNull(updateKind, "updateKind is null");
        this.hiveRowTypeNullsBlock = nativeValueToBlock(hiveRowType.getType(typeManager), null);
        checkArgument(hiveTableHandle.isInAcidTransaction(), "Not in a transaction; hiveTableHandle: %s", hiveTableHandle);
        this.writeId = hiveTableHandle.getWriteId();
        this.initialRowId = initialRowId;
        this.maxNumberOfRowsPerSplit = maxNumberOfRowsPerSplit;
        if (updateKind == AcidOperation.UPDATE) {
            this.dependencyChannels = Optional.of(hiveTableHandle.getUpdateProcessor()
                    .orElseThrow(() -> new IllegalArgumentException("updateProcessor not present"))
                    .makeDependencyChannelNumbers(dependencyColumns));
        }
        else {
            this.dependencyChannels = Optional.empty();
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        ColumnarRow acidBlock = toColumnarRow(rowIds);
        int fieldCount = acidBlock.getFieldCount();
        checkArgument(fieldCount == 3, "The rowId block for DELETE should have 3 children, but has %s", fieldCount);
        deleteRowsInternal(acidBlock);
    }

    private void deleteRowsInternal(ColumnarRow columnarRow)
    {
        int positionCount = columnarRow.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            checkArgument(!columnarRow.isNull(position), "In the delete rowIds, found null row at position %s", position);
        }

        Block originalTransactionChannel = columnarRow.getField(ORIGINAL_TRANSACTION_CHANNEL);
        Block[] blockArray = {
                new RunLengthEncodedBlock(DELETE_OPERATION_BLOCK, positionCount),
                originalTransactionChannel,
                columnarRow.getField(BUCKET_CHANNEL),
                columnarRow.getField(ROW_ID_CHANNEL),
                RunLengthEncodedBlock.create(BIGINT, writeId, positionCount),
                new RunLengthEncodedBlock(hiveRowTypeNullsBlock, positionCount),
        };
        Page deletePage = new Page(blockArray);

        for (int index = 0; index < positionCount; index++) {
            maxWriteId = Math.max(maxWriteId, originalTransactionChannel.getLong(index, 0));
        }

        lazyInitializeDeleteFileWriter();
        deleteFileWriter.orElseThrow(() -> new IllegalArgumentException("deleteFileWriter not present")).appendRows(deletePage);
        rowCount += positionCount;
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        int positionCount = page.getPositionCount();
        verify(positionCount > 0, "Unexpected empty page"); // should be filtered out by engine

        HiveUpdateProcessor updateProcessor = transaction.getUpdateProcessor().orElseThrow(() -> new IllegalArgumentException("updateProcessor not present"));
        ColumnarRow acidBlock = updateProcessor.getAcidBlock(page, columnValueAndRowIdChannels);

        int fieldCount = acidBlock.getFieldCount();
        checkArgument(fieldCount == 3 || fieldCount == 4, "The rowId block for UPDATE should have 3 or 4 children, but has %s", fieldCount);
        deleteRowsInternal(acidBlock);

        Block mergedColumnsBlock = updateProcessor.createMergedColumnsBlock(page, columnValueAndRowIdChannels);

        Block currentTransactionBlock = RunLengthEncodedBlock.create(BIGINT, writeId, positionCount);
        Block[] blockArray = {
                new RunLengthEncodedBlock(INSERT_OPERATION_BLOCK, positionCount),
                currentTransactionBlock,
                acidBlock.getField(BUCKET_CHANNEL),
                createRowIdBlock(positionCount),
                currentTransactionBlock,
                mergedColumnsBlock,
        };

        Page insertPage = new Page(blockArray);
        lazyInitializeInsertFileWriter();
        insertFileWriter.orElseThrow(() -> new IllegalArgumentException("insertFileWriter not present")).appendRows(insertPage);
    }

    Block createRowIdBlock(int positionCount)
    {
        long[] rowIds = new long[positionCount];
        for (int index = 0; index < positionCount; index++) {
            rowIds[index] = initialRowId + insertRowCounter++;
        }
        if (insertRowCounter >= maxNumberOfRowsPerSplit) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, format("Trying to insert too many rows in a single split, max allowed is %d per split", maxNumberOfRowsPerSplit));
        }
        return new LongArrayBlock(positionCount, Optional.empty(), rowIds);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (deleteFileWriter.isEmpty()) {
            return completedFuture(ImmutableList.of());
        }
        OrcFileWriter deleteWriter = (OrcFileWriter) deleteFileWriter.get();
        deleteWriter.setMaxWriteId(maxWriteId);
        deleteWriter.commit();

        Optional<String> deltaDirectoryString;
        switch (updateKind) {
            case DELETE:
                deltaDirectoryString = Optional.empty();
                break;

            case UPDATE:
                OrcFileWriter insertWriter = (OrcFileWriter) insertFileWriter.get();
                insertWriter.setMaxWriteId(maxWriteId);
                insertWriter.commit();
                checkArgument(deltaDirectory.isPresent(), "deltaDirectory not present");
                deltaDirectoryString = Optional.of(deltaDirectory.get().toString());
                break;

            default:
                throw new IllegalArgumentException("Unknown UpdateKind " + updateKind);
        }
        Slice fragment = Slices.wrappedBuffer(CODEC.toJsonBytes(new PartitionAndStatementId(
                partitionName,
                statementId,
                rowCount,
                deleteDeltaDirectory.toString(),
                deltaDirectoryString)));
        return completedFuture(ImmutableList.of(fragment));
    }

    @Override
    public long getCompletedBytes()
    {
        return hivePageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return hivePageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        Page page = hivePageSource.getNextPage();
        if (page == null) {
            close();
            return null;
        }
        if (transaction.isUpdate()) {
            HiveUpdateProcessor updateProcessor = transaction.getUpdateProcessor().orElseThrow(() -> new IllegalArgumentException("updateProcessor not present"));
            List<Integer> channels = dependencyChannels.orElseThrow(() -> new IllegalArgumentException("dependencyChannels not present"));
            return updateProcessor.removeNonDependencyColumns(page, channels);
        }
        else {
            return page;
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return hivePageSource.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            hivePageSource.close();
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, e);
        }
    }
}
