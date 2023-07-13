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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.HiveWriterFactory.RowIdSortingFileWriterMaker;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MergePage;
import io.trino.spi.type.TypeManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.orc.OrcWriter.OrcOperation.DELETE;
import static io.trino.orc.OrcWriter.OrcOperation.INSERT;
import static io.trino.plugin.hive.HivePageSource.BUCKET_CHANNEL;
import static io.trino.plugin.hive.HivePageSource.ORIGINAL_TRANSACTION_CHANNEL;
import static io.trino.plugin.hive.HivePageSource.ROW_ID_CHANNEL;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_NAMES;
import static io.trino.plugin.hive.acid.AcidSchema.createAcidSchema;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.orc.OrcFileWriter.computeBucketValue;
import static io.trino.plugin.hive.util.AcidTables.deleteDeltaSubdir;
import static io.trino.plugin.hive.util.AcidTables.deltaSubdir;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class MergeFileWriter
        implements FileWriter
{
    // The bucketPath looks like this: /root/dir/delta_nnnnnnn_mmmmmmm_ssss/bucket_bbbbb(_aaaa)?
    private static final Pattern BUCKET_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>delta_\\d+_\\d+)_(?<statementId>\\d+)/(?<filenameBase>bucket_(?<bucketNumber>\\d+))(?<attemptId>_\\d+)?$");

    // After compaction, the bucketPath looks like this: /root/dir/base_nnnnnnn(_vmmmmmmm)?/bucket_bbbbb(_aaaa)?
    private static final Pattern BASE_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>base_-?\\d+(_v\\d+)?)/(?<filenameBase>bucket_(?<bucketNumber>\\d+))(?<attemptId>_\\d+)?$");

    private static final Block DELETE_OPERATION_BLOCK = nativeValueToBlock(INTEGER, (long) DELETE.getOperationNumber());
    private static final Block INSERT_OPERATION_BLOCK = nativeValueToBlock(INTEGER, (long) INSERT.getOperationNumber());

    private final AcidTransaction transaction;
    private final OptionalInt bucketNumber;
    private final Block bucketValueBlock;
    private final ConnectorSession session;
    private final Block hiveRowTypeNullsBlock;
    private final Location deltaDirectory;
    private final Location deleteDeltaDirectory;
    private final List<HiveColumnHandle> inputColumns;
    private final RowIdSortingFileWriterMaker sortingFileWriterMaker;
    private final OrcFileWriterFactory orcFileWriterFactory;
    private final HiveCompressionCodec compressionCodec;
    private final Properties hiveAcidSchema;
    private final String bucketFilename;
    private Optional<FileWriter> deleteFileWriter = Optional.empty();
    private Optional<FileWriter> insertFileWriter = Optional.empty();

    private int deleteRowCount;
    private int insertRowCount;

    public MergeFileWriter(
            AcidTransaction transaction,
            int statementId,
            OptionalInt bucketNumber,
            RowIdSortingFileWriterMaker sortingFileWriterMaker,
            String bucketPath,
            OrcFileWriterFactory orcFileWriterFactory,
            HiveCompressionCodec compressionCodec,
            List<HiveColumnHandle> inputColumns,
            ConnectorSession session,
            TypeManager typeManager,
            HiveType hiveRowType)
    {
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.sortingFileWriterMaker = requireNonNull(sortingFileWriterMaker, "sortingFileWriterMaker is null");
        this.bucketValueBlock = nativeValueToBlock(INTEGER, (long) computeBucketValue(bucketNumber.orElse(0), statementId));
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.session = requireNonNull(session, "session is null");
        checkArgument(transaction.isTransactional(), "Not in a transaction: %s", transaction);
        this.hiveAcidSchema = createAcidSchema(hiveRowType);
        this.hiveRowTypeNullsBlock = nativeValueToBlock(hiveRowType.getType(typeManager), null);
        Matcher matcher = BASE_PATH_MATCHER.matcher(bucketPath);
        if (!matcher.matches()) {
            matcher = BUCKET_PATH_MATCHER.matcher(bucketPath);
            checkArgument(matcher.matches(), "bucketPath doesn't have the required format: %s", bucketPath);
        }
        this.bucketFilename = matcher.group("filenameBase");
        long writeId = transaction.getWriteId();
        this.deltaDirectory = Location.of(matcher.group("rootDir")).appendPath(deltaSubdir(writeId, statementId));
        this.deleteDeltaDirectory = Location.of(matcher.group("rootDir")).appendPath(deleteDeltaSubdir(writeId, statementId));
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null");
    }

    @Override
    public void appendRows(Page page)
    {
        if (page.getPositionCount() == 0) {
            return;
        }

        MergePage mergePage = createDeleteAndInsertPages(page, inputColumns.size());
        mergePage.getDeletionsPage().ifPresent(deletePage -> {
            Block acidBlock = deletePage.getBlock(deletePage.getChannelCount() - 1);
            Page orcDeletePage = buildDeletePage(acidBlock, transaction.getWriteId());
            getOrCreateDeleteFileWriter().appendRows(orcDeletePage);
            deleteRowCount += deletePage.getPositionCount();
        });
        mergePage.getInsertionsPage().ifPresent(insertPage -> {
            Page orcInsertPage = buildInsertPage(insertPage, transaction.getWriteId(), inputColumns, bucketValueBlock, insertRowCount);
            getOrCreateInsertFileWriter().appendRows(orcInsertPage);
            insertRowCount += insertPage.getPositionCount();
        });
    }

    @VisibleForTesting
    public static Page buildInsertPage(Page insertPage, long writeId, List<HiveColumnHandle> columns, Block bucketValueBlock, int insertRowCount)
    {
        int positionCount = insertPage.getPositionCount();
        List<Block> dataColumns = columns.stream()
                .filter(column -> !column.isPartitionKey() && !column.isHidden())
                .map(column -> insertPage.getBlock(column.getBaseHiveColumnIndex()))
                .collect(toImmutableList());
        Block mergedColumnsBlock = RowBlock.fromFieldBlocks(positionCount, Optional.empty(), dataColumns.toArray(new Block[] {}));
        Block currentTransactionBlock = RunLengthEncodedBlock.create(BIGINT, writeId, positionCount);
        Block[] blockArray = {
                RunLengthEncodedBlock.create(INSERT_OPERATION_BLOCK, positionCount),
                currentTransactionBlock,
                RunLengthEncodedBlock.create(bucketValueBlock, positionCount),
                createRowIdBlock(positionCount, insertRowCount),
                currentTransactionBlock,
                mergedColumnsBlock
        };

        return new Page(blockArray);
    }

    @Override
    public long getWrittenBytes()
    {
        return deleteFileWriter.map(FileWriter::getWrittenBytes).orElse(0L) +
                insertFileWriter.map(FileWriter::getWrittenBytes).orElse(0L);
    }

    @Override
    public long getMemoryUsage()
    {
        return (deleteFileWriter.map(FileWriter::getMemoryUsage).orElse(0L)) +
                (insertFileWriter.map(FileWriter::getMemoryUsage).orElse(0L));
    }

    @Override
    public Closeable commit()
    {
        Optional<Closeable> deleteRollbackAction = deleteFileWriter.map(FileWriter::commit);
        Optional<Closeable> insertRollbackAction = insertFileWriter.map(FileWriter::commit);
        return () -> {
            try (Closer closer = Closer.create()) {
                insertRollbackAction.ifPresent(closer::register);
                deleteRollbackAction.ifPresent(closer::register);
            }
        };
    }

    @Override
    public void rollback()
    {
        // Make sure both writers get rolled back
        try (Closer closer = Closer.create()) {
            closer.register(() -> insertFileWriter.ifPresent(FileWriter::rollback));
            closer.register(() -> deleteFileWriter.ifPresent(FileWriter::rollback));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return (deleteFileWriter.map(FileWriter::getValidationCpuNanos).orElse(0L)) +
                (insertFileWriter.map(FileWriter::getValidationCpuNanos).orElse(0L));
    }

    public PartitionUpdateAndMergeResults getPartitionUpdateAndMergeResults(PartitionUpdate partitionUpdate)
    {
        return new PartitionUpdateAndMergeResults(
                partitionUpdate.withRowCount(insertRowCount - deleteRowCount),
                insertRowCount,
                insertFileWriter.isPresent() ? Optional.of(deltaDirectory.toString()) : Optional.empty(),
                deleteRowCount,
                deleteFileWriter.isPresent() ? Optional.of(deleteDeltaDirectory.toString()) : Optional.empty());
    }

    private Page buildDeletePage(Block rowIds, long writeId)
    {
        ColumnarRow columnarRow = toColumnarRow(rowIds);
        checkArgument(!columnarRow.mayHaveNull(), "The rowIdsRowBlock may not have null rows");
        int positionCount = rowIds.getPositionCount();
        // We've verified that the rowIds block has no null rows, so it's okay to get the field blocks
        Block[] blockArray = {
                RunLengthEncodedBlock.create(DELETE_OPERATION_BLOCK, positionCount),
                columnarRow.getField(ORIGINAL_TRANSACTION_CHANNEL),
                columnarRow.getField(BUCKET_CHANNEL),
                columnarRow.getField(ROW_ID_CHANNEL),
                RunLengthEncodedBlock.create(BIGINT, writeId, positionCount),
                RunLengthEncodedBlock.create(hiveRowTypeNullsBlock, positionCount),
        };
        return new Page(blockArray);
    }

    private FileWriter getOrCreateInsertFileWriter()
    {
        if (insertFileWriter.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            insertFileWriter = orcFileWriterFactory.createFileWriter(
                    deltaDirectory.appendPath(bucketFilename),
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    compressionCodec,
                    schemaCopy,
                    session,
                    bucketNumber,
                    transaction,
                    true,
                    WriterKind.INSERT);
        }
        return getWriter(insertFileWriter);
    }

    private FileWriter getOrCreateDeleteFileWriter()
    {
        if (deleteFileWriter.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            Location deletePath = deleteDeltaDirectory.appendPath(bucketFilename);
            FileWriter writer = getWriter(orcFileWriterFactory.createFileWriter(
                    deletePath,
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    compressionCodec,
                    schemaCopy,
                    session,
                    bucketNumber,
                    transaction,
                    true,
                    WriterKind.DELETE));
            deleteFileWriter = Optional.of(sortingFileWriterMaker.makeFileWriter(writer, deletePath));
        }
        return getWriter(deleteFileWriter);
    }

    private static Block createRowIdBlock(int positionCount, int rowCounter)
    {
        long[] rowIds = new long[positionCount];
        for (int index = 0; index < positionCount; index++) {
            rowIds[index] = rowCounter;
            rowCounter++;
        }
        return new LongArrayBlock(positionCount, Optional.empty(), rowIds);
    }

    private static FileWriter getWriter(Optional<FileWriter> writer)
    {
        return writer.orElseThrow(() -> new IllegalArgumentException("writer is not present"));
    }
}
