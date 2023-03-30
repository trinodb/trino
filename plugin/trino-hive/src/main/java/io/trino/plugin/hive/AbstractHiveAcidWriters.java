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
import io.trino.plugin.hive.HiveWriterFactory.RowIdSortingFileWriterMaker;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.orc.OrcFileWriter;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hdfs.ConfigurationUtils.toJobConf;
import static io.trino.orc.OrcWriter.OrcOperation.DELETE;
import static io.trino.orc.OrcWriter.OrcOperation.INSERT;
import static io.trino.plugin.hive.HivePageSource.BUCKET_CHANNEL;
import static io.trino.plugin.hive.HivePageSource.ORIGINAL_TRANSACTION_CHANNEL;
import static io.trino.plugin.hive.HivePageSource.ROW_ID_CHANNEL;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_NAMES;
import static io.trino.plugin.hive.acid.AcidSchema.createAcidSchema;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.AcidTables.deleteDeltaSubdir;
import static io.trino.plugin.hive.util.AcidTables.deltaSubdir;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractHiveAcidWriters
{
    protected static final Block DELETE_OPERATION_BLOCK = nativeValueToBlock(INTEGER, Long.valueOf(DELETE.getOperationNumber()));
    protected static final Block INSERT_OPERATION_BLOCK = nativeValueToBlock(INTEGER, Long.valueOf(INSERT.getOperationNumber()));

    // The bucketPath looks like .../delta_nnnnnnn_mmmmmmm_ssss/bucket_bbbbb(_aaaa)?
    public static final Pattern BUCKET_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>delta_[\\d]+_[\\d]+)_(?<statementId>[\\d]+)/(?<filenameBase>bucket_(?<bucketNumber>[\\d]+))(?<attemptId>_[\\d]+)?$");
    // The original file path looks like .../nnnnnnn_m(_copy_ccc)?
    public static final Pattern ORIGINAL_FILE_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<filename>(?<bucketNumber>[\\d]+)_(?<rest>.*)?)$");
    // After compaction, the bucketPath looks like .../base_nnnnnnn(_vmmmmmmm)?/bucket_bbbbb(_aaaa)?
    public static final Pattern BASE_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>base_[-]?[\\d]+(_v[\\d]+)?)/(?<filenameBase>bucket_(?<bucketNumber>[\\d]+))(?<attemptId>_[\\d]+)?$");

    protected final AcidTransaction transaction;
    protected final OptionalInt bucketNumber;
    protected final int statementId;
    protected final Block bucketValueBlock;

    private final Optional<RowIdSortingFileWriterMaker> sortingFileWriterMaker;
    private final OrcFileWriterFactory orcFileWriterFactory;
    private final Configuration configuration;
    protected final ConnectorSession session;
    private final AcidOperation updateKind;
    private final Properties hiveAcidSchema;
    protected final Block hiveRowTypeNullsBlock;
    protected Path deltaDirectory;
    protected final Path deleteDeltaDirectory;
    private final String bucketFilename;

    protected Optional<FileWriter> deleteFileWriter = Optional.empty();
    protected Optional<FileWriter> insertFileWriter = Optional.empty();

    public AbstractHiveAcidWriters(
            AcidTransaction transaction,
            int statementId,
            OptionalInt bucketNumber,
            Optional<RowIdSortingFileWriterMaker> sortingFileWriterMaker,
            Path bucketPath,
            boolean originalFile,
            OrcFileWriterFactory orcFileWriterFactory,
            Configuration configuration,
            ConnectorSession session,
            TypeManager typeManager,
            HiveType hiveRowType,
            AcidOperation updateKind)
    {
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.statementId = statementId;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.sortingFileWriterMaker = requireNonNull(sortingFileWriterMaker, "sortingFileWriterMaker is null");
        this.bucketValueBlock = nativeValueToBlock(INTEGER, Long.valueOf(OrcFileWriter.computeBucketValue(bucketNumber.orElse(0), statementId)));
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.session = requireNonNull(session, "session is null");
        checkArgument(transaction.isTransactional(), "Not in a transaction: %s", transaction);
        this.updateKind = requireNonNull(updateKind, "updateKind is null");
        this.hiveAcidSchema = createAcidSchema(hiveRowType);
        this.hiveRowTypeNullsBlock = nativeValueToBlock(hiveRowType.getType(typeManager), null);
        requireNonNull(bucketPath, "bucketPath is null");
        checkArgument(updateKind != AcidOperation.MERGE || sortingFileWriterMaker.isPresent(), "updateKind is MERGE but sortingFileWriterMaker is not present");
        Matcher matcher;
        if (originalFile) {
            matcher = ORIGINAL_FILE_PATH_MATCHER.matcher(bucketPath.toString());
            checkArgument(matcher.matches(), "Original file bucketPath doesn't have the required format: %s", bucketPath);
            this.bucketFilename = format("bucket_%05d", bucketNumber.isEmpty() ? 0 : bucketNumber.getAsInt());
        }
        else {
            matcher = BASE_PATH_MATCHER.matcher(bucketPath.toString());
            if (matcher.matches()) {
                this.bucketFilename = matcher.group("filenameBase");
            }
            else {
                matcher = BUCKET_PATH_MATCHER.matcher(bucketPath.toString());
                checkArgument(matcher.matches(), "bucketPath doesn't have the required format: %s", bucketPath);
                this.bucketFilename = matcher.group("filenameBase");
            }
        }
        long writeId = transaction.getWriteId();
        this.deltaDirectory = new Path(format("%s/%s", matcher.group("rootDir"), deltaSubdir(writeId, statementId)));
        this.deleteDeltaDirectory = new Path(format("%s/%s", matcher.group("rootDir"), deleteDeltaSubdir(writeId, statementId)));
    }

    protected Page buildDeletePage(Block rowIds, long writeId)
    {
        return buildDeletePage(rowIds, writeId, hiveRowTypeNullsBlock);
    }

    @VisibleForTesting
    public static Page buildDeletePage(Block rowIdsRowBlock, long writeId, Block rowTypeNullsBlock)
    {
        ColumnarRow columnarRow = toColumnarRow(rowIdsRowBlock);
        checkArgument(!columnarRow.mayHaveNull(), "The rowIdsRowBlock may not have null rows");
        int positionCount = rowIdsRowBlock.getPositionCount();
        // We've verified that the rowIds block has no null rows, so it's okay to get the field blocks
        Block[] blockArray = {
                RunLengthEncodedBlock.create(DELETE_OPERATION_BLOCK, positionCount),
                columnarRow.getField(ORIGINAL_TRANSACTION_CHANNEL),
                columnarRow.getField(BUCKET_CHANNEL),
                columnarRow.getField(ROW_ID_CHANNEL),
                RunLengthEncodedBlock.create(BIGINT, writeId, positionCount),
                RunLengthEncodedBlock.create(rowTypeNullsBlock, positionCount),
        };
        return new Page(blockArray);
    }

    @VisibleForTesting
    public static Block createRowIdBlock(int positionCount, int rowCounter)
    {
        long[] rowIds = new long[positionCount];
        for (int index = 0; index < positionCount; index++) {
            rowIds[index] = rowCounter;
            rowCounter++;
        }
        return new LongArrayBlock(positionCount, Optional.empty(), rowIds);
    }

    protected FileWriter getOrCreateDeleteFileWriter()
    {
        if (deleteFileWriter.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            Path deletePath = new Path(format("%s/%s", deleteDeltaDirectory, bucketFilename));
            deleteFileWriter = orcFileWriterFactory.createFileWriter(
                    deletePath,
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    schemaCopy,
                    toJobConf(configuration),
                    session,
                    bucketNumber,
                    transaction,
                    true,
                    WriterKind.DELETE);
            if (updateKind == AcidOperation.MERGE) {
                deleteFileWriter = Optional.of(sortingFileWriterMaker.orElseThrow(() -> new IllegalArgumentException("sortingFileWriterMaker not present"))
                        .makeFileWriter(getWriter(deleteFileWriter), deletePath));
            }
        }
        return getWriter(deleteFileWriter);
    }

    private FileWriter getWriter(Optional<FileWriter> writer)
    {
        return writer.orElseThrow(() -> new IllegalArgumentException("writer is not present"));
    }

    protected FileWriter getOrCreateInsertFileWriter()
    {
        if (insertFileWriter.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            insertFileWriter = orcFileWriterFactory.createFileWriter(
                    new Path(format("%s/%s", deltaDirectory, bucketFilename)),
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    schemaCopy,
                    toJobConf(configuration),
                    session,
                    bucketNumber,
                    transaction,
                    true,
                    WriterKind.INSERT);
        }
        return getWriter(insertFileWriter);
    }
}
