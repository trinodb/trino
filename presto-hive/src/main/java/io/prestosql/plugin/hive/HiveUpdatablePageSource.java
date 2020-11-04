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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.orc.OrcFileWriter;
import io.prestosql.plugin.hive.orc.OrcFileWriterFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.orc.OrcWriter.OrcOperation.DELETE;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.PartitionAndStatementId.CODEC;
import static io.prestosql.plugin.hive.acid.AcidSchema.ACID_COLUMN_NAMES;
import static io.prestosql.plugin.hive.acid.AcidSchema.createAcidSchema;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.prestosql.spi.predicate.Utils.nativeValueToBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deleteDeltaSubdir;

public class HiveUpdatablePageSource
        implements UpdatablePageSource
{
    // The channel numbers of the child blocks in the RowBlock passed to deleteRows()
    public static final int ORIGINAL_TRANSACTION_CHANNEL = 0;
    public static final int ROW_ID_CHANNEL = 1;
    public static final int BUCKET_CHANNEL = 2;
    public static final int ACID_ROW_STRUCT_COLUMN_ID = 6;
    public static final Block DELETE_OPERATION_BLOCK = nativeValueToBlock(INTEGER, Long.valueOf(DELETE.getOperationNumber()));

    // The bucketPath looks like .../delta_nnnnnnn_mmmmmmm_ssss/bucket_bbbbb(_aaaa)?
    public static final Pattern BUCKET_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>delta_[\\d]+_[\\d]+)_(?<statementId>[\\d]+)/(?<filenameBase>bucket_(?<bucketNumber>[\\d]+))(?<attemptId>_[\\d]+)?$");
    // The orignal file path looks like .../nnnnnnn_m(_copy_ccc)?
    public static final Pattern ORIGINAL_FILE_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<filename>(?<bucketNumber>[\\d]+)_(?<rest>.*)?)$");

    private final HiveTableHandle hiveTable;
    private final String partitionName;
    private final int statementId;
    private final ConnectorPageSource hivePageSource;
    private final OptionalInt bucketNumber;
    private final OrcFileWriterFactory orcFileWriterFactory;
    private final Configuration configuration;
    private final ConnectorSession session;
    private final Block hiveRowTypeNullsBlock;
    private final long writeId;
    private final Properties hiveAcidSchema;
    private final Path deleteDeltaDirectory;
    private final String bucketFilename;
    private long maxWriteId;
    private long rowCount;

    private Optional<FileWriter> writer = Optional.empty();
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
            HiveType hiveRowType)
    {
        this.hiveTable = requireNonNull(hiveTableHandle, "hiveTable is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.statementId = statementId;
        this.hivePageSource = requireNonNull(hivePageSource, "hivePageSource is null");
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.session = requireNonNull(session, "session is null");
        this.hiveRowTypeNullsBlock = nativeValueToBlock(hiveRowType.getType(typeManager), null);
        checkArgument(hiveTableHandle.isInAcidTransaction(), "Not in a transaction; hiveTableHandle: %s", hiveTableHandle);
        this.writeId = hiveTableHandle.getWriteId();
        this.hiveAcidSchema = createAcidSchema(hiveRowType);
        requireNonNull(bucketPath, "bucketPath is null");
        if (originalFile) {
            Matcher matcher = ORIGINAL_FILE_PATH_MATCHER.matcher(bucketPath.toString());
            checkArgument(matcher.matches(), "Original file bucketPath doesn't have the required format: %s", bucketPath);
            this.bucketFilename = format("bucket_%05d", bucketNumber.isEmpty() ? 0 : bucketNumber.getAsInt());
            this.deleteDeltaDirectory = new Path(format("%s/%s", matcher.group("rootDir"), deleteDeltaSubdir(writeId, writeId, statementId)));
        }
        else {
            Matcher matcher = BUCKET_PATH_MATCHER.matcher(bucketPath.toString());
            checkArgument(matcher.matches(), "bucketPath doesn't have the required format: %s", bucketPath);
            // delete_delta bucket files should not have attemptId suffix
            this.bucketFilename = matcher.group("filenameBase");
            this.deleteDeltaDirectory = new Path(format("%s/%s", matcher.group("rootDir"), deleteDeltaSubdir(writeId, writeId, statementId)));
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        int positionCount = rowIds.getPositionCount();
        List<Block> blocks = rowIds.getChildren();
        checkArgument(blocks.size() == 3, "The rowId block should have 3 children, but has " + blocks.size());
        Block[] blockArray = new Block[] {
                new RunLengthEncodedBlock(DELETE_OPERATION_BLOCK, positionCount),
                blocks.get(ORIGINAL_TRANSACTION_CHANNEL),
                blocks.get(BUCKET_CHANNEL),
                blocks.get(ROW_ID_CHANNEL),
                RunLengthEncodedBlock.create(BIGINT, writeId, positionCount),
                new RunLengthEncodedBlock(hiveRowTypeNullsBlock, positionCount)
        };
        Page deletePage = new Page(blockArray);

        Block block = blocks.get(ORIGINAL_TRANSACTION_CHANNEL);
        for (int index = 0; index < positionCount; index++) {
            maxWriteId = Math.max(maxWriteId, block.getLong(index, 0));
        }

        lazyInitializeFileWriter();
        checkArgument(writer.isPresent(), "writer not present");
        writer.get().appendRows(deletePage);
        rowCount += positionCount;
    }

    private void lazyInitializeFileWriter()
    {
        if (writer.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            writer = orcFileWriterFactory.createFileWriter(
                    new Path(format("%s/%s", deleteDeltaDirectory, bucketFilename)),
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    schemaCopy,
                    toJobConf(configuration),
                    session,
                    bucketNumber,
                    hiveTable.getTransaction(),
                    true);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (writer.isPresent()) {
            OrcFileWriter orcFileWriter = (OrcFileWriter) writer.get();
            orcFileWriter.setMaxWriteId(maxWriteId);
            orcFileWriter.commit();
            Slice fragment = Slices.wrappedBuffer(CODEC.toJsonBytes(new PartitionAndStatementId(partitionName, statementId, rowCount, deleteDeltaDirectory.toString())));
            return completedFuture(ImmutableList.of(fragment));
        }
        return completedFuture(ImmutableList.of());
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
        return page;
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
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, e);
        }
    }
}
