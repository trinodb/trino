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
package io.prestosql.plugin.hive.orc;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.prestosql.orc.OrcWriter;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.plugin.hive.FileWriter;
import io.prestosql.plugin.hive.acid.AcidTransaction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcFileWriter
        implements FileWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcFileWriter.class).instanceSize();
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    protected final OrcWriter orcWriter;
    private final AcidTransaction transaction;
    private final boolean useAcidSchema;
    private final OptionalInt bucketNumber;
    private final Callable<Void> rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<OrcDataSource>> validationInputFactory;
    private OptionalLong maxWriteId = OptionalLong.empty();

    private long validationCpuNanos;

    public OrcFileWriter(
            OrcDataSink orcDataSink,
            AcidTransaction transaction,
            boolean useAcidSchema,
            OptionalInt bucketNumber,
            Callable<Void> rollbackAction,
            List<String> columnNames,
            List<Type> fileColumnTypes,
            ColumnMetadata<OrcType> fileColumnOrcTypes,
            CompressionKind compression,
            OrcWriterOptions options,
            boolean writeLegacyVersion,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            Optional<Supplier<OrcDataSource>> validationInputFactory,
            OrcWriteValidationMode validationMode,
            OrcWriterStats stats)
    {
        requireNonNull(orcDataSink, "orcDataSink is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.useAcidSchema = useAcidSchema;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");

        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");

        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "outputColumnInputIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : fileColumnTypes) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(null, 1, 0);
            blockBuilder.appendNull();
            nullBlocks.add(blockBuilder.build());
        }
        this.nullBlocks = nullBlocks.build();
        this.validationInputFactory = validationInputFactory;
        orcWriter = new OrcWriter(
                orcDataSink,
                columnNames,
                fileColumnTypes,
                fileColumnOrcTypes,
                compression,
                options,
                writeLegacyVersion,
                metadata,
                validationInputFactory.isPresent(),
                validationMode,
                stats);
    }

    @Override
    public long getWrittenBytes()
    {
        return orcWriter.getWrittenBytes() + orcWriter.getBufferedBytes();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + orcWriter.getRetainedBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        Block[] blocks = new Block[fileInputColumnIndexes.length];
        boolean[] nullBlocksArray = new boolean[fileInputColumnIndexes.length];
        boolean hasNullBlocks = false;
        int positionCount = dataPage.getPositionCount();
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex < 0) {
                hasNullBlocks = true;
                blocks[i] = new RunLengthEncodedBlock(nullBlocks.get(i), positionCount);
            }
            else {
                blocks[i] = dataPage.getBlock(inputColumnIndex);
            }
            nullBlocksArray[i] = inputColumnIndex < 0;
        }
        if (transaction.isInsert() && useAcidSchema) {
            Optional<boolean[]> nullBlocks = hasNullBlocks ? Optional.of(nullBlocksArray) : Optional.empty();
            Block rowBlock = RowBlock.fromFieldBlocks(positionCount, nullBlocks, blocks);
            blocks = buildAcidColumns(rowBlock, transaction);
        }
        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            orcWriter.write(page);
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public void commit()
    {
        try {
            if (transaction.isAcidTransactionRunning() && useAcidSchema) {
                updateUserMetadata();
            }
            orcWriter.close();
        }
        catch (IOException | UncheckedIOException e) {
            try {
                rollbackAction.call();
            }
            catch (Exception ignored) {
                // ignore
            }
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }

        if (validationInputFactory.isPresent()) {
            try {
                try (OrcDataSource input = validationInputFactory.get().get()) {
                    long startThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                    orcWriter.validate(input);
                    validationCpuNanos += THREAD_MX_BEAN.getCurrentThreadCpuTime() - startThreadCpuTime;
                }
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(HIVE_WRITE_VALIDATION_FAILED, e);
            }
        }
    }

    private void updateUserMetadata()
    {
        int bucketValue = computeBucketValue(bucketNumber.orElse(0), 0);
        long writeId = maxWriteId.isPresent() ? maxWriteId.getAsLong() : transaction.getWriteId();
        if (transaction.isAcidTransactionRunning()) {
            int stripeRowCount = orcWriter.getStripeRowCount();
            Map<String, String> userMetadata = new HashMap<>();
            OrcWriter.OrcOperation operation = transaction.getOrcOperation();
            switch (operation) {
                case INSERT:
                    userMetadata.put("hive.acid.stats", format("%s,0,0", stripeRowCount));
                    break;
                case DELETE:
                    userMetadata.put("hive.acid.stats", format("0,0,%s", stripeRowCount));
                    break;
                default:
                    throw new IllegalStateException("In updateUserMetadata, unknown OrcOperation " + operation);
            }
            userMetadata.put("hive.acid.key.index", format("%s,%s,%s;", writeId, bucketValue, stripeRowCount - 1));
            userMetadata.put("hive.acid.version", "2");

            orcWriter.updateUserMetadata(userMetadata);
        }
    }

    @Override
    public void rollback()
    {
        try {
            try {
                orcWriter.close();
            }
            finally {
                rollbackAction.call();
            }
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    public int getStripeRowCount()
    {
        return orcWriter.getStripeRowCount();
    }

    public void setMaxWriteId(long maxWriteId)
    {
        this.maxWriteId = OptionalLong.of(maxWriteId);
    }

    public OptionalLong getMaxWriteId()
    {
        return maxWriteId;
    }

    public void updateUserMetadata(Map<String, String> userMetadata)
    {
        orcWriter.updateUserMetadata(userMetadata);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", orcWriter)
                .toString();
    }

    private Block[] buildAcidColumns(Block rowBlock, AcidTransaction transaction)
    {
        int positionCount = rowBlock.getPositionCount();
        int bucketValue = computeBucketValue(bucketNumber.orElse(0), 0);
        // operation, originalWriteId, bucket, rowId, currentWriteId, row<>
        return new Block[] {
                RunLengthEncodedBlock.create(INTEGER, (long) getOrcOperation(transaction), positionCount),
                RunLengthEncodedBlock.create(BIGINT, transaction.getWriteId(), positionCount),
                RunLengthEncodedBlock.create(INTEGER, (long) bucketValue, positionCount),
                buildAcidRowIdsColumn(positionCount),
                RunLengthEncodedBlock.create(BIGINT, transaction.getWriteId(), positionCount),
                rowBlock
        };
    }

    private int getOrcOperation(AcidTransaction transaction)
    {
        switch (transaction.getOperation()) {
            case INSERT:
                return 0;
            default:
                throw new VerifyException("In getOrcOperation, the transaction operation is not allowed, transaction " + transaction);
        }
    }

    private Block buildAcidRowIdsColumn(int positionCount)
    {
        long[] rowIds = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            rowIds[i] = i;
        }
        return new LongArrayBlock(positionCount, Optional.empty(), rowIds);
    }

    public static int extractBucketNumber(int bucketValue)
    {
        return (bucketValue >> 16) & 0xFFF;
    }

    public static int computeBucketValue(int bucketId, int statementId)
    {
        checkArgument(statementId >= 0 && statementId < 1 << 16, "statementId should be non-negative and less than 1 << 16, but is %s", statementId);
        checkArgument(bucketId >= 0 && bucketId <= 1 << 13, "bucketId should be non-negative and less than 1 << 13, but is %s", bucketId);
        return 1 << 29 | bucketId << 16 | statementId;
    }
}
