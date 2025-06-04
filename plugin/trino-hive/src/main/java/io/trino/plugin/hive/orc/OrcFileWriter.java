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
package io.trino.plugin.hive.orc;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.io.Closeable;
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
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class OrcFileWriter
        implements FileWriter
{
    private static final Logger log = Logger.get(OrcFileWriter.class);
    private static final int INSTANCE_SIZE = instanceSize(OrcFileWriter.class);
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final OrcWriter orcWriter;
    private final WriterKind writerKind;
    private final AcidTransaction transaction;
    private final boolean useAcidSchema;
    private final OptionalInt bucketNumber;
    private final Closeable rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<OrcDataSource>> validationInputFactory;
    private OptionalLong maxWriteId = OptionalLong.empty();
    private long nextRowId;

    private long validationCpuNanos;

    public OrcFileWriter(
            OrcDataSink orcDataSink,
            WriterKind writerKind,
            AcidTransaction transaction,
            boolean useAcidSchema,
            OptionalInt bucketNumber,
            Closeable rollbackAction,
            List<String> columnNames,
            List<Type> fileColumnTypes,
            ColumnMetadata<OrcType> fileColumnOrcTypes,
            CompressionKind compression,
            OrcWriterOptions options,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            Optional<Supplier<OrcDataSource>> validationInputFactory,
            OrcWriteValidationMode validationMode,
            OrcWriterStats stats)
    {
        requireNonNull(orcDataSink, "orcDataSink is null");
        this.writerKind = requireNonNull(writerKind, "writerKind is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.useAcidSchema = useAcidSchema;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");

        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");

        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "fileInputColumnIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : fileColumnTypes) {
            nullBlocks.add(fileColumnType.createNullBlock());
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
                metadata,
                validationInputFactory.isPresent(),
                validationMode,
                stats);
        if (transaction.isTransactional()) {
            this.setMaxWriteId(transaction.getWriteId());
        }
    }

    @Override
    public long getWrittenBytes()
    {
        return orcWriter.getWrittenBytes() + orcWriter.getBufferedBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + orcWriter.getRetainedBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        Block[] blocks = new Block[fileInputColumnIndexes.length];
        boolean hasUnwrittenColumn = false;
        int positionCount = dataPage.getPositionCount();
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex < 0) {
                blocks[i] = RunLengthEncodedBlock.create(nullBlocks.get(i), positionCount);
                hasUnwrittenColumn = true;
            }
            else {
                blocks[i] = dataPage.getBlock(inputColumnIndex);
            }
        }
        if (transaction.isInsert() && useAcidSchema) {
            verify(!hasUnwrittenColumn, "Unwritten columns are not supported for ACID transactional insert");
            Block rowBlock = RowBlock.fromFieldBlocks(positionCount, blocks);
            blocks = buildAcidColumns(rowBlock, transaction);
        }
        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            orcWriter.write(page);
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            if (transaction.isAcidTransactionRunning() && useAcidSchema) {
                updateAcidUserMetadata();
            }
            orcWriter.close();
        }
        catch (IOException | UncheckedIOException e) {
            try {
                rollbackAction.close();
            }
            catch (Exception ex) {
                // ignore
                log.error(ex, "Exception when committing file");
            }
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
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
                throw new TrinoException(HIVE_WRITE_VALIDATION_FAILED, e);
            }
        }

        return rollbackAction;
    }

    private void updateAcidUserMetadata()
    {
        int bucketValue = computeBucketValue(bucketNumber.orElse(0), 0);
        long writeId = maxWriteId.isPresent() ? maxWriteId.getAsLong() : transaction.getWriteId();
        int stripeRowCount = orcWriter.getStripeRowCount();
        Map<String, String> userMetadata = new HashMap<>();
        switch (writerKind) {
            case INSERT:
                userMetadata.put("hive.acid.stats", format("%s,0,0", stripeRowCount));
                break;
            case DELETE:
                userMetadata.put("hive.acid.stats", format("0,0,%s", stripeRowCount));
                break;
            default:
                throw new IllegalStateException("In updateUserMetadata, unknown writerKind " + writerKind);
        }
        userMetadata.put("hive.acid.key.index", format("%s,%s,%s;", writeId, bucketValue, stripeRowCount - 1));
        userMetadata.put("hive.acid.version", "2");

        orcWriter.updateUserMetadata(userMetadata);
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            orcWriter.close();
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    public void setMaxWriteId(long maxWriteId)
    {
        this.maxWriteId = OptionalLong.of(maxWriteId);
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
        return switch (transaction.getOperation()) {
            case INSERT -> 0;
            default -> throw new VerifyException("In getOrcOperation, the transaction operation is not allowed, transaction " + transaction);
        };
    }

    private Block buildAcidRowIdsColumn(int positionCount)
    {
        long[] rowIds = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            rowIds[i] = nextRowId++;
        }
        return new LongArrayBlock(positionCount, Optional.empty(), rowIds);
    }

    public static int computeBucketValue(int bucketId, int statementId)
    {
        checkArgument(statementId >= 0 && statementId < 1 << 16, "statementId should be non-negative and less than 1 << 16, but is %s", statementId);
        checkArgument(bucketId >= 0 && bucketId <= 1 << 13, "bucketId should be non-negative and less than 1 << 13, but is %s", bucketId);
        return 1 << 29 | bucketId << 16 | statementId;
    }
}
