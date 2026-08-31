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

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_CLOSE_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_DATA_ERROR;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PaimonPageSink
        implements ConnectorPageSink
{
    private final BatchTableWrite writer;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;
    private final int[] inputChannels;
    private final List<Type> inputColumnTypes;
    private final List<DataType> inputLogicalTypes;
    private final Object[] defaultValues;
    private final int inputChannelCount;
    private final boolean allColumnsPresent;
    @Nullable
    private final MemoryPoolFactory memoryPoolFactory;
    @Nullable
    private final IOManager ioManager;
    @Nullable
    private final DynamicBucketWriter dynamicBucketWriter;
    @Nullable
    private final KeyDynamicWriter keyDynamicWriter;
    private final long additionalMemoryUsage;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long completedBytes;

    public PaimonPageSink(BatchTableWrite writer, List<Type> columnTypes, List<DataType> logicalTypes)
    {
        this(writer, columnTypes, logicalTypes, identityChannels(columnTypes), null);
    }

    public PaimonPageSink(
            BatchTableWrite writer,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            @Nullable DynamicBucketWriter dynamicBucketWriter)
    {
        this(writer, columnTypes, logicalTypes, identityChannels(columnTypes), dynamicBucketWriter);
    }

    public PaimonPageSink(
            BatchTableWrite writer,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int[] inputChannels,
            @Nullable DynamicBucketWriter dynamicBucketWriter)
    {
        this(writer, columnTypes, logicalTypes, inputChannels, emptyDefaultValues(columnTypes), dynamicBucketWriter);
    }

    public PaimonPageSink(
            BatchTableWrite writer,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int[] inputChannels,
            Object[] defaultValues,
            @Nullable DynamicBucketWriter dynamicBucketWriter)
    {
        this(writer, columnTypes, logicalTypes, inputChannels, defaultValues, dynamicBucketWriter, null);
    }

    public PaimonPageSink(
            BatchTableWrite writer,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int[] inputChannels,
            Object[] defaultValues,
            @Nullable DynamicBucketWriter dynamicBucketWriter,
            @Nullable MemoryPoolFactory memoryPoolFactory)
    {
        this(writer,
                columnTypes,
                logicalTypes,
                inputChannels,
                defaultValues,
                dynamicBucketWriter,
                memoryPoolFactory,
                null);
    }

    public PaimonPageSink(
            BatchTableWrite writer,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int[] inputChannels,
            Object[] defaultValues,
            @Nullable DynamicBucketWriter dynamicBucketWriter,
            @Nullable MemoryPoolFactory memoryPoolFactory,
            @Nullable IOManager ioManager)
    {
        this(writer,
                columnTypes,
                logicalTypes,
                inputChannels,
                defaultValues,
                dynamicBucketWriter,
                null,
                memoryPoolFactory,
                ioManager);
    }

    public PaimonPageSink(
            BatchTableWrite writer,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int[] inputChannels,
            Object[] defaultValues,
            @Nullable DynamicBucketWriter dynamicBucketWriter,
            @Nullable KeyDynamicWriter keyDynamicWriter,
            @Nullable MemoryPoolFactory memoryPoolFactory,
            @Nullable IOManager ioManager)
    {
        this(writer,
                columnTypes,
                logicalTypes,
                inputChannels,
                defaultValues,
                dynamicBucketWriter,
                keyDynamicWriter,
                memoryPoolFactory,
                ioManager,
                0);
    }

    PaimonPageSink(
            BatchTableWrite writer,
            List<Type> columnTypes,
            List<DataType> logicalTypes,
            int[] inputChannels,
            Object[] defaultValues,
            @Nullable DynamicBucketWriter dynamicBucketWriter,
            @Nullable KeyDynamicWriter keyDynamicWriter,
            @Nullable MemoryPoolFactory memoryPoolFactory,
            @Nullable IOManager ioManager,
            long additionalMemoryUsage)
    {
        this.writer = requireNonNull(writer, "writer is null");
        this.columnTypes = copyColumnTypes(columnTypes);
        this.logicalTypes = copyLogicalTypes(logicalTypes);
        checkArgument(this.columnTypes.size() == this.logicalTypes.size(),
                "columnTypes and logicalTypes size mismatch: %s != %s",
                this.columnTypes.size(),
                this.logicalTypes.size());
        this.inputChannels = copyInputChannels(inputChannels);
        checkArgument(this.inputChannels.length == this.columnTypes.size(),
                "inputChannels and columnTypes size mismatch: %s != %s",
                this.inputChannels.length,
                this.columnTypes.size());
        this.inputColumnTypes = inputPageTypes(this.columnTypes, this.inputChannels);
        this.inputLogicalTypes = inputPageTypes(this.logicalTypes, this.inputChannels);
        this.defaultValues = copyDefaultValues(defaultValues);
        checkArgument(this.defaultValues.length == this.columnTypes.size(),
                "defaultValues and columnTypes size mismatch: %s != %s",
                this.defaultValues.length,
                this.columnTypes.size());
        this.inputChannelCount = inputChannelCount(this.inputChannels);
        this.allColumnsPresent = allColumnsPresent(this.inputChannels);
        this.memoryPoolFactory = memoryPoolFactory;
        this.ioManager = ioManager;
        this.dynamicBucketWriter = dynamicBucketWriter;
        this.keyDynamicWriter = keyDynamicWriter;
        checkArgument(additionalMemoryUsage >= 0, "additionalMemoryUsage must be non-negative: %s", additionalMemoryUsage);
        this.additionalMemoryUsage = additionalMemoryUsage;
        checkArgument(dynamicBucketWriter == null || keyDynamicWriter == null,
                "dynamic bucket writers are mutually exclusive");
    }

    private static List<Type> copyColumnTypes(List<Type> columnTypes)
    {
        requireNonNull(columnTypes, "columnTypes is null").forEach(columnType ->
                requireNonNull(columnType, "columnTypes contains null type"));
        return List.copyOf(columnTypes);
    }

    private static List<DataType> copyLogicalTypes(List<DataType> logicalTypes)
    {
        requireNonNull(logicalTypes, "logicalTypes is null").forEach(logicalType ->
                requireNonNull(logicalType, "logicalTypes contains null type"));
        return List.copyOf(logicalTypes);
    }

    private static Object[] copyDefaultValues(Object[] defaultValues)
    {
        return requireNonNull(defaultValues, "defaultValues is null").clone();
    }

    private static Object[] emptyDefaultValues(List<?> columns)
    {
        requireNonNull(columns, "columns is null");
        return new Object[columns.size()];
    }

    private static int[] copyInputChannels(int[] inputChannels)
    {
        int[] channels = requireNonNull(inputChannels, "inputChannels is null").clone();
        for (int channel : channels) {
            checkArgument(channel >= -1, "inputChannels contains invalid channel: %s", channel);
            checkArgument(channel < channels.length, "inputChannels contains channel outside field range: %s", channel);
        }
        boolean[] seenChannels = new boolean[inputChannelCount(channels)];
        for (int channel : channels) {
            if (channel >= 0) {
                checkArgument(!seenChannels[channel], "inputChannels contains duplicate channel: %s", channel);
                seenChannels[channel] = true;
            }
        }
        for (int channel = 0; channel < seenChannels.length; channel++) {
            checkArgument(seenChannels[channel], "inputChannels does not contain input page channel: %s", channel);
        }
        return channels;
    }

    private static <T> List<T> inputPageTypes(List<T> fullFieldTypes, int[] inputChannels)
    {
        List<T> inputPageTypes = new ArrayList<>();
        for (int fullField = 0; fullField < inputChannels.length; fullField++) {
            int channel = inputChannels[fullField];
            if (channel >= 0) {
                while (inputPageTypes.size() <= channel) {
                    inputPageTypes.add(null);
                }
                inputPageTypes.set(channel, fullFieldTypes.get(fullField));
            }
        }
        for (int channel = 0; channel < inputPageTypes.size(); channel++) {
            if (inputPageTypes.get(channel) == null) {
                throw new IllegalArgumentException("Input page channel %s is not mapped to a Paimon field"
                        .formatted(channel));
            }
        }
        return List.copyOf(inputPageTypes);
    }

    private static int inputChannelCount(int[] inputChannels)
    {
        int maxChannel = -1;
        for (int inputChannel : inputChannels) {
            maxChannel = Math.max(maxChannel, inputChannel);
        }
        return maxChannel + 1;
    }

    private static int[] identityChannels(List<?> columns)
    {
        requireNonNull(columns, "columns is null");
        int[] channels = new int[columns.size()];
        Arrays.setAll(channels, index -> index);
        return channels;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        long memoryUsage = memoryPoolFactory == null ? 0 : memoryPoolFactory.usedBufferSize();
        return saturatedAdd(memoryUsage, additionalMemoryUsage, "additional memory usage");
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        writePage(page, RowKind.INSERT);
        return NOT_BLOCKED;
    }

    public void writePage(Page page, RowKind rowKind)
    {
        runWithContextClassLoader(() -> {
            writePageInternal(page, rowKind);
            return null;
        }, PaimonPageSink.class.getClassLoader());
    }

    private void writePageInternal(Page page, RowKind rowKind)
    {
        checkState(!closed.get(), "Paimon page sink is already closed");
        requireNonNull(page, "page is null");
        requireNonNull(rowKind, "rowKind is null");
        try {
            validatePageShape(page);
            for (int i = 0; i < page.getPositionCount(); i++) {
                InternalRow row = row(page, i, rowKind);
                if (dynamicBucketWriter == null && keyDynamicWriter == null) {
                    writer.write(row);
                }
                else if (dynamicBucketWriter != null) {
                    dynamicBucketWriter.write(writer, row);
                }
                else {
                    keyDynamicWriter.write(row);
                }
            }
            completedBytes = saturatedAdd(completedBytes, page.getSizeInBytes(), "completed bytes");
        }
        catch (Exception e) {
            throw wrapWriteException(e);
        }
    }

    private void validatePageShape(Page page)
    {
        int pageChannelCount = page.getChannelCount();
        if (pageChannelCount != inputChannelCount) {
            throw new IllegalArgumentException("page channel count (%s) must match write column count (%s)"
                    .formatted(pageChannelCount, inputChannelCount));
        }
    }

    private InternalRow row(Page page, int position, RowKind rowKind)
    {
        if (allColumnsPresent) {
            return PaimonRow.fromTrustedTypeLists(page, position, rowKind, columnTypes, logicalTypes);
        }
        return new MappedPaimonRow(
                page,
                position,
                rowKind,
                inputColumnTypes,
                inputLogicalTypes,
                inputChannels,
                defaultValues,
                logicalTypes);
    }

    private static boolean allColumnsPresent(int[] inputChannels)
    {
        for (int index = 0; index < inputChannels.length; index++) {
            if (inputChannels[index] != index) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return runWithContextClassLoader(this::finishInternal, PaimonPageSink.class.getClassLoader());
    }

    private CompletableFuture<Collection<Slice>> finishInternal()
    {
        checkState(!closed.get(), "Paimon page sink is already closed");
        Collection<Slice> commitTasks = new ArrayList<>();
        RuntimeException failure = null;
        try {
            if (dynamicBucketWriter != null) {
                dynamicBucketWriter.prepareCommit();
            }
            List<CommitMessage> commitMessages = requireNonNull(writer.prepareCommit(), "Paimon writer returned null commit messages");
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            for (CommitMessage commitMessage : commitMessages) {
                commitTasks.add(wrappedBuffer(serializer.serialize(
                        requireNonNull(commitMessage, "Paimon writer returned null commit message"))));
            }
        }
        catch (Exception e) {
            failure = wrapWriteException(e);
        }
        failure = close(failure, false);
        if (failure != null) {
            throw failure;
        }
        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        runWithContextClassLoader(() -> {
            abortInternal();
            return null;
        }, PaimonPageSink.class.getClassLoader());
    }

    private void abortInternal()
    {
        RuntimeException failure = close(null, true);
        if (failure != null) {
            throw failure;
        }
    }

    @Nullable
    private RuntimeException close(@Nullable RuntimeException failure)
    {
        return close(failure, false);
    }

    @Nullable
    private RuntimeException close(@Nullable RuntimeException failure, boolean abort)
    {
        if (!closed.compareAndSet(false, true)) {
            return failure;
        }
        failure = closeKeyDynamicWriter(keyDynamicWriter, failure, abort);
        failure = closeWriter(writer, failure);
        failure = closeIoManager(ioManager, failure);
        return failure;
    }

    @Nullable
    static RuntimeException closeKeyDynamicWriter(
            @Nullable KeyDynamicWriter keyDynamicWriter,
            @Nullable RuntimeException failure)
    {
        return closeKeyDynamicWriter(keyDynamicWriter, failure, false);
    }

    @Nullable
    private static RuntimeException closeKeyDynamicWriter(
            @Nullable KeyDynamicWriter keyDynamicWriter,
            @Nullable RuntimeException failure,
            boolean abort)
    {
        if (keyDynamicWriter == null) {
            return failure;
        }
        try {
            if (abort) {
                keyDynamicWriter.abort();
            }
            else {
                keyDynamicWriter.close();
            }
        }
        catch (Exception e) {
            RuntimeException closeFailure = wrapWriterCloseException(e);
            if (failure != null) {
                failure.addSuppressed(closeFailure);
            }
            else {
                failure = closeFailure;
            }
        }
        return failure;
    }

    @Nullable
    static RuntimeException closeWriter(BatchTableWrite writer, @Nullable RuntimeException failure)
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            RuntimeException closeFailure = wrapWriterCloseException(e);
            if (failure != null) {
                failure.addSuppressed(closeFailure);
            }
            else {
                failure = closeFailure;
            }
        }
        return failure;
    }

    @Nullable
    static RuntimeException closeIoManager(@Nullable IOManager ioManager, @Nullable RuntimeException failure)
    {
        if (ioManager == null) {
            return failure;
        }
        try {
            ioManager.close();
        }
        catch (Exception e) {
            RuntimeException closeFailure = wrapIoManagerCloseException(e);
            if (failure != null) {
                failure.addSuppressed(closeFailure);
            }
            else {
                failure = closeFailure;
            }
        }
        return failure;
    }

    static RuntimeException wrapWriteException(Exception exception)
    {
        Throwable writeFailure = firstMatchingCause(exception, PaimonPageSink::isRecognizedWriteFailure);
        if (writeFailure instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (writeFailure instanceof UnsupportedOperationException unsupportedOperationException) {
            String detail = unsupportedOperationException.getMessage();
            return new TrinoException(
                    NOT_SUPPORTED,
                    detail == null || detail.isBlank()
                            ? "Paimon write uses features which are not supported by the Trino connector"
                            : "Paimon write uses features which are not supported by the Trino connector: " + detail,
                    unsupportedOperationException);
        }
        if (writeFailure instanceof Exception writeException && isWriterDataException(writeException)) {
            return new TrinoException(PAIMON_WRITER_DATA_ERROR, writerDataErrorMessage(writeException), writeException);
        }
        String message = writerDataErrorMessage(firstCauseWithMessage(exception));
        if (exception instanceof RuntimeException runtimeException) {
            return new TrinoException(PAIMON_WRITER_DATA_ERROR, message, runtimeException);
        }
        return new TrinoException(PAIMON_WRITER_DATA_ERROR, message, exception);
    }

    private static String writerDataErrorMessage(Throwable exception)
    {
        String detail = exception.getMessage();
        if (detail == null || detail.isBlank()) {
            return "Failed to write data to Paimon";
        }
        return "Failed to write data to Paimon: " + detail;
    }

    private static Throwable firstCauseWithMessage(Throwable exception)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = exception;
        while (current != null && visited.add(current)) {
            String message = current.getMessage();
            if (message != null && !message.isBlank()) {
                return current;
            }
            current = current.getCause();
        }
        return exception;
    }

    static RuntimeException wrapWriterCloseException(Exception exception)
    {
        Throwable closeFailure = firstMatchingCause(exception, PaimonPageSink::isRecognizedWriterCloseFailure);
        if (closeFailure instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (closeFailure instanceof UnsupportedOperationException unsupportedOperationException) {
            String detail = unsupportedOperationException.getMessage();
            return new TrinoException(
                    NOT_SUPPORTED,
                    detail == null || detail.isBlank()
                            ? "Paimon writer close uses features which are not supported by the Trino connector"
                            : "Paimon writer close uses features which are not supported by the Trino connector: " + detail,
                    unsupportedOperationException);
        }
        if (exception instanceof RuntimeException runtimeException) {
            return new TrinoException(PAIMON_WRITER_CLOSE_ERROR, "Failed to close Paimon writer", runtimeException);
        }
        return new TrinoException(PAIMON_WRITER_CLOSE_ERROR, "Failed to close Paimon writer", exception);
    }

    static RuntimeException wrapIoManagerCloseException(Exception exception)
    {
        Throwable closeFailure = firstMatchingCause(exception, PaimonPageSink::isRecognizedIoManagerCloseFailure);
        if (closeFailure instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof RuntimeException runtimeException) {
            return new TrinoException(PAIMON_WRITER_CLOSE_ERROR, "Failed to close Paimon writer IO manager", runtimeException);
        }
        return new TrinoException(PAIMON_WRITER_CLOSE_ERROR, "Failed to close Paimon writer IO manager", exception);
    }

    private static Throwable firstMatchingCause(Throwable exception, Predicate<Throwable> predicate)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = exception;
        while (current != null && visited.add(current)) {
            if (predicate.test(current)) {
                return current;
            }
            current = current.getCause();
        }
        return exception;
    }

    private static boolean isRecognizedWriteFailure(Throwable exception)
    {
        return exception instanceof TrinoException ||
                exception instanceof UnsupportedOperationException ||
                isWriterDataException(exception);
    }

    private static boolean isRecognizedWriterCloseFailure(Throwable exception)
    {
        return exception instanceof TrinoException ||
                exception instanceof UnsupportedOperationException;
    }

    private static boolean isRecognizedIoManagerCloseFailure(Throwable exception)
    {
        return exception instanceof TrinoException;
    }

    private static boolean isWriterDataException(Throwable exception)
    {
        return exception instanceof IllegalArgumentException ||
                exception instanceof IllegalStateException ||
                exception instanceof NullPointerException ||
                exception instanceof IllegalFormatException;
    }

    static class DynamicBucketWriter
    {
        private final RowPartitionKeyExtractor keyExtractor;
        private final BucketAssigner bucketAssigner;

        DynamicBucketWriter(RowPartitionKeyExtractor keyExtractor, BucketAssigner bucketAssigner)
        {
            this.keyExtractor = requireNonNull(keyExtractor, "keyExtractor is null");
            this.bucketAssigner = requireNonNull(bucketAssigner, "bucketAssigner is null");
        }

        void write(BatchTableWrite writer, InternalRow row)
                throws Exception
        {
            BinaryRow partition = keyExtractor.partition(row);
            int bucket = bucketAssigner.assign(partition, keyExtractor.trimmedPrimaryKey(row).hashCode());
            writer.write(row, bucket);
        }

        void prepareCommit()
        {
            bucketAssigner.prepareCommit(BatchWriteBuilder.COMMIT_IDENTIFIER);
        }
    }

    static final class KeyDynamicWriter
    {
        private final BatchTableWrite writer;
        private final GlobalIndexAssigner assigner;

        KeyDynamicWriter(BatchTableWrite writer, GlobalIndexAssigner assigner)
        {
            this(writer, assigner, null, null);
        }

        KeyDynamicWriter(
                BatchTableWrite writer,
                GlobalIndexAssigner assigner,
                @Nullable RowPartitionKeyExtractor keyExtractor,
                @Nullable PaimonKeyDynamicBootstrap.KeyFingerprintWriter keyFingerprintWriter)
        {
            this.writer = requireNonNull(writer, "writer is null");
            this.assigner = requireNonNull(assigner, "assigner is null");
            this.keyExtractor = keyExtractor;
            this.keyFingerprintWriter = keyFingerprintWriter;
        }

        @Nullable
        private final RowPartitionKeyExtractor keyExtractor;
        @Nullable
        private final PaimonKeyDynamicBootstrap.KeyFingerprintWriter keyFingerprintWriter;

        void write(InternalRow row)
                throws Exception
        {
            if (keyExtractor != null) {
                requireNonNull(keyFingerprintWriter, "keyFingerprintWriter is null")
                        .add(keyExtractor.trimmedPrimaryKey(row));
            }
            assigner.processInput(row);
        }

        void writeAssignedRow(InternalRow row, int bucket)
        {
            try {
                writer.write(row, bucket);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to write a row assigned by Paimon KEY_DYNAMIC index", e);
            }
        }

        void close()
                throws Exception
        {
            Exception failure = null;
            try {
                assigner.close();
            }
            catch (Exception e) {
                failure = e;
            }
            if (keyFingerprintWriter != null) {
                try {
                    keyFingerprintWriter.close();
                }
                catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    }
                    else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        void abort()
                throws Exception
        {
            Exception failure = null;
            try {
                assigner.close();
            }
            catch (Exception e) {
                failure = e;
            }
            if (keyFingerprintWriter != null) {
                try {
                    keyFingerprintWriter.abort();
                }
                catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    }
                    else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static class MappedPaimonRow
            implements InternalRow
    {
        private final PaimonRow inputRow;
        private final int[] inputChannels;
        private final Object[] defaultValues;
        private final List<DataType> logicalTypes;
        private RowKind rowKind;

        MappedPaimonRow(
                Page page,
                int position,
                RowKind rowKind,
                List<Type> inputColumnTypes,
                List<DataType> inputLogicalTypes,
                int[] inputChannels,
                Object[] defaultValues,
                List<DataType> logicalTypes)
        {
            this.inputRow = PaimonRow.fromTrustedTypeLists(page, position, rowKind, inputColumnTypes, inputLogicalTypes);
            this.inputChannels = inputChannels;
            this.defaultValues = defaultValues;
            this.logicalTypes = logicalTypes;
            this.rowKind = rowKind;
        }

        @Override
        public int getFieldCount()
        {
            return inputChannels.length;
        }

        @Override
        public RowKind getRowKind()
        {
            return rowKind;
        }

        @Override
        public void setRowKind(RowKind rowKind)
        {
            this.rowKind = requireNonNull(rowKind, "rowKind is null");
            inputRow.setRowKind(rowKind);
        }

        @Override
        public boolean isNullAt(int pos)
        {
            int inputChannel = inputChannels[pos];
            if (inputChannel >= 0) {
                return inputRow.isNullAt(inputChannel);
            }
            return defaultValues[pos] == null;
        }

        @Override
        public boolean getBoolean(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getBoolean(inputChannel) : (boolean) defaultValue(pos);
        }

        @Override
        public byte getByte(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getByte(inputChannel) : (byte) defaultValue(pos);
        }

        @Override
        public short getShort(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getShort(inputChannel) : (short) defaultValue(pos);
        }

        @Override
        public int getInt(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getInt(inputChannel) : (int) defaultValue(pos);
        }

        @Override
        public long getLong(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getLong(inputChannel) : (long) defaultValue(pos);
        }

        @Override
        public float getFloat(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getFloat(inputChannel) : (float) defaultValue(pos);
        }

        @Override
        public double getDouble(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getDouble(inputChannel) : (double) defaultValue(pos);
        }

        @Override
        public BinaryString getString(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getString(inputChannel) : (BinaryString) defaultValue(pos);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getDecimal(inputChannel, precision, scale) : (Decimal) defaultValue(pos);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getTimestamp(inputChannel, precision) : (Timestamp) defaultValue(pos);
        }

        @Override
        public byte[] getBinary(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getBinary(inputChannel) :
                    PaimonRow.normalizeBinaryValue((byte[]) defaultValue(pos), logicalTypes.get(pos));
        }

        @Override
        public Variant getVariant(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getVariant(inputChannel) : (Variant) defaultValue(pos);
        }

        @Override
        public Blob getBlob(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getBlob(inputChannel) : (Blob) defaultValue(pos);
        }

        @Override
        public InternalArray getArray(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getArray(inputChannel) : (InternalArray) defaultValue(pos);
        }

        @Override
        public InternalVector getVector(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getVector(inputChannel) : (InternalVector) defaultValue(pos);
        }

        @Override
        public InternalMap getMap(int pos)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getMap(inputChannel) : (InternalMap) defaultValue(pos);
        }

        @Override
        public InternalRow getRow(int pos, int numFields)
        {
            int inputChannel = inputChannels[pos];
            return inputChannel >= 0 ? inputRow.getRow(inputChannel, numFields) : (InternalRow) defaultValue(pos);
        }

        private Object defaultValue(int pos)
        {
            Object defaultValue = defaultValues[pos];
            checkArgument(defaultValue != null, "Column %s is not present in the input page and has no default value", pos);
            return defaultValue;
        }
    }
}
