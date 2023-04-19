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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.pinot.PinotPageSinkProvider.GenericRowBuffers;
import io.trino.plugin.pinot.PinotPageSinkProvider.GenericRowWithSize;
import io.trino.plugin.pinot.PinotPageSinkProvider.SegmentBuildAndPushInfo;
import io.trino.plugin.pinot.PinotPageSinkProvider.SegmentBuilderSpec;
import io.trino.plugin.pinot.PinotPageSinkProvider.SegmentProcessor;
import io.trino.plugin.pinot.PinotPageSinkProvider.SegmentProcessorResults;
import io.trino.plugin.pinot.PinotSessionProperties.InsertExistingSegmentsBehavior;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.client.PinotClient.PinotStartReplaceSegmentsResponse;
import io.trino.plugin.pinot.encoders.Encoder;
import io.trino.plugin.pinot.encoders.ProcessedSegmentMetadata;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.getDone;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.pinot.encoders.EncoderFactory.createEncoder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;

public class PinotPageSink
        implements ConnectorPageSink
{
    public static final JsonCodec<ProcessedSegmentMetadata> PROCESSED_SEGMENT_METADATA_JSON_CODEC = jsonCodec(ProcessedSegmentMetadata.class);

    private static final String DIMENSION_TABLE_SEGMENT_KEY = "dim";

    private final SegmentProcessor segmentProcessor;
    private final GenericRowBuffers genericRowBuffers;
    private final ListeningExecutorService finishExecutor;
    private final PinotInsertTableHandle pinotInsertTableHandle;
    private final String taskTempLocation;
    private final PinotClient pinotClient;
    private final long segmentBuildTimeoutMillis;
    private final long finishInsertTimeoutMillis;
    private final InsertExistingSegmentsBehavior insertExistingSegmentsBehavior;
    private final boolean forceCleanupAfterInsertEnabled;
    private final Map<String, FieldSpec> fieldSpecMap;
    private final Map<Integer, Encoder> channelToEncoderMap;
    private final Function<GenericRow, String> rowToSegmentFunction;
    private final ScheduledExecutorService timeoutExecutor;

    public PinotPageSink(
            SegmentBuilderSpec segmentBuilderSpec,
            SegmentProcessor segmentProcessor,
            PinotInsertTableHandle pinotInsertTableHandle,
            PinotClient pinotClient,
            long thresholdBytes,
            long segmentBuildTimeoutMillis,
            long finishInsertTimeoutMillis,
            InsertExistingSegmentsBehavior insertExistingSegmentsBehavior,
            boolean forceCleanupAfterInsertEnabled,
            ListeningExecutorService buildExecutor,
            ListeningExecutorService finishExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        requireNonNull(segmentBuilderSpec, "segmentBuilderSpec is null");
        this.segmentProcessor = requireNonNull(segmentProcessor, "segmentProcessor is null");
        this.pinotInsertTableHandle = segmentBuilderSpec.getPinotInsertTableHandle();
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.insertExistingSegmentsBehavior = requireNonNull(insertExistingSegmentsBehavior, "insertExistingSegmentsBehavior is null");
        this.forceCleanupAfterInsertEnabled = forceCleanupAfterInsertEnabled;
        this.taskTempLocation = segmentBuilderSpec.getTaskTempLocation();
        requireNonNull(buildExecutor, "buildExecutor is null");
        this.finishExecutor = requireNonNull(finishExecutor, "finishExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        if (pinotInsertTableHandle.getDateTimeField().isPresent()) {
            String timeColumnName = pinotInsertTableHandle.getDateTimeField().get().getColumnName();
            PinotDateTimeField dateTimeFieldTransformer = pinotInsertTableHandle.getDateTimeField().get();
            LongUnaryOperator transformFunction = dateTimeFieldTransformer.getToMillisTransform();
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            this.rowToSegmentFunction = row -> dateFormat.format(Instant.ofEpochMilli(transformFunction.applyAsLong((long) row.getValue(timeColumnName))).atZone(ZoneId.of("UTC")).toLocalDate());
        }
        else {
            this.rowToSegmentFunction = row -> DIMENSION_TABLE_SEGMENT_KEY;
        }
        this.fieldSpecMap = segmentBuilderSpec.getPinotSchema().getAllFieldSpecs().stream()
                .collect(toMap(fieldSpec -> fieldSpec.getName(), fieldSpec -> fieldSpec));
        ImmutableMap.Builder<Integer, Encoder> channelToEncoderMapBuilder = ImmutableMap.builder();
        for (int channel = 0; channel < pinotInsertTableHandle.getColumnHandles().size(); channel++) {
            PinotColumnHandle columnHandle = pinotInsertTableHandle.getColumnHandles().get(channel);
            channelToEncoderMapBuilder.put(channel, createEncoder(fieldSpecMap.get(columnHandle.getColumnName()), columnHandle.getDataType()));
        }
        this.channelToEncoderMap = channelToEncoderMapBuilder.buildOrThrow();
        this.segmentBuildTimeoutMillis = segmentBuildTimeoutMillis;
        this.finishInsertTimeoutMillis = finishInsertTimeoutMillis;
        try {
            Files.createDirectories(Paths.get(segmentBuilderSpec.getSegmentTempLocation()));
            Files.createDirectories(Paths.get(segmentBuilderSpec.getSegmentBuildLocation()));
        }
        catch (IOException e) {
            cleanup();
            throw new UncheckedIOException(e);
        }
        this.genericRowBuffers = new GenericRowBuffers(segmentBuilderSpec, segmentProcessor, buildExecutor, thresholdBytes);
    }

    @Override
    public long getCompletedBytes()
    {
        return ConnectorPageSink.super.getCompletedBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return ConnectorPageSink.super.getMemoryUsage();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return ConnectorPageSink.super.getValidationCpuNanos();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        List<GenericRowWithSize> rows = toGenericRows(page);
        Map<String, List<GenericRowWithSize>> segmentToRows = new HashMap<>();
        for (GenericRowWithSize genericRowWithSize : rows) {
            String segmentDate = rowToSegmentFunction.apply(genericRowWithSize.row());
            segmentToRows.computeIfAbsent(segmentDate, k -> new ArrayList<>()).add(genericRowWithSize);
        }
        for (Map.Entry<String, List<GenericRowWithSize>> entry : segmentToRows.entrySet()) {
            genericRowBuffers.add(entry.getKey(), entry.getValue());
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        SegmentProcessorResults segmentProcessorResults = genericRowBuffers.finish();
        // Do not start replace segments if there are no segments to replace
        // TODO: This is needed in rare cases where there were rows filtered out by the segment builder.
        // It may no longer be necessary due to fixes in newer Pinot version
        if (segmentProcessorResults.segmentBuildFutures().isEmpty()) {
            return completedFuture(ImmutableList.of());
        }

        List<String> segmentsToReplace;
        if (insertExistingSegmentsBehavior == InsertExistingSegmentsBehavior.OVERWRITE) {
            segmentsToReplace = segmentProcessorResults.segmentKeys().stream()
                    .flatMap(segmentDate -> getSegmentsToReplace(segmentDate).stream())
                    .collect(toImmutableList());
        }
        else {
            segmentsToReplace = List.of();
        }
        ListenableFuture<PinotStartReplaceSegmentsResponse> pinotStartReplaceSegmentsResponseFuture = Futures.transform(
                Futures.withTimeout(Futures.allAsList(segmentProcessorResults.segmentBuildFutures()), segmentBuildTimeoutMillis, MILLISECONDS, timeoutExecutor),
                segmentsBuildAndPushInfo -> startReplaceSegments(
                        segmentsToReplace,
                        segmentsBuildAndPushInfo.stream()
                                .map(SegmentBuildAndPushInfo::segmentName)
                                .collect(toImmutableList())),
                finishExecutor);
        SettableFuture<List<ProcessedSegmentMetadata>> processedSegmentMetadataFuture = SettableFuture.create();
        Futures.addCallback(pinotStartReplaceSegmentsResponseFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(PinotStartReplaceSegmentsResponse pinotStartReplaceSegmentsResponse)
            {
                processedSegmentMetadataFuture.setFuture(
                        Futures.withTimeout(Futures.allAsList(segmentProcessorResults.segmentBuildFutures().stream()
                                .map(segmentBuildFuture -> Futures.transform(segmentBuildFuture, segmentProcessor::finish, finishExecutor))
                                .collect(toUnmodifiableList())), finishInsertTimeoutMillis, MILLISECONDS, timeoutExecutor));
            }

            @Override
            public void onFailure(Throwable t)
            {
                processedSegmentMetadataFuture.setException(t);
            }
        }, finishExecutor);
        SettableFuture<Collection<Slice>> fragmentFuture = SettableFuture.create();
        processedSegmentMetadataFuture.addListener(this::cleanup, directExecutor());
        Futures.addCallback(processedSegmentMetadataFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(List<ProcessedSegmentMetadata> processedSegmentMetadata)
            {
                try {
                    endReplaceSegments(getDone(pinotStartReplaceSegmentsResponseFuture), processedSegmentMetadata.stream()
                            .map(ProcessedSegmentMetadata::getSegmentName)
                            .collect(toUnmodifiableList()));
                    deleteSegmentsBestEffort(segmentsToReplace);
                    fragmentFuture.set(processedSegmentMetadata.stream()
                            .map(completionMetadata -> wrappedBuffer(PROCESSED_SEGMENT_METADATA_JSON_CODEC.toJsonBytes(completionMetadata)))
                            .collect(toImmutableList()));
                }
                catch (Exception e) {
                    fragmentFuture.setException(e);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                fragmentFuture.setException(t);
            }
        }, directExecutor());

        return toCompletableFuture(fragmentFuture);
    }

    @Override
    public void abort()
    {
        genericRowBuffers.cancel();
        cleanup();
    }

    private void cleanup()
    {
        try {
            deleteRecursively(Paths.get(taskTempLocation), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<GenericRowWithSize> toGenericRows(Page page)
    {
        requireNonNull(page, "page is null");
        List<PinotColumnHandle> columnHandles = pinotInsertTableHandle.getColumnHandles();
        checkState(page.getChannelCount() == columnHandles.size(), "Unexpected channel count for page: %s", page.getChannelCount());
        if (page.getPositionCount() <= 0) {
            return ImmutableList.of();
        }
        long[] dataSize = new long[page.getBlock(0).getPositionCount()];
        GenericRow[] rows = new GenericRow[page.getBlock(0).getPositionCount()];
        for (int position = 0; position < page.getPositionCount(); position++) {
            rows[position] = new GenericRow();
        }
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            String fieldName = columnHandles.get(channel).getColumnName();
            FieldSpec fieldSpec = fieldSpecMap.get(fieldName);
            Object defaultNullValue;
            if (fieldSpec.isSingleValueField()) {
                defaultNullValue = fieldSpec.getDefaultNullValue();
            }
            else {
                defaultNullValue = new Object[] {fieldSpec.getDefaultNullValue()};
            }

            Encoder encoder = channelToEncoderMap.get(channel);
            for (int position = 0; position < block.getPositionCount(); position++) {
                Object value = encoder.encode(block, position);
                if (value == null) {
                    rows[position].putDefaultNullValue(fieldName, defaultNullValue);
                }
                else {
                    rows[position].putValue(fieldName, value);
                }
                dataSize[position] += block.getEstimatedDataSizeForStats(position);
            }
        }
        ImmutableList.Builder<GenericRowWithSize> rowWithSizeBuilder = ImmutableList.builder();
        for (int position = 0; position < rows.length; position++) {
            rowWithSizeBuilder.add(new GenericRowWithSize(rows[position], dataSize[position]));
        }
        return rowWithSizeBuilder.build();
    }

    private List<String> getSegmentsToReplace(String segmentDate)
    {
        return pinotClient.getSegments(pinotInsertTableHandle.getPinotTableName()).getOffline().stream()
                .filter(segment -> segmentDate.equals(DIMENSION_TABLE_SEGMENT_KEY) || segment.contains(segmentDate))
                .collect(toImmutableList());
    }

    private void deleteSegmentsBestEffort(List<String> segmentsToDelete)
    {
        if (segmentsToDelete.isEmpty()) {
            return;
        }
        try {
            pinotClient.deleteSegments(pinotInsertTableHandle.getPinotTableName(), segmentsToDelete);
        }
        catch (Exception e) {
            // ignored
        }
    }

    private PinotStartReplaceSegmentsResponse startReplaceSegments(List<String> segmentsToReplace, List<String> newSegments)
    {
        return pinotClient.startReplaceSegments(pinotInsertTableHandle.getPinotTableName(), segmentsToReplace, newSegments, forceCleanupAfterInsertEnabled);
    }

    private void endReplaceSegments(PinotStartReplaceSegmentsResponse response, List<String> newSegments)
    {
        try {
            pinotClient.endReplaceSegments(pinotInsertTableHandle.getPinotTableName(), response);
        }
        catch (Exception e) {
            deleteSegmentsBestEffort(newSegments);
            throw e;
        }
    }
}
