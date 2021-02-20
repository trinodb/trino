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
import com.google.common.net.HostAndPort;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.encoders.ErrorSuppressingRecordTransformerWrapper;
import io.trino.plugin.pinot.encoders.GenericRowBuffer;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.apache.pinot.core.data.recordtransformer.RecordTransformer;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.core.segment.creator.SegmentCreationDataSource;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.retry.RetryPolicies;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toMap;

public class PinotPageSink
        implements ConnectorPageSink
{
    private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();

    private final PinotInsertTableHandle pinotInsertTableHandle;
    private final List<String> pinotControllerUrls;
    private final String taskTempLocation;
    private final String segmentTempLocation;
    private final GenericRowBuffer genericRowBuffer;
    private final TableConfig tableConfig;
    private final Schema pinotSchema;
    private final String timeColumnName;

    @SuppressWarnings("unchecked")
    public PinotPageSink(PinotInsertTableHandle pinotInsertTableHandle, PinotClient pinotClient, String segmentCreationBaseDirectory, List<String> pinotControllerUrls)
    {
        this.pinotInsertTableHandle = requireNonNull(pinotInsertTableHandle, "pinotInsertTableHandle is null");
        this.pinotControllerUrls = requireNonNull(pinotControllerUrls, "pinotControllerUrls is null");
        this.taskTempLocation = String.join(File.separator, segmentCreationBaseDirectory, UUID.randomUUID().toString());
        this.segmentTempLocation = String.join(File.separator, taskTempLocation, pinotInsertTableHandle.getPinotTableName(), "segments");
        this.tableConfig = pinotClient.getTableConfig(pinotInsertTableHandle.getPinotTableName(), true);
        this.pinotSchema = pinotClient.getSchema(pinotInsertTableHandle.getPinotTableName());
        this.timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
        PinotDateTimeField dateTimeFieldTransformer = pinotInsertTableHandle.getDateTimeField().get();
        LongUnaryOperator transformFunction = dateTimeFieldTransformer.getToMillisTransform();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        List<String> sortedColumns = this.tableConfig.getIndexingConfig().getSortedColumn();
        Optional<Comparator<GenericRow>> comparator = Optional.empty();
        if (sortedColumns != null && !sortedColumns.isEmpty()) {
            String sortColumn = getOnlyElement(sortedColumns);
            String finalSortColumn = sortColumn;
            comparator = Optional.of((a, b) -> ((Comparable) a.getValue(finalSortColumn)).compareTo(b.getValue(finalSortColumn)));
        }

        Map<String, FieldSpec> fieldSpecMap = pinotSchema.getAllFieldSpecs().stream()
                .collect(toMap(fieldSpec -> fieldSpec.getName().toLowerCase(ENGLISH), fieldSpec -> fieldSpec));
        this.genericRowBuffer = new GenericRowBuffer(
                pinotInsertTableHandle.getColumnHandles().stream().map(PinotColumnHandle::getColumnName).map(fieldSpecMap::get).collect(toImmutableList()),
                pinotInsertTableHandle.getColumnHandles().stream().map(PinotColumnHandle::getDataType).collect(toImmutableList()),
                row -> dateFormat.format(Instant.ofEpochMilli(transformFunction.applyAsLong((long) row.getValue(timeColumnName))).atZone(ZoneId.of("UTC")).toLocalDate()),
                comparator);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        genericRowBuffer.append(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Map<String, GenericRowRecordReader> segments = genericRowBuffer.build();
        if (!segments.isEmpty()) {
            for (Iterator<Map.Entry<String, GenericRowRecordReader>> iterator = segments.entrySet().iterator(); iterator.hasNext(); ) {
                GenericRowRecordReader recordReader = iterator.next().getValue();
                iterator.remove();
                Path segmentPath = createSegment(recordReader);
                publishOfflineSegment(segmentPath);
            }
            cleanup();
        }
        return completedFuture(ImmutableList.of(Slices.EMPTY_SLICE));
    }

    @Override
    public void abort()
    {
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

    private synchronized Path createSegment(RecordReader recordReader)
    {
        try {
            Files.createDirectories(Paths.get(taskTempLocation));
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, pinotSchema);
            segmentGeneratorConfig.setTableName(pinotInsertTableHandle.getPinotTableName());
            segmentGeneratorConfig.setOutDir(segmentTempLocation);
            DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(pinotSchema.getDateTimeSpec(timeColumnName).getFormat());

            segmentGeneratorConfig.setSegmentNameGenerator(
                    new NormalizedDateSegmentNameGenerator(
                            pinotInsertTableHandle.getPinotTableName(),
                            null,
                            true,
                            tableConfig.getValidationConfig().getSegmentPushType(),
                            tableConfig.getValidationConfig().getSegmentPushFrequency(),
                            formatSpec));
            SegmentCreationDataSource dataSource = new RecordReaderSegmentCreationDataSource(recordReader);
            RecordTransformer recordTransformer =
                    new ErrorSuppressingRecordTransformerWrapper(
                            CompositeTransformer.getDefaultTransformer(tableConfig, pinotSchema));
            SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
            File segmentOutputDirectory = null;
            driver.init(segmentGeneratorConfig, dataSource, recordTransformer);
            driver.build();
            segmentOutputDirectory = driver.getOutputDirectory();
            File tgzPath = new File(String.join(File.separator, taskTempLocation, segmentOutputDirectory.getName() + ".tar.gz"));
            TarGzCompressionUtils.createTarGzFile(segmentOutputDirectory, tgzPath);
            return Paths.get(tgzPath.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void publishOfflineSegment(Path segmentPath)
    {
        try {
            HostAndPort controllerHostAndPort = HostAndPort.fromString(pinotControllerUrls.get(ThreadLocalRandom.current().nextInt(pinotControllerUrls.size())));
            String fileName = segmentPath.toFile().getName();
            checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
            String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
            RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(() -> {
                try (InputStream inputStream = Files.newInputStream(segmentPath)) {
                    SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT.uploadSegment(
                            FileUploadDownloadClient.getUploadSegmentHttpURI(controllerHostAndPort.getHost(), controllerHostAndPort.getPort()),
                            segmentName,
                            inputStream,
                            pinotInsertTableHandle.getPinotTableName());
                    // TODO: {"status":"Successfully uploaded segment: myTable2_2020-09-09_2020-09-09 of table: myTable2"}
                    checkState(response.getStatusCode() == 200, "Unexpected response: '%s'", response.getResponse());
                    return true;
                }
                catch (HttpErrorStatusException e) {
                    int statusCode = e.getStatusCode();
                    if (statusCode >= 500) {
                        // Temporary exception
                        //LOGGER.warn("Caught temporary exception while pushing table: {} segment: {} to {}, will retry", TOPIC_AND_TABLE, segmentName, controllerHostAndPort, e);
                        return false;
                    }
                    else {
                        // Permanent exception
                        //LOGGER.error("Caught permanent exception while pushing table: {} segment: {} to {}, won't retry", TOPIC_AND_TABLE,                                 segmentName, controllerHostAndPort, e);
                        throw e;
                    }
                }
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                Files.deleteIfExists(segmentPath);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
