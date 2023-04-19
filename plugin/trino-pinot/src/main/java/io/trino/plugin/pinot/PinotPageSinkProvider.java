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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ExecutorServiceAdapter;
import io.trino.plugin.pinot.auth.PinotControllerAuthenticationProvider;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.client.PinotClient.PinotTableConfig;
import io.trino.plugin.pinot.deepstore.DeepStore;
import io.trino.plugin.pinot.encoders.ProcessedSegmentMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.FileUploadDownloadClient.CustomHeaders;
import org.apache.pinot.common.utils.FileUploadDownloadClient.FileUploadType;
import org.apache.pinot.common.utils.FileUploadDownloadClient.QueryParameters;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.trino.plugin.pinot.PinotSessionProperties.getInsertExistingSegmentsBehavior;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.pinot.common.utils.http.HttpClient.AUTH_HTTP_HEADER;
import static org.apache.pinot.common.utils.http.HttpClient.DEFAULT_SOCKET_TIMEOUT_MS;
import static org.apache.pinot.core.segment.processing.framework.MergeType.CONCAT;
import static org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils.getFieldSpecs;
import static org.apache.pinot.spi.utils.IngestionConfigUtils.getBatchSegmentIngestionFrequency;
import static org.apache.pinot.spi.utils.IngestionConfigUtils.getBatchSegmentIngestionType;
import static org.apache.pinot.spi.utils.builder.TableNameBuilder.extractRawTableName;

public class PinotPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();

    private final String segmentCreationBaseDirectory;
    private final long segmentSizeBytes;
    private final long segmentBuildTimeoutMillis;
    private final long finishInsertTimeoutMillis;
    private final PinotClient pinotClient;
    private final boolean segmentBuildOnHeapEnabled;
    private final boolean forceCleanupAfterInsertEnabled;
    private final int perQuerySegmentBuilderParallelism;
    private final BoundedExecutor segmentBuilderExecutor;
    private final ListeningExecutorService finishExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final SegmentProcessorFactory segmentProcessorFactory;

    @Inject
    public PinotPageSinkProvider(
            PinotConfig config,
            PinotClient pinotClient,
            Optional<DeepStore> pinotDeepStore,
            PinotControllerAuthenticationProvider controllerAuthenticationProvider,
            @ForPinotInsert ExecutorService coreExecutor,
            @ForPinotInsert BoundedExecutor segmentBuilderExecutor,
            @ForPinotInsert ScheduledExecutorService timeoutExecutor)
    {
        requireNonNull(config, "config is null");
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        requireNonNull(pinotDeepStore, "pinotDeepStore is null");
        requireNonNull(controllerAuthenticationProvider, "controllerAuthenticationProvider is null");
        this.segmentCreationBaseDirectory = config.getSegmentCreationBaseDirectory();
        List<URI> pinotControllerUrls = ImmutableList.copyOf(config.getControllerUrls());
        this.segmentSizeBytes = config.getSegmentCreationDataSize().toBytes();
        this.segmentBuildTimeoutMillis = config.getSegmentBuildTimeout().toMillis();
        this.finishInsertTimeoutMillis = config.getFinishInsertTimeout().toMillis();
        this.segmentBuildOnHeapEnabled = config.isSegmentBuildOnHeapEnabled();
        this.forceCleanupAfterInsertEnabled = config.isForceCleanupAfterInsertEnabled();
        this.perQuerySegmentBuilderParallelism = config.getPerQuerySegmentBuilderParallelism();
        this.finishExecutor = listeningDecorator(requireNonNull(coreExecutor, "coreExecutor is null"));
        this.segmentBuilderExecutor = requireNonNull(segmentBuilderExecutor, "segmentBuilderExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        Ticker ticker = Ticker.systemTicker();
        if (pinotDeepStore.isPresent()) {
            this.segmentProcessorFactory = new MetadataPushSegmentProcessorFactory(
                    controllerAuthenticationProvider,
                    pinotControllerUrls,
                    ticker,
                    pinotDeepStore.get(),
                    finishExecutor,
                    config.getSegmentUploadTimeout().toMillis());
        }
        else {
            this.segmentProcessorFactory = new TarGzSegmentProcessorFactory(controllerAuthenticationProvider, pinotControllerUrls, ticker);
        }
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        String taskTempLocation = String.join(File.separator, segmentCreationBaseDirectory, UUID.randomUUID().toString());
        PinotInsertTableHandle pinotInsertTableHandle = (PinotInsertTableHandle) insertTableHandle;
        PinotTableConfig pinotTableConfig = pinotClient.getTableConfig(pinotInsertTableHandle.getPinotTableName());
        checkState(pinotTableConfig.getOfflineConfig().isPresent(), "Offline config for table %s not found", pinotInsertTableHandle.getPinotTableName());
        TableConfig tableConfig = pinotTableConfig.getOfflineConfig().get();
        Schema pinotSchema = pinotClient.getTableSchema(pinotInsertTableHandle.getPinotTableName());
        SegmentBuilderSpec segmentBuilderSpec = new SegmentBuilderSpec(pinotInsertTableHandle, tableConfig, pinotSchema, segmentBuildOnHeapEnabled, taskTempLocation, Instant.now().toEpochMilli());

        return new PinotPageSink(
                segmentBuilderSpec,
                segmentProcessorFactory.create(segmentBuilderSpec),
                pinotInsertTableHandle,
                pinotClient,
                segmentSizeBytes,
                segmentBuildTimeoutMillis,
                finishInsertTimeoutMillis,
                getInsertExistingSegmentsBehavior(session),
                forceCleanupAfterInsertEnabled,
                listeningDecorator(ExecutorServiceAdapter.from(new BoundedExecutor(segmentBuilderExecutor, perQuerySegmentBuilderParallelism))),
                finishExecutor,
                timeoutExecutor);
    }

    public static class TimedBlockingQueue<T>
            extends ArrayBlockingQueue<T>
    {
        private final long timeoutMillis;

        public TimedBlockingQueue(int maxSize, long timeoutMillis)
        {
            super(maxSize);
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public boolean offer(T element)
        {
            try {
                if (!offer(element, timeoutMillis, MILLISECONDS)) {
                    throw new UncheckedTimeoutException();
                }
                return true;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    public static class GenericRowBuffers
    {
        @GuardedBy("this")
        private final Map<String, GenericRowBuffer> genericRowBuffers = new HashMap<>();
        private final SegmentBuilderSpec segmentBuilderSpec;
        private final SegmentTasksTracker segmentTasksTracker;
        private final long thresholdBytes;

        public GenericRowBuffers(
                SegmentBuilderSpec segmentBuilderSpec,
                SegmentProcessor segmentProcessor,
                ListeningExecutorService buildExecutor,
                long thresholdBytes)
        {
            this.segmentBuilderSpec = requireNonNull(segmentBuilderSpec, "segmentBuilderSpec is null");
            requireNonNull(segmentProcessor, "segmentProcessor is null");
            requireNonNull(buildExecutor, "buildExecutor is null");
            this.segmentTasksTracker = new SegmentTasksTracker(segmentProcessor, buildExecutor);
            this.thresholdBytes = thresholdBytes;
        }

        private GenericRowBuffer create()
        {
            return new GenericRowBuffer(segmentBuilderSpec, segmentTasksTracker, thresholdBytes);
        }

        public synchronized void add(String segmentKey, List<GenericRowWithSize> genericRowWithSizes)
        {
            genericRowBuffers.computeIfAbsent(segmentKey, ignored -> create()).add(genericRowWithSizes);
        }

        public synchronized SegmentProcessorResults finish()
        {
            genericRowBuffers.values().forEach(GenericRowBuffer::finish);
            return new SegmentProcessorResults(genericRowBuffers.keySet(), segmentTasksTracker.getSegmentBuildFutures());
        }

        public synchronized void cancel()
        {
            segmentTasksTracker.cancel();
        }
    }

    public record SegmentProcessorResults(Collection<String> segmentKeys, List<ListenableFuture<SegmentBuildAndPushInfo>> segmentBuildFutures)
    {
        public SegmentProcessorResults
        {
            segmentKeys = List.copyOf(requireNonNull(segmentKeys, "segmentKeys is null"));
            segmentBuildFutures = List.copyOf(requireNonNull(segmentBuildFutures, "segmentBuildFutures is null"));
        }
    }

    private static class SegmentTasksTracker
    {
        private final SegmentProcessor segmentProcessor;
        private final ListeningExecutorService buildExecutor;

        @GuardedBy("this")
        private final List<ListenableFuture<SegmentBuildAndPushInfo>> segmentBuildFutures = new ArrayList<>();

        @GuardedBy("this")
        private SegmentsTaskTrackerState state = SegmentsTaskTrackerState.ACCEPTING;

        public SegmentTasksTracker(SegmentProcessor segmentProcessor, ListeningExecutorService buildExecutor)
        {
            this.segmentProcessor = requireNonNull(segmentProcessor, "segmentProcessor is null");
            this.buildExecutor = requireNonNull(buildExecutor, "buildExecutor is null");
        }

        public synchronized void cancel()
        {
            state = SegmentsTaskTrackerState.CANCELLED;
            segmentBuildFutures.forEach(PinotPageSinkProvider::cancelFutureIgnoringExceptions);
        }

        public synchronized void process(int sequenceId, RecordReader recordReader)
        {
            if (state != SegmentsTaskTrackerState.ACCEPTING) {
                throw new PinotException(PINOT_EXCEPTION, Optional.empty(), "Not accepting new segments");
            }
            segmentBuildFutures.add(buildExecutor.submit(() -> segmentProcessor.process(sequenceId, recordReader)));
        }

        public synchronized List<ListenableFuture<SegmentBuildAndPushInfo>> getSegmentBuildFutures()
        {
            checkState(state == SegmentsTaskTrackerState.ACCEPTING, "Unexpected state %s", state);
            state = SegmentsTaskTrackerState.FINISHING;
            return List.copyOf(segmentBuildFutures);
        }
    }

    private enum SegmentsTaskTrackerState
    {
        ACCEPTING,
        FINISHING,
        CANCELLED
    }

    public static void cancelFutureIgnoringExceptions(Future<?> future)
    {
        try {
            future.cancel(true);
        }
        catch (Exception e) {
            // ignored
        }
    }

    public interface SegmentProcessor
    {
        SegmentBuildAndPushInfo process(int sequenceId, RecordReader recordReader);

        ProcessedSegmentMetadata finish(SegmentBuildAndPushInfo segmentBuildAndPushInfo);
    }

    public interface SegmentProcessorFactory
    {
        SegmentProcessor create(SegmentBuilderSpec segmentBuilderSpec);
    }

    public abstract static class AbstractSegmentProcessor
            implements SegmentProcessor
    {
        protected final SegmentBuilderSpec segmentBuilderSpec;
        protected final PinotControllerAuthenticationProvider controllerAuthenticationProvider;
        private final List<URI> pinotControllerUrls;
        protected final Ticker ticker;

        public AbstractSegmentProcessor(
                SegmentBuilderSpec segmentBuilderSpec,
                PinotControllerAuthenticationProvider controllerAuthenticationProvider,
                List<URI> pinotControllerUrls,
                Ticker ticker)
        {
            this.segmentBuilderSpec = requireNonNull(segmentBuilderSpec, "segmentBuilderSpec is null");
            this.controllerAuthenticationProvider = requireNonNull(controllerAuthenticationProvider, "controllerAuthenticationProvider is null");
            this.pinotControllerUrls = List.copyOf(pinotControllerUrls);
            this.ticker = requireNonNull(ticker, "ticker is null");
        }

        protected URI getControllerUrl()
        {
            return pinotControllerUrls.get(ThreadLocalRandom.current().nextInt(pinotControllerUrls.size()));
        }

        protected SegmentBuildAndPushInfo createSegment(int sequenceId, RecordReader recordReader)
        {
            try {
                long beginNanos = ticker.read();
                SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(segmentBuilderSpec.getTableConfig(), segmentBuilderSpec.getPinotSchema());
                segmentGeneratorConfig.setTableName(segmentBuilderSpec.getRawTableName());
                segmentGeneratorConfig.setOutDir(segmentBuilderSpec.getSegmentTempLocation());
                segmentGeneratorConfig.setOnHeap(segmentBuilderSpec.isSegmentBuildOnHeapEnabled());
                if (segmentBuilderSpec.getTimeColumnName().isPresent()) {
                    DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(segmentBuilderSpec.getPinotSchema().getDateTimeSpec(segmentBuilderSpec.getTimeColumnName().get()).getFormat());
                    segmentGeneratorConfig.setSegmentNameGenerator(new NormalizedDateSegmentNameGenerator(
                            segmentBuilderSpec.getRawTableName(),
                            format("%s-%s", segmentBuilderSpec.getRawTableName(), segmentBuilderSpec.getCreatedAtEpochMillis()),
                            false,
                            segmentBuilderSpec.getSegmentPushType(),
                            segmentBuilderSpec.getSegmentPushFrequency(),
                            formatSpec,
                            null));
                }
                else {
                    // If time column is null, this is either a dimension table or unpartitioned offline table
                    // This means that the segment name will not have a date component which implies REFRESH push type
                    segmentGeneratorConfig.setSegmentNameGenerator(new NormalizedDateSegmentNameGenerator(
                            segmentBuilderSpec.getRawTableName(),
                            format("%s-%s", segmentBuilderSpec.getRawTableName(), segmentBuilderSpec.getCreatedAtEpochMillis()),
                            false,
                            "REFRESH",
                            null,
                            null,
                            null));
                }
                segmentGeneratorConfig.setSequenceId(sequenceId);
                SegmentCreationDataSource dataSource = new RecordReaderSegmentCreationDataSource(recordReader);
                RecordTransformer recordTransformer = CompositeTransformer.getPassThroughTransformer();
                SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
                driver.init(segmentGeneratorConfig, dataSource, recordTransformer, ComplexTypeTransformer.getComplexTypeTransformer(segmentBuilderSpec.getTableConfig()));
                driver.build();
                File segmentOutputDirectory = driver.getOutputDirectory();
                File tgzPath = new File(String.join(File.separator, segmentBuilderSpec.getTaskTempLocation(), segmentOutputDirectory.getName() + ".tar.gz"));
                TarGzCompressionUtils.createTarGzFile(segmentOutputDirectory, tgzPath);
                long buildTimeNanos = ticker.read() - beginNanos;
                long segmentSize = getTotalSize(segmentOutputDirectory);
                return new SegmentBuildAndPushInfo(driver.getSegmentName(), tgzPath.toPath(), segmentOutputDirectory.toPath(), driver.getSegmentStats().getTotalDocCount(), segmentSize, tgzPath.length(), NANOSECONDS.toMillis(buildTimeNanos), Optional.empty());
            }
            catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        private long getTotalSize(File path)
        {
            if (path.isFile()) {
                return path.length();
            }
            long totalSize = 0;
            for (File file : path.listFiles()) {
                if (file.isFile()) {
                    totalSize += file.length();
                }
                else if (file.isDirectory()) {
                    totalSize += getTotalSize(file);
                }
                else {
                    throw new UnsupportedOperationException(format("Unsupported file type for file '%s'", file.getPath()));
                }
            }
            return totalSize;
        }
    }

    public static class TarGzSegmentProcessorFactory
            implements SegmentProcessorFactory
    {
        private final PinotControllerAuthenticationProvider controllerAuthenticationProvider;
        private final List<URI> pinotControllerUrls;
        private final Ticker ticker;

        public TarGzSegmentProcessorFactory(
                PinotControllerAuthenticationProvider controllerAuthenticationProvider,
                List<URI> pinotControllerUrls,
                Ticker ticker)
        {
            this.controllerAuthenticationProvider = requireNonNull(controllerAuthenticationProvider, "controllerAuthenticationProvider is null");
            this.pinotControllerUrls = List.copyOf(pinotControllerUrls);
            this.ticker = requireNonNull(ticker, "ticker is null");
        }

        @Override
        public SegmentProcessor create(SegmentBuilderSpec segmentBuilderSpec)
        {
            return new TarGzSegmentProcessor(segmentBuilderSpec, controllerAuthenticationProvider, pinotControllerUrls, ticker);
        }
    }

    public static class TarGzSegmentProcessor
            extends AbstractSegmentProcessor
    {
        public TarGzSegmentProcessor(
                SegmentBuilderSpec segmentBuilderSpec,
                PinotControllerAuthenticationProvider controllerAuthenticationProvider,
                List<URI> pinotControllerUrls,
                Ticker ticker)
        {
            super(segmentBuilderSpec, controllerAuthenticationProvider, pinotControllerUrls, ticker);
        }

        @Override
        public SegmentBuildAndPushInfo process(int sequenceId, RecordReader recordReader)
        {
            return createSegment(sequenceId, recordReader);
        }

        @Override
        public ProcessedSegmentMetadata finish(SegmentBuildAndPushInfo segmentBuildAndPushInfo)
        {
            try {
                long beginNanos = ticker.read();
                publishOfflineSegment(segmentBuildAndPushInfo.segmentTgzPath());
                long segmentPushTimeNanos = ticker.read() - beginNanos;
                return new ProcessedSegmentMetadata(segmentBuildAndPushInfo.segmentName(),
                        false,
                        segmentBuildAndPushInfo.segmentBuildTimeMillis(),
                        NANOSECONDS.toMillis(segmentPushTimeNanos),
                        0,
                        segmentBuildAndPushInfo.segmentSizeBytes(),
                        segmentBuildAndPushInfo.compressedSegmentSizeBytes(),
                        segmentBuildAndPushInfo.segmentRows());
            }
            catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        // Upload an offline segment directly to the controller
        private synchronized void publishOfflineSegment(Path segmentPath)
        {
            try {
                String fileName = segmentPath.toFile().getName();
                checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
                String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
                NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, segmentBuilderSpec.getPinotInsertTableHandle().getPinotTableName());
                List<NameValuePair> parameters = Arrays.asList(tableNameValuePair);
                List<Header> headers = new ArrayList<>();
                controllerAuthenticationProvider.getAuthenticationToken().ifPresent(token -> headers.add(new BasicHeader(AUTH_HTTP_HEADER, token)));
                RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(() -> {
                    try (InputStream inputStream = Files.newInputStream(segmentPath)) {
                        SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT.uploadSegment(
                                getControllerUrl().resolve("/v2/segments"),
                                segmentName,
                                inputStream,
                                headers,
                                parameters,
                                DEFAULT_SOCKET_TIMEOUT_MS);
                        checkState(response.getStatusCode() == 200, "Unexpected response: '%s'", response.getResponse());
                        return true;
                    }
                    catch (HttpErrorStatusException e) {
                        int statusCode = e.getStatusCode();
                        if (statusCode >= 500) {
                            return false;
                        }
                        else {
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

    public static class MetadataPushSegmentProcessorFactory
            implements SegmentProcessorFactory
    {
        private final PinotControllerAuthenticationProvider controllerAuthenticationProvider;
        private final List<URI> pinotControllerUrls;
        private final Ticker ticker;
        private final URI segmentDeepstoreURI;
        private final Supplier<PinotFS> pinotFSSupplier;
        private final ExecutorService executorService;
        private final long segmentUploadTimeoutMillis;

        public MetadataPushSegmentProcessorFactory(
                PinotControllerAuthenticationProvider controllerAuthenticationProvider,
                List<URI> pinotControllerUrls,
                Ticker ticker,
                DeepStore pinotDeepStore,
                ExecutorService executorService,
                long segmentUploadTimeoutMillis)
        {
            this.controllerAuthenticationProvider = requireNonNull(controllerAuthenticationProvider, "controllerAuthenticationProvider is null");
            this.pinotControllerUrls = List.copyOf(pinotControllerUrls);
            this.ticker = requireNonNull(ticker, "ticker is null");
            requireNonNull(pinotDeepStore, "pinotDeepStore is null");
            this.pinotFSSupplier = memoize(() -> {
                PinotFSFactory.init(pinotDeepStore.getPinotConfiguration());
                return PinotFSFactory.create(pinotDeepStore.getDeepStoreUri().getScheme());
            });
            this.segmentDeepstoreURI = pinotDeepStore.getDeepStoreUri();
            this.executorService = requireNonNull(executorService, "executorService is null");
            this.segmentUploadTimeoutMillis = segmentUploadTimeoutMillis;
        }

        @Override
        public SegmentProcessor create(SegmentBuilderSpec segmentBuilderSpec)
        {
            return new MetadataPushSegmentProcessor(segmentBuilderSpec, controllerAuthenticationProvider, pinotControllerUrls, ticker, segmentDeepstoreURI, pinotFSSupplier.get(), executorService, segmentUploadTimeoutMillis);
        }
    }

    public static class MetadataPushSegmentProcessor
            extends AbstractSegmentProcessor
    {
        private final URI segmentDeepstoreURI;
        private final PinotFS pinotFS;
        private final ExecutorService executorService;
        private final long segmentUploadTimeoutMillis;

        public MetadataPushSegmentProcessor(
                SegmentBuilderSpec segmentBuilderSpec,
                PinotControllerAuthenticationProvider controllerAuthenticationProvider,
                List<URI> pinotControllerUrls,
                Ticker ticker,
                URI segmentDeepstoreURI,
                PinotFS pinotFS,
                ExecutorService executorService,
                long segmentUploadTimeoutMillis)
        {
            super(segmentBuilderSpec, controllerAuthenticationProvider, pinotControllerUrls, ticker);
            this.segmentDeepstoreURI = requireNonNull(segmentDeepstoreURI, "segmentDeepstoreURI is null");
            this.pinotFS = requireNonNull(pinotFS, "pinotFS is null");
            this.executorService = requireNonNull(executorService, "executorService is null");
            this.segmentUploadTimeoutMillis = segmentUploadTimeoutMillis;
        }

        @Override
        public SegmentBuildAndPushInfo process(int sequenceId, RecordReader recordReader)
        {
            return pushSegmentAndExtractMetadata(createSegment(sequenceId, recordReader));
        }

        @Override
        public ProcessedSegmentMetadata finish(SegmentBuildAndPushInfo segmentBuildAndPushInfo)
        {
            try {
                long beginNanos = ticker.read();
                List<Header> headers = new ArrayList<>();
                headers.add(new BasicHeader(CustomHeaders.DOWNLOAD_URI, segmentBuildAndPushInfo.metadataPushInfo().get().segmentURI().toString()));
                headers.add(new BasicHeader(CustomHeaders.UPLOAD_TYPE,
                        FileUploadType.METADATA.toString()));
                controllerAuthenticationProvider.getAuthenticationToken().ifPresent(token -> headers.add(new BasicHeader(AUTH_HTTP_HEADER, token)));

                RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(
                        () -> {
                            try {
                                SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT
                                        .uploadSegmentMetadata(FileUploadDownloadClient.getUploadSegmentURI(getControllerUrl()), segmentBuildAndPushInfo.segmentName(),
                                                segmentBuildAndPushInfo.metadataPushInfo().get().segmentMetadataFile(), headers, FileUploadDownloadClient.makeTableParam(segmentBuilderSpec.getRawTableName()),
                                                DEFAULT_SOCKET_TIMEOUT_MS);
                                checkState(response.getStatusCode() == 200, "Unexpected response: '%s'", response.getResponse());
                                return true;
                            }
                            catch (Exception e) {
                                return false;
                            }
                        });
                long metadataPushTimeMillis = ticker.read() - beginNanos;
                return new ProcessedSegmentMetadata(segmentBuildAndPushInfo.segmentName(),
                        true,
                        segmentBuildAndPushInfo.segmentBuildTimeMillis(),
                        segmentBuildAndPushInfo.metadataPushInfo().get().segmentPushTimeMillis(),
                        NANOSECONDS.toMillis(metadataPushTimeMillis),
                        segmentBuildAndPushInfo.segmentSizeBytes(),
                        segmentBuildAndPushInfo.compressedSegmentSizeBytes(),
                        segmentBuildAndPushInfo.segmentRows());
            }
            catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        private SegmentBuildAndPushInfo pushSegmentAndExtractMetadata(SegmentBuildAndPushInfo segmentBuildAndPushInfo)
        {
            URI segmentURI = segmentDeepstoreURI.resolve(segmentDeepstoreURI.getPath() + File.separator + segmentBuilderSpec.getRawTableName() + File.separator + segmentBuildAndPushInfo.segmentName()).normalize();
            try {
                long beginNanos = ticker.read();
                RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(
                        () ->
                                executorService.submit(() -> {
                                    try {
                                        // This may hang if there are issues with the underlying blob store
                                        pinotFS.copyFromLocalFile(segmentBuildAndPushInfo.segmentTgzPath.toFile(), segmentURI);
                                        return true;
                                    }
                                    catch (Exception e) {
                                        throwIfUnchecked(e);
                                        throw new RuntimeException(e);
                                    }
                                }).get(segmentUploadTimeoutMillis, MILLISECONDS));
                long pushTimeMillis = ticker.read() - beginNanos;
                return segmentBuildAndPushInfo.withMetadataPushInfo(new SegmentMetadataPushInfo(segmentURI, generateSegmentMetadataFileFromDirectory(segmentBuildAndPushInfo.segmentOutputDirectory()), NANOSECONDS.toMillis(pushTimeMillis)));
            }
            catch (AttemptsExceededException | RetriableOperationException e) {
                throw new RuntimeException(e);
            }
        }

        private File generateSegmentMetadataFileFromDirectory(Path segmentDirectory)
        {
            File segmentMetadataDir = new File(segmentBuilderSpec.getTaskTempLocation(), "segmentMetadataDir-" + randomUUID());
            Path segmentSubdirectory = segmentDirectory.resolve(SegmentVersion.v3.name());
            try {
                Files.createDirectories(segmentMetadataDir.toPath());
                Files.move(segmentSubdirectory.resolve(MetadataKeys.METADATA_FILE_NAME), segmentMetadataDir.toPath().resolve(MetadataKeys.METADATA_FILE_NAME));
                Files.move(segmentSubdirectory.resolve(V1Constants.SEGMENT_CREATION_META), segmentMetadataDir.toPath().resolve(V1Constants.SEGMENT_CREATION_META));

                File segmentMetadataTarFile = new File(segmentBuilderSpec.getTaskTempLocation(),
                        "segmentMetadata-" + randomUUID() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
                TarGzCompressionUtils.createTarGzFile(segmentMetadataDir, segmentMetadataTarFile);

                return segmentMetadataTarFile;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                try {
                    deleteRecursively(segmentMetadataDir.toPath(), ALLOW_INSECURE);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    public static class SegmentBuilderSpec
    {
        private final PinotInsertTableHandle pinotInsertTableHandle;
        private final TableConfig tableConfig;
        private final Schema pinotSchema;
        private final boolean segmentBuildOnHeapEnabled;
        private final String taskTempLocation;
        private final long createdAtEpochMillis;
        private final String rawTableName;
        private final Optional<String> timeColumnName;
        private final String segmentPushType;
        private final String segmentPushFrequency;
        private final String segmentBuildLocation;
        private final String segmentTempLocation;

        public SegmentBuilderSpec(
                PinotInsertTableHandle pinotInsertTableHandle,
                TableConfig tableConfig,
                Schema pinotSchema,
                boolean segmentBuildOnHeapEnabled,
                String taskTempLocation,
                long createdAtEpochMillis)
        {
            this.pinotInsertTableHandle = requireNonNull(pinotInsertTableHandle, "pinotInsertTableHandle is null");
            this.tableConfig = requireNonNull(tableConfig, "tableConfig is null");
            this.pinotSchema = requireNonNull(pinotSchema, "pinotSchema is null");
            this.segmentBuildOnHeapEnabled = segmentBuildOnHeapEnabled;
            this.taskTempLocation = requireNonNull(taskTempLocation, "taskTempLocation is null");
            this.createdAtEpochMillis = createdAtEpochMillis;
            this.rawTableName = extractRawTableName(tableConfig.getTableName());
            this.timeColumnName = pinotInsertTableHandle.getDateTimeField().map(PinotDateTimeField::getColumnName);
            this.segmentPushType = getBatchSegmentIngestionType(tableConfig);
            this.segmentPushFrequency = getBatchSegmentIngestionFrequency(tableConfig);
            this.segmentBuildLocation = String.join(File.separator, taskTempLocation, rawTableName, "segmentBuilder");
            this.segmentTempLocation = String.join(File.separator, taskTempLocation, rawTableName, "segments");
        }

        public PinotInsertTableHandle getPinotInsertTableHandle()
        {
            return pinotInsertTableHandle;
        }

        public TableConfig getTableConfig()
        {
            return tableConfig;
        }

        public Schema getPinotSchema()
        {
            return pinotSchema;
        }

        public boolean isSegmentBuildOnHeapEnabled()
        {
            return segmentBuildOnHeapEnabled;
        }

        public String getTaskTempLocation()
        {
            return taskTempLocation;
        }

        public long getCreatedAtEpochMillis()
        {
            return createdAtEpochMillis;
        }

        public String getRawTableName()
        {
            return rawTableName;
        }

        public Optional<String> getTimeColumnName()
        {
            return timeColumnName;
        }

        public String getSegmentPushType()
        {
            return segmentPushType;
        }

        public String getSegmentPushFrequency()
        {
            return segmentPushFrequency;
        }

        public String getSegmentBuildLocation()
        {
            return segmentBuildLocation;
        }

        public String getSegmentTempLocation()
        {
            return segmentTempLocation;
        }
    }

    public record SegmentMetadataPushInfo(URI segmentURI, File segmentMetadataFile, long segmentPushTimeMillis)
    {
        public SegmentMetadataPushInfo
        {
            requireNonNull(segmentURI, "segmentURI is null");
            requireNonNull(segmentMetadataFile, "segmentMetadataFile is null");
        }
    }

    public record SegmentBuildAndPushInfo(
            String segmentName,
            Path segmentTgzPath,
            Path segmentOutputDirectory,
            long segmentRows,
            long segmentSizeBytes,
            long compressedSegmentSizeBytes,
            long segmentBuildTimeMillis,
            Optional<SegmentMetadataPushInfo> metadataPushInfo)
    {
        public SegmentBuildAndPushInfo
        {
            requireNonNull(segmentName, "segmentName is null");
            requireNonNull(segmentTgzPath, "segmentTgzPath is null");
            requireNonNull(segmentOutputDirectory, "segmentOutputDirectory is null");
            requireNonNull(metadataPushInfo, "metadataPushInfo is null");
        }

        public SegmentBuildAndPushInfo withMetadataPushInfo(SegmentMetadataPushInfo segmentMetadataPushInfo)
        {
            requireNonNull(segmentMetadataPushInfo, "segmentMetadataPushInfo is null");
            return new SegmentBuildAndPushInfo(
                    segmentName,
                    segmentTgzPath,
                    segmentOutputDirectory,
                    segmentRows,
                    segmentSizeBytes,
                    compressedSegmentSizeBytes,
                    segmentBuildTimeMillis,
                    Optional.of(segmentMetadataPushInfo));
        }
    }

    public static class GenericRowFileReaderBuilder
    {
        private final List<FieldSpec> fieldSpecs;
        private final int sortedColumnCount;
        private final GenericRowFileManager fileManager;
        private final Path outputDirectory;
        private final TableConfig tableConfig;

        public GenericRowFileReaderBuilder(TableConfig tableConfig, Schema pinotSchema, String segmentBuildLocation)
        {
            this.tableConfig = requireNonNull(tableConfig, "tableConfig is null");
            requireNonNull(pinotSchema, "pinotSchema is null");
            Pair<List<FieldSpec>, Integer> fieldSpecsAndSortColumns = getFieldSpecs(pinotSchema, CONCAT, getSortedColumns());
            this.fieldSpecs = fieldSpecsAndSortColumns.getLeft();
            this.sortedColumnCount = fieldSpecsAndSortColumns.getRight();
            try {
                this.outputDirectory = Paths.get(segmentBuildLocation).resolve(randomUUID().toString());
                Files.createDirectories(outputDirectory);
                this.fileManager = new GenericRowFileManager(outputDirectory.toFile(), fieldSpecs, tableConfig.getIndexingConfig().isNullHandlingEnabled(), sortedColumnCount);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private List<String> getSortedColumns()
        {
            if (tableConfig.getIndexingConfig() == null || tableConfig.getIndexingConfig().getSortedColumn() == null) {
                return ImmutableList.of();
            }
            return tableConfig.getIndexingConfig().getSortedColumn();
        }

        public void add(GenericRow row)
        {
            try {
                fileManager.getFileWriter().write(row);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public GenericRowFileReader getRecordFileReader()
        {
            try {
                fileManager.closeFileWriter();
                return fileManager.getFileReader();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class GenericRowBuffer
    {
        private final SegmentBuilderSpec segmentBuilderSpec;
        private final SegmentTasksTracker segmentTasksTracker;
        private final long thresholdBytes;

        @GuardedBy("this")
        private int sequenceId;

        @GuardedBy("this")
        private long currentSize;

        // Necessary because currentGenericRowFileReaderBuilder.getRecordFileReader() will throw an exception if the rowcount is 0
        @GuardedBy("this")
        private int currentRowCount;

        @GuardedBy("this")
        private int bufferedRowCount;

        @GuardedBy("this")
        private GenericRowFileReaderBuilder currentGenericRowFileReaderBuilder;

        @GuardedBy("this")
        private GenericRowFileReaderBuilder bufferedGenericRowFileReaderBuilder;

        @GuardedBy("this")
        private boolean finished;

        public GenericRowBuffer(SegmentBuilderSpec segmentBuilderSpec, SegmentTasksTracker segmentTasksTracker, long thresholdBytes)
        {
            this.segmentBuilderSpec = requireNonNull(segmentBuilderSpec, "segmentBuilderSpec is null");
            this.segmentTasksTracker = requireNonNull(segmentTasksTracker, "segmentTasksTracker is null");
            this.thresholdBytes = thresholdBytes;
            this.currentGenericRowFileReaderBuilder = createGenericRowFileReaderBuilder();
        }

        private GenericRowFileReaderBuilder createGenericRowFileReaderBuilder()
        {
            return new GenericRowFileReaderBuilder(segmentBuilderSpec.getTableConfig(), segmentBuilderSpec.getPinotSchema(), segmentBuilderSpec.getSegmentBuildLocation());
        }

        public synchronized void add(List<GenericRowWithSize> rowsWithSizes)
        {
            checkState(!finished, "not accepting records");
            for (GenericRowWithSize rowWithSize : rowsWithSizes) {
                currentGenericRowFileReaderBuilder.add(rowWithSize.row());
                currentSize += rowWithSize.dataSize();
                currentRowCount++;
                if (currentSize >= thresholdBytes) {
                    if (bufferedRowCount > 0) {
                        flush(bufferedGenericRowFileReaderBuilder.getRecordFileReader().getRecordReader());
                    }
                    bufferedGenericRowFileReaderBuilder = currentGenericRowFileReaderBuilder;
                    bufferedRowCount = currentRowCount;
                    currentGenericRowFileReaderBuilder = createGenericRowFileReaderBuilder();
                    currentSize = 0;
                    currentRowCount = 0;
                }
            }
        }

        private synchronized void flush(RecordReader recordReader)
        {
            segmentTasksTracker.process(++sequenceId, recordReader);
        }

        public synchronized void finish()
        {
            checkState(!finished, "already finished");
            finished = true;
            try {
                if (bufferedRowCount == 0) {
                    if (currentRowCount > 0) {
                        flush(currentGenericRowFileReaderBuilder.getRecordFileReader().getRecordReader());
                    }
                }
                else if (currentRowCount == 0) {
                    flush(bufferedGenericRowFileReaderBuilder.getRecordFileReader().getRecordReader());
                }
                else {
                    // Combine the buffered record reader with the current record reader and create 2 segments with similar size
                    // Segment skew causes memory bloat and other performance issues in Pinot
                    // Best effort using rowcount since size per row is no longer available
                    // Add current records to buffered record builder so the records are sorted
                    RecordReader currentRowsReader = currentGenericRowFileReaderBuilder.getRecordFileReader().getRecordReader();
                    while (currentRowsReader.hasNext()) {
                        bufferedGenericRowFileReaderBuilder.add(currentRowsReader.next());
                    }
                    GenericRowFileReader bufferedRecordReader = bufferedGenericRowFileReaderBuilder.getRecordFileReader();
                    int totalRows = bufferedRecordReader.getNumRows();
                    // There are atleast 2 rows
                    int mid = totalRows / 2;
                    // These will be cleaned up when the task finishes
                    // Calling asynchronously can result in the file being deleted by one task while read by another
                    flush(bufferedRecordReader.getRecordReader().getRecordReaderForRange(0, mid));
                    flush(bufferedRecordReader.getRecordReader().getRecordReaderForRange(mid, totalRows));
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                currentSize = 0;
                currentRowCount = 0;
                bufferedRowCount = 0;
                bufferedGenericRowFileReaderBuilder = null;
                currentGenericRowFileReaderBuilder = null;
            }
        }
    }

    public record GenericRowWithSize(GenericRow row, long dataSize)
    {
        public GenericRowWithSize
        {
            requireNonNull(row, "row is null");
        }
    }
}
