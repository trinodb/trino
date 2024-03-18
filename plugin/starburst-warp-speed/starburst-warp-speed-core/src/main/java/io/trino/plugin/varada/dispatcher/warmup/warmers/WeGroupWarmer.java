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
package io.trino.plugin.varada.dispatcher.warmup.warmers;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmUtils;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupImportService;
import io.trino.spi.connector.ConnectorSession;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.cloudvendors.model.StorageObjectMetadata;
import io.varada.log.ShapingLogger;
import io.varada.tools.util.StopWatch;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Singleton
public class WeGroupWarmer
{
    private static final Logger logger = Logger.get(WeGroupWarmer.class);
    private final ShapingLogger shapingLogger;

    public static final String WARMUP_IMPORTER_STAT_GROUP = "import-service";

    private final GlobalConfiguration globalConfiguration;
    private final CloudVendorConfiguration cloudVendorConfiguration;
    private final StorageEngineConstants storageEngineConstants;
    private final RowGroupDataService rowGroupDataService;
    private final CloudVendorService cloudVendorService;
    private final VaradaStatsWarmupImportService varadaStatsWarmupImportService;

    @Inject
    public WeGroupWarmer(
            GlobalConfiguration globalConfiguration,
            @ForWarp CloudVendorConfiguration cloudVendorConfiguration,
            StorageEngineConstants storageEngineConstants,
            RowGroupDataService rowGroupDataService,
            @ForWarp CloudVendorService cloudVendorService,
            MetricsManager metricsManager)
    {
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.cloudVendorConfiguration = requireNonNull(cloudVendorConfiguration);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.cloudVendorService = requireNonNull(cloudVendorService);
        this.varadaStatsWarmupImportService = metricsManager.registerMetric(new VaradaStatsWarmupImportService(WARMUP_IMPORTER_STAT_GROUP));
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    @VisibleForTesting
    IsNeedDownloadResults isNeedDownload(RowGroupKey rowGroupKey, String cloudPath)
    {
        RowGroupDataValidation dataValidation = new RowGroupDataValidation(0, 0);
        boolean isNeedDownload;

        try {
            // check if weGroupFile exist on cloud
            StorageObjectMetadata storageObjectMetadata = cloudVendorService.getObjectMetadata(cloudPath);
            isNeedDownload = storageObjectMetadata.getLastModified().isPresent();
            dataValidation = new RowGroupDataValidation(storageObjectMetadata);

            // check if weGroupFile already downloaded
            if (isNeedDownload) {
                RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
                if (rowGroupData != null && rowGroupData.getDataValidation().isValid()) {
                    // footer validation
                    isNeedDownload = !rowGroupData.getDataValidation().equals(dataValidation);
                    if (isNeedDownload) {
                        varadaStatsWarmupImportService.incimport_we_group_footer_validation();
                        logger.debug("isNeedDownload rowGroupKey %s cloudPath '%s' local %s cloud %s",
                                rowGroupKey, cloudPath, rowGroupData.getDataValidation(), dataValidation);
                    }
                }
            }
            logger.debug("rowGroupKey %s cloudPath '%s' => isNeedDownload %s", rowGroupKey, cloudPath, isNeedDownload);
        }
        catch (Exception e) {
            isNeedDownload = false;
            shapingLogger.error("failed to getObjectMetadata rowGroupKey %s cloudPath '%s' message: %s", rowGroupKey, cloudPath, e.getMessage());
        }
        return new IsNeedDownloadResults(isNeedDownload, dataValidation);
    }

    private boolean download(RowGroupKey rowGroupKey, String cloudPath, String localFileName)
    {
        try {
            varadaStatsWarmupImportService.incimport_we_group_download_started();

            FileUtils.createParentDirectories(new File(localFileName));
            cloudVendorService.downloadFileFromCloud(cloudPath, new File(localFileName));

            return true;
        }
        catch (Exception e) {
            varadaStatsWarmupImportService.incimport_we_group_download_failed();
            shapingLogger.error("failed to download weGroupFile rowGroupKey %s cloudPath '%s' message: %s", rowGroupKey, cloudPath, e.getMessage());
            return false;
        }
        finally {
            varadaStatsWarmupImportService.incimport_we_group_download_accomplished();
        }
    }

    private void copyFile(String sourceFileName, String targetFileName)
    {
        try {
            Files.copy(Paths.get(sourceFileName), Paths.get(targetFileName));
        }
        catch (IOException e) {
            throw new RuntimeException("Files.copy %s to %s failed".formatted(sourceFileName, targetFileName), e);
        }
    }

    private RowGroupData refreshRowGroupDataAfterImport(RowGroupKey rowGroupKey, String localTmpFileName, String localFileName, RowGroupDataValidation dataValidation)
    {
        RowGroupData rowGroupData = null;
        boolean locked = false;
        String localSaveFileName = null;

        // take writeLock
        try {
            rowGroupData = rowGroupDataService.get(rowGroupKey);

            if (rowGroupData != null) {
                rowGroupData.getLock().writeLock();
                locked = true;

                // save local file
                localSaveFileName = localFileName + ".save";
                copyFile(localFileName, localSaveFileName);

                // delete local file and invalidate cache
                rowGroupDataService.deleteData(rowGroupData, true);
            }

            // rename
            File localTmpFile = new File(localTmpFileName);
            if (!localTmpFile.renameTo(new File(localFileName))) {
                // delete localTmpFile
                //noinspection ResultOfMethodCallIgnored
                localTmpFile.delete();
                throw new RuntimeException("failed renaming file " + localTmpFileName + " => " + localFileName);
            }

            // reload
            RowGroupData newRowGroupData = rowGroupDataService.reload(rowGroupKey, rowGroupData);

            // update
            if (newRowGroupData != null) {
                List<WarmUpElement> updatedWarmUpElements = new ArrayList<>();

                for (WarmUpElement existingWarmUpElement : newRowGroupData.getWarmUpElements()) {
                    updatedWarmUpElements.add(WarmUpElement.builder(existingWarmUpElement).isImported(true).build());
                }
                rowGroupData = RowGroupData.builder(newRowGroupData)
                        .warmUpElements(updatedWarmUpElements)
                        .nextExportOffset(newRowGroupData.getNextOffset())
                        .dataValidation(dataValidation)
                        .build();
                rowGroupDataService.save(rowGroupData);

                if (localSaveFileName != null) {
                    // delete saved file
                    File localSaveFile = new File(localSaveFileName);
                    //noinspection ResultOfMethodCallIgnored
                    localSaveFile.delete();
                }
            }
            else {
                // ToDo: import_we_group_download_corrupted_file
                shapingLogger.error("failed to get RowGroupData for row group %s", rowGroupKey);
                if (localSaveFileName != null) {
                    // restore local file and cache
                    copyFile(localSaveFileName, localFileName);
                    rowGroupDataService.save(rowGroupData);
                }
            }
            return rowGroupData;
        }
        catch (InterruptedException e) {
            shapingLogger.warn(e, "failed to acquire write lock for row group %s", rowGroupKey);
            return null;
        }
        finally {
            if (rowGroupData != null && locked) {
                rowGroupData.getLock().writeUnlock();
            }
        }
    }

    public Optional<RowGroupData> importWeGroup(ConnectorSession session, RowGroupKey rowGroupKey)
    {
        String cloudImportExportPath = VaradaSessionProperties.getS3ImportExportPath(session, cloudVendorConfiguration, cloudVendorService);
        String cloudPath = WarmUtils.getCloudPath(rowGroupKey, cloudImportExportPath);
        IsNeedDownloadResults isNeedDownloadResults;
        StopWatch stopWatch = new StopWatch();

        logger.debug("importWeGroup rowGroupKey %s cloudPath '%s'", rowGroupKey, cloudPath);
        try {
            stopWatch.start();
            varadaStatsWarmupImportService.incimport_row_group_count_started();

            isNeedDownloadResults = isNeedDownload(rowGroupKey, cloudPath);
            if (!isNeedDownloadResults.isNeedDownload) {
                return Optional.empty();
            }

            String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
            String localTmpFileName = localFileName + ".tmp";
            if (!download(rowGroupKey, cloudPath, localTmpFileName)) {
                varadaStatsWarmupImportService.incimport_row_group_count_failed();
                return Optional.empty();
            }

            // at this point lastModified must be present, if it's not it's a bug
            RowGroupData rowGroupData = refreshRowGroupDataAfterImport(rowGroupKey, localTmpFileName, localFileName, isNeedDownloadResults.dataValidation);
            if (rowGroupData == null) {
                varadaStatsWarmupImportService.incimport_row_group_count_failed();
                return Optional.empty();
            }
            return Optional.of(rowGroupData);
        }
        finally {
            stopWatch.stop();
            varadaStatsWarmupImportService.addimport_row_group_total_time(stopWatch.getNanoTime());
            varadaStatsWarmupImportService.incimport_row_group_count_accomplished();
        }
    }

    private RowGroupDataValidation getRowGroupDataValidation(String cloudPath)
    {
        StorageObjectMetadata storageObjectMetadata = cloudVendorService.getObjectMetadata(cloudPath);
        return new RowGroupDataValidation(storageObjectMetadata);
    }

    private DownloadResults download(RowGroupData rowGroupData, WarmUpElement warmUpElement, String cloudPath, String localFileName)
    {
        // 1st footer validation
        RowGroupDataValidation dataValidation = getRowGroupDataValidation(cloudPath);
        boolean isValidationOk = rowGroupData.getDataValidation().equals(dataValidation);
        boolean isDownloadDone = false;

        if (isValidationOk) {
            long startOffset = (long) warmUpElement.getStartOffset() * storageEngineConstants.getPageSize();
            int totalLength = (warmUpElement.getEndOffset() - warmUpElement.getStartOffset()) * storageEngineConstants.getPageSize();

            varadaStatsWarmupImportService.incimport_elements_started();
            try (InputStream inputStream = cloudVendorService.downloadRangeFromCloud(cloudPath, startOffset, totalLength);
                    RandomAccessFile outputRandomAccessFile = new RandomAccessFile(localFileName, "rw")) {
                byte[] buffer = new byte[Math.min(totalLength, 8192 * 100)]; // PageSize * 100
                int length;

                outputRandomAccessFile.seek(startOffset);
                while ((length = inputStream.read(buffer)) > 0) {
                    outputRandomAccessFile.write(buffer, 0, length);
                }
                isDownloadDone = true;

                // 2nd footer validation
                dataValidation = getRowGroupDataValidation(cloudPath);
                isValidationOk = rowGroupData.getDataValidation().equals(dataValidation);

                if (!isValidationOk) {
                    varadaStatsWarmupImportService.incimport_elements_failed();
                    varadaStatsWarmupImportService.incimport_element_2nd_footer_validation();
                    logger.debug("download (2nd footer validation) rowGroupKey %s cloudPath '%s' local %s cloud %s",
                            rowGroupData.getRowGroupKey(), cloudPath, rowGroupData.getDataValidation(), dataValidation);
                }
            }
            catch (Exception e) {
                varadaStatsWarmupImportService.incimport_elements_failed();
                logger.error("failed to download warmUpElement rowGroupKey %s cloudPath '%s' message: %s",
                        rowGroupData.getRowGroupKey(), cloudPath, e.getMessage());
            }
            finally {
                varadaStatsWarmupImportService.incimport_elements_accomplished();
            }
        }
        else {
            varadaStatsWarmupImportService.incimport_element_1st_footer_validation();
            logger.debug("download (1st footer validation) rowGroupKey %s cloudPath '%s' local %s cloud %s",
                    rowGroupData.getRowGroupKey(), cloudPath, rowGroupData.getDataValidation(), dataValidation);
        }
        return new DownloadResults(isValidationOk, isDownloadDone);
    }

    public Optional<RowGroupData> importWarmUpElements(ConnectorSession session, RowGroupKey rowGroupKey, List<WarmUpElement> warmWarmUpElements)
    {
        String cloudImportExportPath = VaradaSessionProperties.getS3ImportExportPath(session, cloudVendorConfiguration, cloudVendorService);
        String cloudPath = WarmUtils.getCloudPath(rowGroupKey, cloudImportExportPath);
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        boolean locked = false;

        logger.debug("importWarmUpElements rowGroupKey %s cloudPath '%s' localFileName '%s' warmWarmUpElements size %d",
                rowGroupKey, cloudPath, localFileName, warmWarmUpElements.size());
        // take writeLock
        try {
            rowGroupData.getLock().writeLock();
            locked = true;

            List<WarmUpElement> warmUpElements = new ArrayList<>(rowGroupData.getWarmUpElements());
            boolean isDownloadDone = false;

            for (WarmUpElement warmWarmUpElement : warmWarmUpElements) {
                DownloadResults downloadResults = download(rowGroupData, warmWarmUpElement, cloudPath, localFileName);

                if (!downloadResults.isValidationOk) {
                    rowGroupDataService.deleteData(rowGroupData, true);
                    return Optional.empty();
                }

                if (downloadResults.isDownloadDone) {
                    WarmUpElement warmUpElement = WarmUpElement.builder(warmWarmUpElement)
                            .isImported(true)
                            .warmState(WarmState.HOT)
                            .build();

                    Optional<WarmUpElement> weToOverride = warmUpElements.stream().filter(warmWarmUpElement::isSameColNameAndWarmUpType).findFirst();
                    weToOverride.ifPresent(warmUpElements::remove);
                    warmUpElements.add(warmUpElement);
                    isDownloadDone = true;
                }
            }

            if (isDownloadDone) {
                rowGroupData = RowGroupData.builder(rowGroupData)
                        .warmUpElements(warmUpElements)
                        .build();
                rowGroupDataService.save(rowGroupData);
                rowGroupDataService.flush(rowGroupKey);
            }
        }
        catch (InterruptedException e) {
            shapingLogger.warn(e, "failed to acquire write lock for row group %s", rowGroupKey);
        }
        finally {
            if (locked) {
                rowGroupData.getLock().writeUnlock();
            }
        }
        return Optional.of(rowGroupData);
    }

    @VisibleForTesting
    record IsNeedDownloadResults(boolean isNeedDownload, RowGroupDataValidation dataValidation)
    {
    }

    private record DownloadResults(boolean isValidationOk, boolean isDownloadDone)
    {
    }
}
