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
package io.trino.plugin.varada.dispatcher.warmup.export;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmUtils;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupExportService;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.model.StorageObjectMetadata;

import java.io.File;

import static io.trino.plugin.varada.dispatcher.warmup.export.WarmupExportingService.WARMUP_EXPORTER_STAT_GROUP;
import static java.util.Objects.requireNonNull;

public class WarmupElementsCloudExporter
{
    private static final Logger logger = Logger.get(WarmupElementsCloudExporter.class);

    private static final int MB = 1048576;

    private final GlobalConfiguration globalConfiguration;
    private final StorageEngineConstants storageEngineConstants;
    private final RowGroupDataService rowGroupDataService;
    private final CloudVendorService cloudVendorService;
    private final VaradaStatsWarmupExportService statsWarmupExportService;

    @Inject
    public WarmupElementsCloudExporter(GlobalConfiguration globalConfiguration,
            StorageEngineConstants storageEngineConstants,
            RowGroupDataService rowGroupDataService,
            @ForWarp CloudVendorService cloudVendorService,
            MetricsManager metricsManager)
    {
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.cloudVendorService = cloudVendorService;
        this.statsWarmupExportService = metricsManager.registerMetric(new VaradaStatsWarmupExportService(WARMUP_EXPORTER_STAT_GROUP));
    }

    private RowGroupDataValidation getRowGroupDataValidation(String cloudPath)
    {
        StorageObjectMetadata storageObjectMetadata = cloudVendorService.getObjectMetadata(cloudPath);
        return new RowGroupDataValidation(storageObjectMetadata);
    }

    ExportFileResults exportFile(RowGroupData rowGroupData, String cloudImportExportPath)
    {
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        String cloudPath = WarmUtils.getCloudPath(rowGroupKey, cloudImportExportPath);
        RowGroupDataValidation dataValidation = getRowGroupDataValidation(cloudPath);
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        File localFile = new File(localFileName);
        boolean isUploadDone = false;

        // no export file on cloud
        if (!dataValidation.isValid()) {
            // local file is not sparse
            if (!rowGroupData.isSparseFile()) {
                // first time
                logger.debug("exportFile new rowGroupKey %s cloudPath '%s' localFileName '%s' nextOffset %d nextExportOffset %d",
                        rowGroupKey, cloudPath, localFileName, rowGroupData.getNextOffset(), rowGroupData.getNextExportOffset());
                isUploadDone = cloudVendorService.uploadFileToCloud(cloudPath, localFile, () -> true);
            }
            // local file is sparse
            else {
                // ToDo: export_row_group_file_not_exist
                logger.error("exportFile fail (no export file on cloud, local file is sparse) rowGroupKey %s cloudPath '%s' localFileName '%s'",
                        rowGroupKey, cloudPath, localFileName);
            }
        }
        // valid export file on cloud
        else {
            // 1st footer validation
            if (rowGroupData.getDataValidation().equals(dataValidation)) {
                long contentLength = dataValidation.fileContentLength();
                long startOffset = (long) rowGroupData.getNextExportOffset() * storageEngineConstants.getPageSize();

                logger.debug("exportFile append rowGroupKey %s cloudPath '%s' localFileName '%s' contentLength %d nextOffset %d nextExportOffset %d isSparseFile %b",
                        rowGroupKey, cloudPath, localFileName, contentLength, rowGroupData.getNextOffset(), rowGroupData.getNextExportOffset(), rowGroupData.isSparseFile());
                if ((contentLength < 5 * MB) || !globalConfiguration.getEnableExportAppendOnCloud()) {
                    if (rowGroupData.isSparseFile()) {
                        // local file is sparse, so append locally
                        isUploadDone = cloudVendorService.appendOnLocal(cloudPath, localFile, startOffset,
                                // 2nd footer validation
                                () -> rowGroupData.getDataValidation().equals(getRowGroupDataValidation(cloudPath)));
                    }
                    else {
                        // local file is not sparse, so upload it
                        isUploadDone = cloudVendorService.uploadFileToCloud(cloudPath, localFile,
                                // 2nd footer validation
                                () -> rowGroupData.getDataValidation().equals(getRowGroupDataValidation(cloudPath)));
                    }
                }
                else {
                    isUploadDone = cloudVendorService.appendOnCloud(cloudPath, localFile, startOffset, rowGroupData.isSparseFile(),
                            // 2nd footer validation
                            () -> rowGroupData.getDataValidation().equals(getRowGroupDataValidation(cloudPath)));
                }
                if (!isUploadDone) {
                    statsWarmupExportService.incexport_row_group_2nd_footer_validation();
                    logger.debug("exportFile (2nd footer validation) rowGroupKey %s cloudPath '%s' local %s cloud %s",
                            rowGroupKey, cloudPath, rowGroupData.getDataValidation(), dataValidation);
                }
            }
            // local footer is not equal to cloud footer
            else {
                statsWarmupExportService.incexport_row_group_1st_footer_validation();
                logger.debug("exportFile (1st footer validation) rowGroupKey %s cloudPath '%s' local %s cloud %s",
                        rowGroupKey, cloudPath, rowGroupData.getDataValidation(), dataValidation);
                // local file is not sparse
                if (!rowGroupData.isSparseFile()) {
                    // overwrite file on cloud
                    // ToDo: export_row_group_overwrite_file
                    logger.debug("exportFile overwrite rowGroupKey %s cloudPath '%s' localFileName '%s' nextOffset %d nextExportOffset %d",
                            rowGroupKey, cloudPath, localFileName, rowGroupData.getNextOffset(), rowGroupData.getNextExportOffset());
                    isUploadDone = cloudVendorService.uploadFileToCloud(cloudPath, localFile, () -> true);
                }
                // local file is sparse
                else {
                    statsWarmupExportService.incexport_row_group_failed_due_local_sparse();
                    logger.debug("exportFile fail (export file exist on cloud, local file is sparse) rowGroupKey %s cloudPath '%s' localFileName '%s'",
                            rowGroupKey, cloudPath, localFileName);
                }
            }
        }
        if (isUploadDone) {
            return new ExportFileResults(true, getRowGroupDataValidation(cloudPath));
        }
        else {
            rowGroupDataService.deleteData(rowGroupData, true);
            return new ExportFileResults(false, new RowGroupDataValidation(0, 0));
        }
    }

    record ExportFileResults(boolean isExportDone, RowGroupDataValidation dataValidation)
    {
    }
}
