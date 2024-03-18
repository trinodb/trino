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

import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupExportService;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.model.StorageObjectMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;

import static io.trino.plugin.varada.dispatcher.warmup.export.WarmupExportingService.WARMUP_EXPORTER_STAT_GROUP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WarmupElementsCloudExporterTest
{
    private CloudVendorService cloudVendorService;
    private VaradaStatsWarmupExportService varadaStatsWarmupExportService;
    private WarmupElementsCloudExporter warmupElementsCloudExporter;

    @BeforeEach
    void setUp()
    {
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setLocalStorePath("/tmp/test/");

        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);

        RowGroupDataService rowGroupDataService = mock(RowGroupDataService.class);

        cloudVendorService = mock(CloudVendorService.class);
        when(cloudVendorService.getLocation(anyString())).thenCallRealMethod();

        varadaStatsWarmupExportService = new VaradaStatsWarmupExportService(WARMUP_EXPORTER_STAT_GROUP);
        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any(VaradaStatsWarmupExportService.class))).thenReturn(varadaStatsWarmupExportService);

        warmupElementsCloudExporter = new WarmupElementsCloudExporter(globalConfiguration,
                storageEngineConstants,
                rowGroupDataService,
                cloudVendorService,
                metricsManager);
    }

    @Test
    void test_exportFile_1stTime()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getRowGroupKey()).thenReturn(rowGroupKey);
        when(rowGroupData.isSparseFile()).thenReturn(false);

        String cloudImportExportPath = "bucket-test/path-test/file_name_test";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        when(cloudVendorService.uploadFileToCloud(anyString(), any(File.class), any())).thenReturn(true);

        WarmupElementsCloudExporter.ExportFileResults exportFileResults =
                warmupElementsCloudExporter.exportFile(rowGroupData, cloudImportExportPath);

        Assertions.assertTrue(exportFileResults.isExportDone());
    }

    @Test
    void test_exportFile_fileNotExist()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getRowGroupKey()).thenReturn(rowGroupKey);
        when(rowGroupData.isSparseFile()).thenReturn(true);

        String cloudImportExportPath = "bucket-test/path-test/file_name_test";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        WarmupElementsCloudExporter.ExportFileResults exportFileResults =
                warmupElementsCloudExporter.exportFile(rowGroupData, cloudImportExportPath);

        Assertions.assertFalse(exportFileResults.isExportDone());
    }

    @Test
    void test_exportFile_append()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(1234567);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getRowGroupKey()).thenReturn(rowGroupKey);
        when(rowGroupData.isSparseFile()).thenReturn(false);
        when(rowGroupData.getDataValidation()).thenReturn(new RowGroupDataValidation(storageObjectMetadata));

        String cloudImportExportPath = "bucket-test/path-test/file_name_test";

        when(cloudVendorService.uploadFileToCloud(anyString(), any(File.class), any())).thenReturn(true);

        WarmupElementsCloudExporter.ExportFileResults exportFileResults =
                warmupElementsCloudExporter.exportFile(rowGroupData, cloudImportExportPath);

        Assertions.assertTrue(exportFileResults.isExportDone());
    }

    @Test
    void test_exportFile_1stFooterValidation()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getRowGroupKey()).thenReturn(rowGroupKey);
        when(rowGroupData.getDataValidation()).thenReturn(new RowGroupDataValidation(0, 0));

        String cloudImportExportPath = "bucket-test/path-test/file_name_test";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        WarmupElementsCloudExporter.ExportFileResults exportFileResults =
                warmupElementsCloudExporter.exportFile(rowGroupData, cloudImportExportPath);

        Assertions.assertFalse(exportFileResults.isExportDone());
        Assertions.assertEquals(1, varadaStatsWarmupExportService.getexport_row_group_1st_footer_validation());
    }

    @Test
    void test_exportFile_2ndFooterValidation()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(1234567);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getRowGroupKey()).thenReturn(rowGroupKey);
        when(rowGroupData.isSparseFile()).thenReturn(false);
        when(rowGroupData.getDataValidation()).thenReturn(new RowGroupDataValidation(storageObjectMetadata));

        String cloudImportExportPath = "bucket-test/path-test/file_name_test";

        when(cloudVendorService.uploadFileToCloud(anyString(), any(File.class), any())).thenReturn(false);

        WarmupElementsCloudExporter.ExportFileResults exportFileResults =
                warmupElementsCloudExporter.exportFile(rowGroupData, cloudImportExportPath);

        Assertions.assertFalse(exportFileResults.isExportDone());
        Assertions.assertEquals(1, varadaStatsWarmupExportService.getexport_row_group_2nd_footer_validation());
    }
}
