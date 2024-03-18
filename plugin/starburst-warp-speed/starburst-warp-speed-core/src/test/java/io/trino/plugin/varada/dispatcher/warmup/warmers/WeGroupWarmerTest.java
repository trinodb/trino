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

import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupImportService;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.cloudvendors.model.StorageObjectMetadata;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.plugin.varada.dispatcher.warmup.warmers.WeGroupWarmer.WARMUP_IMPORTER_STAT_GROUP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WeGroupWarmerTest
{
    private static Path localStorePath;

    private GlobalConfiguration globalConfiguration;
    private RowGroupDataService rowGroupDataService;
    private CloudVendorService cloudVendorService;
    private VaradaStatsWarmupImportService varadaStatsWarmupImportService;
    private WeGroupWarmer weGroupWarmer;

    @BeforeAll
    static void beforeAll()
            throws IOException
    {
        localStorePath = Files.createTempDirectory("WeGroupWarmerTest");
    }

    @AfterAll
    static void afterAll()
    {
        if (Objects.nonNull(localStorePath)) {
            try (Stream<Path> stream = Files.walk(localStorePath)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
            catch (IOException e) {
                System.out.printf("failed to delete localStorePath '%s'%n", localStorePath);
            }
        }
    }

    @BeforeEach
    void setUp()
    {
        globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setLocalStorePath(localStorePath.toFile().getAbsolutePath());

        CloudVendorConfiguration cloudVendorConfiguration = new CloudVendorConfiguration();
        cloudVendorConfiguration.setStoreType("s3");
        cloudVendorConfiguration.setStorePath("s3://store-bucket/");

        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);

        rowGroupDataService = mock(RowGroupDataService.class);

        cloudVendorService = mock(CloudVendorService.class);
        when(cloudVendorService.getLocation(anyString())).thenCallRealMethod();

        MetricsManager metricsManager = mock(MetricsManager.class);
        varadaStatsWarmupImportService = VaradaStatsWarmupImportService.create(WARMUP_IMPORTER_STAT_GROUP);
        when(metricsManager.registerMetric(any())).thenReturn(varadaStatsWarmupImportService);

        weGroupWarmer = new WeGroupWarmer(globalConfiguration,
                cloudVendorConfiguration,
                storageEngineConstants,
                rowGroupDataService,
                cloudVendorService,
                metricsManager);
    }

    @Test
    void test_isNeedDownload_true()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String path = "test-bucket/test-object-name";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(eq(path))).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getDataValidation()).thenReturn(new RowGroupDataValidation(0, 0));
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        WeGroupWarmer.IsNeedDownloadResults isNeedDownloadResults = weGroupWarmer.isNeedDownload(rowGroupKey, path);
        Assertions.assertTrue(isNeedDownloadResults.isNeedDownload());
        Assertions.assertEquals(storageObjectMetadata.getLastModified().orElseThrow(), isNeedDownloadResults.dataValidation().fileModifiedTime());
        Assertions.assertEquals(storageObjectMetadata.getContentLength().orElseThrow(), isNeedDownloadResults.dataValidation().fileContentLength());
    }

    @Test
    void test_isNeedDownload_AlreadyDownloaded()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String path = "test-bucket/test-object-name";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(eq(path))).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getDataValidation()).thenReturn(new RowGroupDataValidation(storageObjectMetadata));
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        WeGroupWarmer.IsNeedDownloadResults isNeedDownloadResults = weGroupWarmer.isNeedDownload(rowGroupKey, path);
        Assertions.assertFalse(isNeedDownloadResults.isNeedDownload());
    }

    @Test
    void test_isNeedDownload_NoFile()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String path = "test-bucket/test-object-name";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(eq(path))).thenReturn(storageObjectMetadata);

        WeGroupWarmer.IsNeedDownloadResults isNeedDownloadResults = weGroupWarmer.isNeedDownload(rowGroupKey, path);
        Assertions.assertFalse(isNeedDownloadResults.isNeedDownload());
    }

    @Test
    void test_importWeGroup_downloadNotNeeded()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        Optional<RowGroupData> rowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(rowGroupData.isEmpty());
    }

    @Test
    void test_importWeGroup_Exception()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        doThrow(new IllegalArgumentException("test-exception")).when(cloudVendorService).downloadFileFromCloud(anyString(), any(File.class));

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getDataValidation()).thenReturn(new RowGroupDataValidation(0, 0));
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(optionalRowGroupData.isEmpty());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_we_group_download_started());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_we_group_download_failed());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_we_group_download_accomplished());
    }

    @Test
    void test_importWeGroup_downloadOk()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        File localFile = new File(localFileName);
        File localTmpFile = new File(localFileName + ".tmp");

        FileUtils.createParentDirectories(localFile);
        localFile.createNewFile();
        localTmpFile.createNewFile();

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(List.of())
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);
        when(rowGroupDataService.reload(eq(rowGroupKey), eq(rowGroupData))).thenReturn(rowGroupData);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(optionalRowGroupData.isPresent());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_we_group_download_started());
        Assertions.assertEquals(0, varadaStatsWarmupImportService.getimport_we_group_download_failed());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_we_group_download_accomplished());

        localFile.delete();
        localTmpFile.delete();
    }

    @Test
    void test_importWeGroup_corruptedFile()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 1, 1L, 1, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        File localFile = new File(localFileName);
        File localTmpFile = new File(localFileName + ".tmp");

        FileUtils.createParentDirectories(localFile);
        localFile.createNewFile();
        localTmpFile.createNewFile();

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(List.of())
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);
        when(rowGroupDataService.reload(eq(rowGroupKey), eq(rowGroupData))).thenAnswer(invocation -> {
            localFile.delete();
            return null;
        });

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(optionalRowGroupData.isPresent());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_we_group_download_started());
        Assertions.assertEquals(0, varadaStatsWarmupImportService.getimport_we_group_download_failed());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_we_group_download_accomplished());

        localFile.delete();
        localTmpFile.delete();
    }

    @Test
    void test_importWarmUpElements_ok()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-1", "table-1", "s3://test-bucket/column_split_file", 1, 1L, 1, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        File localFile = new File(localFileName);

        FileUtils.createParentDirectories(localFile);
        localFile.createNewFile();

        long lastModified = Instant.now().toEpochMilli();
        long contentLength = 123456789;
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(lastModified);
        storageObjectMetadata.setContentLength(contentLength);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        List<WarmUpElement> warmUpElements = new ArrayList<>();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName("colName-" + i)
                    .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                    .warmupElementStats(new WarmupElementStats(0, Long.MAX_VALUE, Long.MIN_VALUE))
                    .warmState(((i % 2) == 1) ? WarmState.WARM : WarmState.HOT)
                    .build();
            warmUpElements.add(warmUpElement);
            if ((i % 2) == 1) {
                warmWarmUpElements.add(warmUpElement);
            }
        }

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .dataValidation(new RowGroupDataValidation(lastModified, contentLength))
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt())).thenReturn(InputStream.nullInputStream());

        ArgumentCaptor<RowGroupKey> argumentCaptor1 = ArgumentCaptor.forClass(RowGroupKey.class);
        ArgumentCaptor<RowGroupData> argumentCaptor2 = ArgumentCaptor.forClass(RowGroupData.class);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, warmWarmUpElements);
        Assertions.assertTrue(optionalRowGroupData.isPresent());

        RowGroupData returnedRowGroupData = optionalRowGroupData.orElseThrow();
        Assertions.assertEquals(4, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(0, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        Assertions.assertEquals(2, varadaStatsWarmupImportService.getimport_elements_started());
        Assertions.assertEquals(0, varadaStatsWarmupImportService.getimport_elements_failed());
        Assertions.assertEquals(2, varadaStatsWarmupImportService.getimport_elements_accomplished());

        verify(rowGroupDataService, times(1)).flush(argumentCaptor1.capture());
        verify(rowGroupDataService, times(1)).save(argumentCaptor2.capture());
        RowGroupData savedRowGroupData = argumentCaptor2.getValue();
        Assertions.assertEquals(4, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(0, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        localFile.delete();
    }

    @Test
    void test_importWarmUpElements_Exception()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        long lastModified = Instant.now().toEpochMilli();
        long contentLength = 123456789;
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(lastModified);
        storageObjectMetadata.setContentLength(contentLength);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        List<WarmUpElement> warmUpElements = new ArrayList<>();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName("colName-" + i)
                    .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                    .warmupElementStats(new WarmupElementStats(0, Long.MAX_VALUE, Long.MIN_VALUE))
                    .warmState(((i % 2) == 1) ? WarmState.WARM : WarmState.HOT)
                    .build();
            warmUpElements.add(warmUpElement);
            if ((i % 2) == 1) {
                warmWarmUpElements.add(warmUpElement);
            }
        }

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .dataValidation(new RowGroupDataValidation(lastModified, contentLength))
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt())).thenThrow(new IllegalArgumentException("test-exception"));

        ArgumentCaptor<RowGroupKey> argumentCaptor1 = ArgumentCaptor.forClass(RowGroupKey.class);
        ArgumentCaptor<RowGroupData> argumentCaptor2 = ArgumentCaptor.forClass(RowGroupData.class);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, warmWarmUpElements);
        Assertions.assertTrue(optionalRowGroupData.isPresent());

        RowGroupData returnedRowGroupData = optionalRowGroupData.orElseThrow();
        Assertions.assertEquals(warmUpElements, returnedRowGroupData.getWarmUpElements());
        Assertions.assertEquals(2, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(2, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        Assertions.assertEquals(2, varadaStatsWarmupImportService.getimport_elements_started());
        Assertions.assertEquals(2, varadaStatsWarmupImportService.getimport_elements_failed());
        Assertions.assertEquals(2, varadaStatsWarmupImportService.getimport_elements_accomplished());

        verify(rowGroupDataService, times(0)).flush(argumentCaptor1.capture());
        verify(rowGroupDataService, times(0)).save(argumentCaptor2.capture());
    }

    @Test
    void test_importWarmUpElements_partial()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-2", "table-2", "s3://test-bucket/column_split_file", 2, 2L, 2, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        File localFile = new File(localFileName);

        FileUtils.createParentDirectories(localFile);
        localFile.createNewFile();

        long lastModified = Instant.now().toEpochMilli();
        long contentLength = 123456789;
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(lastModified);
        storageObjectMetadata.setContentLength(contentLength);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        List<WarmUpElement> warmUpElements = new ArrayList<>();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName("colName-" + i)
                    .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                    .warmupElementStats(new WarmupElementStats(0, Long.MAX_VALUE, Long.MIN_VALUE))
                    .warmState(((i % 2) == 1) ? WarmState.WARM : WarmState.HOT)
                    .build();
            warmUpElements.add(warmUpElement);
            if ((i % 2) == 1) {
                warmWarmUpElements.add(warmUpElement);
            }
        }

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .dataValidation(new RowGroupDataValidation(lastModified, contentLength))
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt()))
                .thenThrow(new IllegalArgumentException("test-exception"))
                .thenReturn(InputStream.nullInputStream());

        ArgumentCaptor<RowGroupKey> argumentCaptor1 = ArgumentCaptor.forClass(RowGroupKey.class);
        ArgumentCaptor<RowGroupData> argumentCaptor2 = ArgumentCaptor.forClass(RowGroupData.class);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, warmWarmUpElements);
        Assertions.assertTrue(optionalRowGroupData.isPresent());

        RowGroupData returnedRowGroupData = optionalRowGroupData.orElseThrow();
        Assertions.assertEquals(3, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(1, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        Assertions.assertEquals(2, varadaStatsWarmupImportService.getimport_elements_started());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_elements_failed());
        Assertions.assertEquals(2, varadaStatsWarmupImportService.getimport_elements_accomplished());

        verify(rowGroupDataService, times(1)).flush(argumentCaptor1.capture());
        verify(rowGroupDataService, times(1)).save(argumentCaptor2.capture());
        RowGroupData savedRowGroupData = argumentCaptor2.getValue();
        Assertions.assertEquals(3, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(1, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        localFile.delete();
    }

    @Test
    void test_importWarmUpElements_1stFooterValidation()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        long lastModified = Instant.now().toEpochMilli();
        long contentLength = 123456789;

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(lastModified);
        storageObjectMetadata.setContentLength(contentLength);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        List<WarmUpElement> warmUpElements = new ArrayList<>();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName("colName-" + i)
                    .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                    .warmupElementStats(new WarmupElementStats(0, Long.MAX_VALUE, Long.MIN_VALUE))
                    .warmState(((i % 2) == 1) ? WarmState.WARM : WarmState.HOT)
                    .build();
            warmUpElements.add(warmUpElement);
            if ((i % 2) == 1) {
                warmWarmUpElements.add(warmUpElement);
            }
        }

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .dataValidation(new RowGroupDataValidation(lastModified, contentLength - 1))
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, warmWarmUpElements);

        Assertions.assertTrue(optionalRowGroupData.isEmpty());
        Assertions.assertEquals(0, varadaStatsWarmupImportService.getimport_elements_started());
    }

    @Test
    void test_importWarmUpElements_2ndFooterValidation()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-3", "table-3", "s3://test-bucket/column_split_file", 3, 3L, 3, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        File localFile = new File(localFileName);

        FileUtils.createParentDirectories(localFile);
        localFile.createNewFile();

        long lastModified = Instant.now().toEpochMilli();
        long contentLength = 123456789;

        StorageObjectMetadata storageObjectMetadata1 = new StorageObjectMetadata();
        storageObjectMetadata1.setLastModified(lastModified);
        storageObjectMetadata1.setContentLength(contentLength);

        StorageObjectMetadata storageObjectMetadata2 = new StorageObjectMetadata();
        storageObjectMetadata2.setLastModified(lastModified);
        storageObjectMetadata2.setContentLength(contentLength + 1);

        when(cloudVendorService.getObjectMetadata(anyString()))
                .thenReturn(storageObjectMetadata1)
                .thenReturn(storageObjectMetadata2);

        List<WarmUpElement> warmUpElements = new ArrayList<>();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName("colName-" + i)
                    .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                    .warmupElementStats(new WarmupElementStats(0, Long.MAX_VALUE, Long.MIN_VALUE))
                    .warmState(((i % 2) == 1) ? WarmState.WARM : WarmState.HOT)
                    .build();
            warmUpElements.add(warmUpElement);
            if ((i % 2) == 1) {
                warmWarmUpElements.add(warmUpElement);
            }
        }

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .dataValidation(new RowGroupDataValidation(lastModified, contentLength))
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt())).thenReturn(InputStream.nullInputStream());

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, warmWarmUpElements);

        Assertions.assertTrue(optionalRowGroupData.isEmpty());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_elements_started());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_elements_failed());
        Assertions.assertEquals(1, varadaStatsWarmupImportService.getimport_elements_accomplished());

        localFile.delete();
    }
}
