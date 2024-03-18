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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.FastWarmingState;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupExportService;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.createRegularWarmupElements;
import static io.trino.plugin.varada.dispatcher.warmup.export.WarmupExportingService.WARMUP_EXPORTER_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WeGroupCloudExporterTaskTest
{
    private static final RegularColumn column1 = new RegularColumn("c1");
    private static final RegularColumn column2 = new RegularColumn("c2");
    private static final RegularColumn column3 = new RegularColumn("c3");

    private RowGroupKey rowGroupKey;
    private String cloudImportExportPath;
    private WorkerTaskExecutorService workerTaskExecutorService;
    private RowGroupDataService rowGroupDataService;
    private WarmupElementsCloudExporter warmupElementsCloudExporter;
    private GlobalConfiguration globalConfiguration;
    private VaradaStatsWarmupExportService varadaStatsWarmupExportService;

    @BeforeEach
    void setUp()
    {
        rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        workerTaskExecutorService = mock(WorkerTaskExecutorService.class);

        Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
        columnNameToWarmUpType.put(column1, WarmUpType.WARM_UP_TYPE_BASIC);
        columnNameToWarmUpType.put(column2, WarmUpType.WARM_UP_TYPE_BASIC);
        columnNameToWarmUpType.put(column3, WarmUpType.WARM_UP_TYPE_BASIC);

        List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);
        RowGroupData rowGroupData = createRowGroup(warmupElements);

        rowGroupDataService = mock(RowGroupDataService.class);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);

        warmupElementsCloudExporter = mock(WarmupElementsCloudExporter.class);

        globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setEnableImportExport(true);

        CloudVendorConfiguration cloudVendorConfiguration = new CloudVendorConfiguration();
        cloudVendorConfiguration.setStoreType("s3");
        cloudVendorConfiguration.setStorePath("s3://bucket/");

        varadaStatsWarmupExportService = new VaradaStatsWarmupExportService(WARMUP_EXPORTER_STAT_GROUP);

        CloudVendorService cloudVendorService = mock(CloudVendorService.class);
        when(cloudVendorService.getLocation(anyString())).thenCallRealMethod();

        cloudImportExportPath = VaradaSessionProperties.getS3ImportExportPath(null, cloudVendorConfiguration, cloudVendorService);
    }

    @Test
    void test_exportWeGroup()
    {
        when(warmupElementsCloudExporter.exportFile(any(RowGroupData.class), eq(cloudImportExportPath)))
                .thenReturn(new WarmupElementsCloudExporter.ExportFileResults(true, new RowGroupDataValidation(0, 0)));

        ArgumentCaptor<RowGroupData> rowGroupDataCaptor = ArgumentCaptor.forClass(RowGroupData.class);

        WeGroupCloudExporterTask task = new WeGroupCloudExporterTask(rowGroupKey,
                cloudImportExportPath,
                workerTaskExecutorService,
                rowGroupDataService,
                warmupElementsCloudExporter,
                globalConfiguration,
                varadaStatsWarmupExportService);
        task.run();

        verify(warmupElementsCloudExporter, times(1)).exportFile(any(RowGroupData.class), eq(cloudImportExportPath));

        assertThat(varadaStatsWarmupExportService.getexport_row_group_accomplished()).isEqualTo(1);
        assertThat(varadaStatsWarmupExportService.getexport_row_group_finished()).isEqualTo(1);
        assertThat(varadaStatsWarmupExportService.getexport_skipped_due_key_demoted_row_group()).isZero();

        verify(workerTaskExecutorService, times(1)).taskFinished(eq(rowGroupKey));
        verify(rowGroupDataService, times(1)).save(rowGroupDataCaptor.capture());

        Assertions.assertEquals(FastWarmingState.EXPORTED, rowGroupDataCaptor.getValue().getFastWarmingState());
    }

    @Test
    void test_exportWeGroup_fail()
    {
        doThrow(new RuntimeException("test"))
                .when(warmupElementsCloudExporter)
                .exportFile(any(RowGroupData.class), eq(cloudImportExportPath));

        WeGroupCloudExporterTask task = new WeGroupCloudExporterTask(rowGroupKey,
                cloudImportExportPath,
                workerTaskExecutorService,
                rowGroupDataService,
                warmupElementsCloudExporter,
                globalConfiguration,
                varadaStatsWarmupExportService);
        task.run();

        verify(warmupElementsCloudExporter, times(1))
                .exportFile(any(RowGroupData.class), eq(cloudImportExportPath));

        assertThat(varadaStatsWarmupExportService.getexport_row_group_accomplished()).isEqualTo(0);
        assertThat(varadaStatsWarmupExportService.getexport_row_group_failed()).isEqualTo(1);
        assertThat(varadaStatsWarmupExportService.getexport_row_group_finished()).isEqualTo(1);
        assertThat(varadaStatsWarmupExportService.getexport_skipped_due_key_demoted_row_group()).isZero();

        verify(workerTaskExecutorService, times(1)).taskFinished(eq(rowGroupKey));
    }

    private RowGroupData createRowGroup(List<WarmUpElement> warmupElements)
    {
        return RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmupElements)
                .nextOffset(10)
                .fastWarmingState(FastWarmingState.NOT_EXPORTED)
                .build();
    }
}
