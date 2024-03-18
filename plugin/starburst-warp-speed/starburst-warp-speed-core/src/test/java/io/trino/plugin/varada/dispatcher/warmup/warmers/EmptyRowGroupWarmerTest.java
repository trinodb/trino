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

import com.google.common.collect.SetMultimap;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmData;
import io.trino.plugin.varada.dispatcher.warmup.WarmExecutionState;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.IntegerType;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.createRequiredWarmUpTypes;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EmptyRowGroupWarmerTest
{
    private MetricsManager metricsManager;
    private SchemaTableName schemaTableName;

    @BeforeEach
    public void before()
    {
        metricsManager = mock(MetricsManager.class);
        VaradaStatsWarmingService varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);
        when(metricsManager.registerMetric(any())).thenReturn(varadaStatsWarmingService);
        when(metricsManager.get(any())).thenReturn(varadaStatsWarmingService);
        schemaTableName = new SchemaTableName("schema", "table");
    }

    @Test
    public void testEmptyRowGroup()
    {
        RowGroupDataService rowGroupDataService = mock(RowGroupDataService.class);

        WarmupElementsCreator warmupElementsCreator = new WarmupElementsCreator(
                rowGroupDataService,
                metricsManager,
                mock(StorageEngineConstants.class),
                new TestingConnectorProxiedConnectorTransformer(),
                new GlobalConfiguration());

        EmptyRowGroupWarmer emptyRowGroupWarmer = new EmptyRowGroupWarmer(rowGroupDataService);
        String columnName = "C1";
        WarmUpType[] requiredWarmupType = {WarmUpType.WARM_UP_TYPE_DATA};

        RowGroupData rowGroupData = mock(RowGroupData.class);
        RowGroupKey rowGroupKey = mock(RowGroupKey.class);
        when(rowGroupKey.schema()).thenReturn(schemaTableName.getSchemaName());
        when(rowGroupKey.table()).thenReturn(schemaTableName.getTableName());
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);
        List<ColumnHandle> columns = mockColumns(List.of(Pair.of(columnName, IntegerType.INTEGER)));
        SetMultimap<VaradaColumn, WarmupProperties> newRequiredWarmUpTypes = createRequiredWarmUpTypes(columns,
                List.of(requiredWarmupType));
        WarmData warmData = new WarmData(columns, newRequiredWarmUpTypes, WarmExecutionState.EMPTY_ROW_GROUP, true, mock(QueryContext.class), null);
        List<WarmUpElement> warmupElements = warmupElementsCreator.createWarmupElements(rowGroupKey, warmData.requiredWarmUpTypeMap(), schemaTableName, warmData.columnHandleList());
        emptyRowGroupWarmer.warm(rowGroupKey, warmupElements);

        ArgumentCaptor<List> argCaptor = ArgumentCaptor.forClass(List.class);
        verify(rowGroupDataService, times(1)).updateEmptyRowGroup(eq(rowGroupData), argCaptor.capture(), anyList());
        List<WarmUpElement> capturedResult = argCaptor.getValue();
        assertThat(capturedResult.size()).isEqualTo(1);
    }
}
