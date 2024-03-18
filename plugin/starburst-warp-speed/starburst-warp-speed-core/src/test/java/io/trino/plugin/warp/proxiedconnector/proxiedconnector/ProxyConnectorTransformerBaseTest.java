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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector;

import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.spi.Node;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class ProxyConnectorTransformerBaseTest
{
    protected final Node node = NodeUtils.node(1, true);

    protected void testCalculateColumnsStatisticsBucketPriority(
            DispatcherProxiedConnectorTransformer transformer,
            Map<ColumnHandle, Double> columnHandleEstimateMap,
            Function<ColumnHandle, String> columnNameFunction)
    {
        Map<ColumnHandle, ColumnStatistics> columnStatisticsMap = columnHandleEstimateMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> new ColumnStatistics(
                                Estimate.of(1D),
                                Estimate.of(entry.getValue()),
                                Estimate.of(entry.getValue()),
                                Optional.empty())));

        DispatcherStatisticsProvider statisticsProvider = mock(DispatcherStatisticsProvider.class);
        when(statisticsProvider.getColumnCardinalityBucket(any(Estimate.class)))
                .thenAnswer(invocation -> {
                    Estimate estimate = (Estimate) invocation.getArguments()[0];
                    return Double.valueOf(estimate.getValue()).intValue();
                });

        Map<String, Integer> expectedResult = columnHandleEstimateMap.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> columnNameFunction.apply(entry.getKey()),
                        entry -> entry.getValue().intValue()));

        Map<String, Integer> result = transformer.calculateColumnsStatisticsBucketPriority(statisticsProvider, columnStatisticsMap);
        assertThat(result).isEqualTo(expectedResult);
    }

    protected void testCreateDispatcherSplit(
            DispatcherProxiedConnectorTransformer transformer,
            ConnectorSplit connectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            DispatcherSplit expectedDispatcherSplit)
    {
        Node node = NodeUtils.node(1, true);

        ConnectorSplitNodeDistributor connectorSplitNodeDistributor = mock(ConnectorSplitNodeDistributor.class);
        when(connectorSplitNodeDistributor.getNode(any(String.class)))
                .thenReturn(node);

        DispatcherSplit dispatcherSplit = transformer.createDispatcherSplit(
                connectorSplit,
                dispatcherTableHandle,
                connectorSplitNodeDistributor,
                mock(ConnectorSession.class));

        assertThat(dispatcherSplit).isEqualTo(expectedDispatcherSplit);
    }

    protected void testCreateProxyTableHandleForWarming(
            DispatcherProxiedConnectorTransformer transformer,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorTableHandle expectedTableHandleForWarming)
    {
        ConnectorTableHandle result = transformer.createProxyTableHandleForWarming(dispatcherTableHandle);
        assertTablesAreEqual(expectedTableHandleForWarming, result);
    }

    protected void testCreateProxiedConnectorTableHandleForMixedQuery(
            DispatcherProxiedConnectorTransformer transformer,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorTableHandle expectedTableHandleMixedQuery)
    {
        ConnectorTableHandle result = transformer.createProxiedConnectorTableHandleForMixedQuery(dispatcherTableHandle);
        assertTablesAreEqual(expectedTableHandleMixedQuery, result);
    }

    protected void assertTablesAreEqual(ConnectorTableHandle expected, ConnectorTableHandle actual)
    {
        assertThat(actual).isEqualTo(expected);
    }
}
