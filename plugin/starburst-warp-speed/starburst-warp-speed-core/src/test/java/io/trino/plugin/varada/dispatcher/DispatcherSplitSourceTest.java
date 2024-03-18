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
package io.trino.plugin.varada.dispatcher;

import com.google.common.collect.MoreCollectors;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.storage.splits.ConnectorSplitConsistentHashNodeDistributor;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.spi.Node;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.testing.InterfaceTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DispatcherSplitSourceTest
{
    final int maxSize = 100;
    private List<ConnectorSplit> connectorSplits;
    private CoordinatorNodeManager coordinatorNodeManager;
    private List<Node> workers;
    private DispatcherSplitSource dispatcherSplitSource;

    @SuppressWarnings({"UnstableApiUsage", "MockNotUsedInProduction"})
    @BeforeEach
    public void before()
    {
        ConnectorSplitSource connectorSplitSource = mock(ConnectorSplitSource.class);
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();

        connectorSplits = IntStream.range(0, 10)
                .mapToObj(i -> new DispatcherSplit(
                        "database",
                        "table",
                        "path" + i,
                        0,
                        1,
                        2L,
                        List.of(),
                        List.of(),
                        "",
                        mock(ConnectorSplit.class)))
                .collect(Collectors.toList());
        ConnectorSplitSource.ConnectorSplitBatch connectorSplitBatch = mock(ConnectorSplitSource.ConnectorSplitBatch.class);
        when(connectorSplitBatch.getSplits()).thenAnswer(invocation -> connectorSplits);
        CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> connectorSplitBatchCompletableFuture = CompletableFuture.completedFuture(connectorSplitBatch);
        when(connectorSplitSource.getNextBatch(eq(maxSize))).thenReturn(connectorSplitBatchCompletableFuture);

        coordinatorNodeManager = mock(CoordinatorNodeManager.class);
        mockNodeManager(1, 2, 3);
        dispatcherSplitSource = new DispatcherSplitSource(connectorSplitSource,
                mock(DispatcherTableHandle.class),
                mock(ConnectorSession.class),
                new TestingConnectorProxiedConnectorTransformer(),
                new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager));
    }

    @Test
    public void testGetNextBatch()
            throws ExecutionException, InterruptedException
    {
        CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> splitBatchCompletableFuture = dispatcherSplitSource.getNextBatch(maxSize);

        List<ConnectorSplit> connectorSplitsResult1 = splitBatchCompletableFuture.get().getSplits();

        assertThat(connectorSplits.size()).isEqualTo(connectorSplitsResult1.size());

        Set<String> workerHostAddress = workers.stream()
                .map(Node::getHost)
                .collect(Collectors.toSet());

        for (int i = 0; i < connectorSplits.size(); i++) {
            DispatcherSplit hiveSplit = (DispatcherSplit) connectorSplits.get(i);
            DispatcherSplit connectorSplitResult = (DispatcherSplit) connectorSplitsResult1.get(i);
            assertThat(hiveSplit.getPath()).isEqualTo(connectorSplitResult.getPath());
            assertThat(connectorSplitResult.getAddresses()).hasSize(1);
            assertThat(workerHostAddress.contains(connectorSplitResult.getAddresses().stream().collect(MoreCollectors.onlyElement()).getHostText())).isTrue();
        }
    }

    @Test
    public void testEverythingImplemented()
    {
        InterfaceTestUtils.assertAllMethodsOverridden(
                ConnectorSplitSource.class,
                DispatcherSplitSource.class);
    }

    private void mockNodeManager(Integer... indexes)
    {
        Map<Integer, Node> workersMap = Arrays.stream(indexes)
                .collect(Collectors.toMap(Function.identity(), i -> NodeUtils.node(i, false)));

        workers = new ArrayList<>(workersMap.values());
        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers);
    }
}
