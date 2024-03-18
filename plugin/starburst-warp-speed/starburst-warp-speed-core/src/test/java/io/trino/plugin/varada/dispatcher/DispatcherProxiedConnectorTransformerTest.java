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

import io.airlift.slice.Slices;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.connector.TestingConnectorTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.spi.Node;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DispatcherProxiedConnectorTransformerTest
{
    @Test
    public void testGetHostAddressForSplit()
    {
        Node node = NodeUtils.node(1, true);

        ConnectorSplitNodeDistributor connectorSplitNodeDistributor = mock(ConnectorSplitNodeDistributor.class);
        when(connectorSplitNodeDistributor.getNode(any(String.class)))
                .thenReturn(node);

        DispatcherProxiedConnectorTransformer transformer = new TestingConnectorProxiedConnectorTransformer();
        assertThat(transformer.getHostAddressForSplit("splitKey", connectorSplitNodeDistributor))
                .isEqualTo(List.of(node.getHostAndPort()));
    }

    @Test
    public void testCreateProxiedMetadata()
    {
        ConnectorTransactionHandle connectorTransactionHandle = mock(ConnectorTransactionHandle.class);
        ConnectorMetadata metadata = mock(ConnectorMetadata.class);

        Connector connector = mock(Connector.class);
        when(connector.beginTransaction(any(), anyBoolean(), anyBoolean())).thenReturn(connectorTransactionHandle);
        when(connector.getMetadata(any(), eq(connectorTransactionHandle))).thenReturn(metadata);

        DispatcherProxiedConnectorTransformer transformer = new TestingConnectorProxiedConnectorTransformer();
        assertThat(transformer.createProxiedMetadata(connector, mock(ConnectorSession.class)))
                .isEqualTo(Pair.of(metadata, connectorTransactionHandle));
    }

    @Test
    public void testGetSimplifiedColumns()
    {
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col1");
        TestingConnectorTableHandle connectorTableHandle = new TestingConnectorTableHandle(
                "schema",
                "table",
                List.of(columnHandle,
                        new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col2")),
                List.of(new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col3"),
                        new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col4")),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1L),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                connectorTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);

        DispatcherProxiedConnectorTransformer transformer = new TestingConnectorProxiedConnectorTransformer();
        assertThat(transformer.getSimplifiedColumns(
                dispatcherTableHandle,
                TupleDomain.withColumnDomains(
                        Map.of(columnHandle, Domain.singleValue(columnHandle.type(), Slices.utf8Slice("e")))),
                1_000_000))
                .isEqualTo(dispatcherTableHandle.getSimplifiedColumns());
    }
}
