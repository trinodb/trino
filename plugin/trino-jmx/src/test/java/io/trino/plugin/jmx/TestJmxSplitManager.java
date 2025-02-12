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
package io.trino.plugin.jmx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static io.trino.plugin.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJmxSplitManager
{
    private static final Duration JMX_STATS_DUMP = new Duration(100, TimeUnit.MILLISECONDS);
    private static final int SLEEP_TIME = toIntExact(JMX_STATS_DUMP.toMillis() / 5);
    private static final long TIMEOUT_TIME = JMX_STATS_DUMP.toMillis() * 40;
    private static final String TEST_BEANS = "java.lang:type=Runtime";
    private static final String CONNECTOR_ID = "test-id";
    private final Node localNode = createTestingNode("host1");
    private final Set<Node> nodes = ImmutableSet.of(localNode, createTestingNode("host2"), createTestingNode("host3"));
    private final NodeManager nodeManager = new TestingNodeManager(localNode, nodes);

    private final JmxConnector jmxConnector =
            (JmxConnector) new JmxConnectorFactory()
                    .create(CONNECTOR_ID, ImmutableMap.of(
                            "jmx.dump-tables", TEST_BEANS,
                            "jmx.dump-period", format("%dms", JMX_STATS_DUMP.toMillis()),
                            "jmx.max-entries", "1000",
                            "bootstrap.quiet", "true"),
                            new ConnectorContext()
                            {
                                @Override
                                public NodeManager getNodeManager()
                                {
                                    return nodeManager;
                                }
                            });

    private final JmxColumnHandle columnHandle = new JmxColumnHandle("node", createUnboundedVarcharType());

    private final JmxSplitManager splitManager = jmxConnector.getSplitManager();
    private final JmxMetadata metadata = jmxConnector.getMetadata(SESSION, new ConnectorTransactionHandle() {});
    private final JmxRecordSetProvider recordSetProvider = jmxConnector.getRecordSetProvider();

    @AfterAll
    public void tearDown()
    {
        jmxConnector.shutdown();
    }

    @Test
    public void testPredicatePushdown()
            throws Exception
    {
        for (Node node : nodes) {
            String nodeIdentifier = node.getNodeIdentifier();
            TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(columnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice(nodeIdentifier))));
            JmxTableHandle tableHandle = new JmxTableHandle(new SchemaTableName("schema", "tableName"), ImmutableList.of("objectName"), ImmutableList.of(columnHandle), true, nodeTupleDomain);

            ConnectorSplitSource splitSource = splitManager.getSplits(JmxTransactionHandle.INSTANCE, SESSION, tableHandle, DynamicFilter.EMPTY, alwaysTrue());
            List<ConnectorSplit> allSplits = getAllSplits(splitSource);

            assertThat(allSplits).hasSize(1);
            assertThat(allSplits.get(0).getAddresses()).hasSize(1);
            assertThat(allSplits.get(0).getAddresses().get(0).getHostText())
                    .isEqualTo(nodeIdentifier);
        }
    }

    @Test
    public void testNoPredicate()
            throws Exception
    {
        JmxTableHandle tableHandle = new JmxTableHandle(new SchemaTableName("schema", "tableName"), ImmutableList.of("objectName"), ImmutableList.of(columnHandle), true, TupleDomain.all());
        ConnectorSplitSource splitSource = splitManager.getSplits(JmxTransactionHandle.INSTANCE, SESSION, tableHandle, DynamicFilter.EMPTY, alwaysTrue());
        List<ConnectorSplit> allSplits = getAllSplits(splitSource);
        assertThat(allSplits).hasSize(nodes.size());

        Set<String> actualNodes = nodes.stream().map(Node::getNodeIdentifier).collect(toSet());
        Set<String> expectedNodes = new HashSet<>();
        for (ConnectorSplit split : allSplits) {
            List<HostAddress> addresses = split.getAddresses();
            assertThat(addresses).hasSize(1);
            expectedNodes.add(addresses.get(0).getHostText());
        }
        assertThat(actualNodes).isEqualTo(expectedNodes);
    }

    @Test
    public void testRecordSetProvider()
            throws Exception
    {
        for (SchemaTableName schemaTableName : metadata.listTables(SESSION, Optional.of(JMX_SCHEMA_NAME))) {
            RecordSet recordSet = getRecordSet(schemaTableName);
            try (RecordCursor cursor = recordSet.cursor()) {
                while (cursor.advanceNextPosition()) {
                    for (int i = 0; i < recordSet.getColumnTypes().size(); i++) {
                        cursor.isNull(i);
                    }
                }
            }
        }
    }

    @Test
    public void testNonExistentObjectName()
            throws Exception
    {
        JmxTableHandle jmxTableHandle = metadata.listTables(SESSION, Optional.of(JMX_SCHEMA_NAME)).stream()
                .map(schemaTableName -> metadata.getTableHandle(SESSION, schemaTableName, Optional.empty(), Optional.empty()))
                .filter(Objects::nonNull)
                .filter(tableHandle -> !tableHandle.objectNames().isEmpty())
                .findFirst()
                .orElseThrow();

        ImmutableList<String> objectNamesWithUnknowns = ImmutableList.<String>builder()
                .addAll(jmxTableHandle.objectNames())
                .add("JMImplementation:type=Unknown")
                .build();
        JmxTableHandle tableHandleWithUnknownObject = new JmxTableHandle(
                jmxTableHandle.tableName(),
                objectNamesWithUnknowns,
                jmxTableHandle.columnHandles(),
                jmxTableHandle.liveData(),
                jmxTableHandle.nodeFilter());

        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandleWithUnknownObject).values());
        ConnectorSplitSource splitSource = splitManager.getSplits(JmxTransactionHandle.INSTANCE, SESSION, tableHandleWithUnknownObject, DynamicFilter.EMPTY, alwaysTrue());
        List<ConnectorSplit> allSplits = getAllSplits(splitSource);
        ConnectorSplit split = allSplits.getFirst();

        RecordSet recordSet = recordSetProvider.getRecordSet(JmxTransactionHandle.INSTANCE, SESSION, split, tableHandleWithUnknownObject, columnHandles);

        int count = 0;
        try (RecordCursor cursor = recordSet.cursor()) {
            while (cursor.advanceNextPosition()) {
                count++;
            }
        }

        assertThat(count).isEqualTo(objectNamesWithUnknowns.size() - 1);
    }

    @Test
    public void testHistoryRecordSetProvider()
            throws Exception
    {
        for (SchemaTableName schemaTableName : metadata.listTables(SESSION, Optional.of(HISTORY_SCHEMA_NAME))) {
            // wait for at least two samples
            List<Long> timeStamps = ImmutableList.of();
            for (int waited = 0; waited < TIMEOUT_TIME; waited += SLEEP_TIME) {
                RecordSet recordSet = getRecordSet(schemaTableName);
                timeStamps = readTimeStampsFrom(recordSet);
                if (timeStamps.size() >= 2) {
                    break;
                }
                Thread.sleep(SLEEP_TIME);
            }
            assertThat(timeStamps).matches(value -> value.size() >= 2);

            // we don't have equality check here because JmxHistoryDumper scheduling can lag
            assertThat(timeStamps.get(1) - timeStamps.get(0)).matches(delta -> delta >= JMX_STATS_DUMP.toMillis());
        }
    }

    private List<Long> readTimeStampsFrom(RecordSet recordSet)
    {
        ImmutableList.Builder<Long> result = ImmutableList.builder();
        try (RecordCursor cursor = recordSet.cursor()) {
            while (cursor.advanceNextPosition()) {
                for (int i = 0; i < recordSet.getColumnTypes().size(); i++) {
                    cursor.isNull(i);
                }
                if (cursor.isNull(0)) {
                    return result.build();
                }
                assertThat(recordSet.getColumnTypes().get(0)).isEqualTo(createTimestampWithTimeZoneType(3));
                result.add(cursor.getLong(0));
            }
        }
        return result.build();
    }

    private RecordSet getRecordSet(SchemaTableName schemaTableName)
            throws Exception
    {
        JmxTableHandle tableHandle = metadata.getTableHandle(SESSION, schemaTableName, Optional.empty(), Optional.empty());
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        ConnectorSplitSource splitSource = splitManager.getSplits(JmxTransactionHandle.INSTANCE, SESSION, tableHandle, DynamicFilter.EMPTY, alwaysTrue());
        List<ConnectorSplit> allSplits = getAllSplits(splitSource);
        assertThat(allSplits).hasSize(nodes.size());
        ConnectorSplit split = allSplits.get(0);

        return recordSetProvider.getRecordSet(JmxTransactionHandle.INSTANCE, SESSION, split, tableHandle, columnHandles);
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException, ExecutionException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(splitSource.getNextBatch(1000).get().getSplits());
        }
        return splits.build();
    }

    private static Node createTestingNode(String hostname)
    {
        return new InternalNode(hostname, URI.create(format("http://%s:8080", hostname)), NodeVersion.UNKNOWN, false);
    }
}
