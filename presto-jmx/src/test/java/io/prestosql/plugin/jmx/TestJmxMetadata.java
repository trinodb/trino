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
package io.prestosql.plugin.jmx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.client.NodeVersion;
import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.Node;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static io.prestosql.plugin.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJmxMetadata
{
    private static final String RUNTIME_OBJECT = "java.lang:type=Runtime";
    private static final SchemaTableName RUNTIME_TABLE = new SchemaTableName(JMX_SCHEMA_NAME, RUNTIME_OBJECT.toLowerCase(ENGLISH));
    private static final SchemaTableName RUNTIME_HISTORY_TABLE = new SchemaTableName(HISTORY_SCHEMA_NAME, RUNTIME_OBJECT.toLowerCase(ENGLISH));

    private final Node localNode = createTestingNode("host1");
    private final Set<Node> nodes = ImmutableSet.of(localNode, createTestingNode("host2"), createTestingNode("host3"));
    private final JmxMetadata metadata = new JmxMetadata(getPlatformMBeanServer(), new JmxHistoricalData(1000, ImmutableSet.of(RUNTIME_OBJECT.toLowerCase(ENGLISH))));

    @Test
    public void testListSchemas()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of(JMX_SCHEMA_NAME, HISTORY_SCHEMA_NAME));
    }

    @Test
    public void testListTables()
    {
        assertTrue(metadata.listTables(SESSION, Optional.of(JMX_SCHEMA_NAME)).contains(RUNTIME_TABLE));
        assertTrue(metadata.listTables(SESSION, Optional.of(HISTORY_SCHEMA_NAME)).contains(RUNTIME_HISTORY_TABLE));
    }

    @Test
    public void testGetTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, RUNTIME_TABLE);
        assertEquals(handle.getObjectNames(), ImmutableList.of(RUNTIME_OBJECT));

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertTrue(columns.contains(new JmxColumnHandle("node", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("Name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("StartTime", BIGINT)));
    }

    @Test
    public void testGetTimeTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, RUNTIME_HISTORY_TABLE);
        assertEquals(handle.getObjectNames(), ImmutableList.of(RUNTIME_OBJECT));

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertTrue(columns.contains(new JmxColumnHandle("timestamp", TIMESTAMP)));
        assertTrue(columns.contains(new JmxColumnHandle("node", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("Name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("StartTime", BIGINT)));
    }

    @Test
    public void testGetCumulativeTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));
        assertTrue(handle.getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(handle.getObjectNames().size() > 1);

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertTrue(columns.contains(new JmxColumnHandle("node", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("object_name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("Name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("StartTime", BIGINT)));

        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*java.lang:type=Runtime*")).getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*=Runtime")).getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*")).getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*:*")).getObjectNames().contains(RUNTIME_OBJECT));
    }

    @Test
    public void testApplyFilterWithoutConstraint()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, handle, new Constraint(TupleDomain.all()));

        assertFalse(result.isPresent());
    }

    @Test
    public void testApplyFilterWithConstraint()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));

        JmxColumnHandle nodeColumnHandle = new JmxColumnHandle("node", createUnboundedVarcharType());
        NullableValue nodeColumnValue = NullableValue.of(createUnboundedVarcharType(), utf8Slice(localNode.getNodeIdentifier()));

        JmxColumnHandle objectNameColumnHandle = new JmxColumnHandle("object_name", createUnboundedVarcharType());
        NullableValue objectNameColumnValue = NullableValue.of(createUnboundedVarcharType(), utf8Slice("presto.memory:type=MemoryPool,name=reserved"));

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(nodeColumnHandle, nodeColumnValue, objectNameColumnHandle, objectNameColumnValue));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, handle, new Constraint(tupleDomain));

        assertTrue(result.isPresent());
        assertEquals(result.get().getRemainingFilter(), TupleDomain.fromFixedValues(ImmutableMap.of(objectNameColumnHandle, objectNameColumnValue)));
        assertEquals(((JmxTableHandle) result.get().getHandle()).getNodeFilter(), TupleDomain.fromFixedValues(ImmutableMap.of(nodeColumnHandle, nodeColumnValue)));
    }

    @Test
    public void testApplyFilterWithSameConstraint()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));

        JmxColumnHandle columnHandle = new JmxColumnHandle("node", createUnboundedVarcharType());
        TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(columnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice(localNode.getNodeIdentifier()))));

        JmxTableHandle newTableHandle = new JmxTableHandle(handle.getTableName(), handle.getObjectNames(), handle.getColumnHandles(), handle.isLiveData(), nodeTupleDomain);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, newTableHandle, new Constraint(nodeTupleDomain));
        assertFalse(result.isPresent());
    }

    private static Node createTestingNode(String hostname)
    {
        return new InternalNode(hostname, URI.create(format("http://%s:8080", hostname)), NodeVersion.UNKNOWN, false);
    }
}
