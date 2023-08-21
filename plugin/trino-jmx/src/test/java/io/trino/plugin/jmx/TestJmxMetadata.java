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
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.spi.Node;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static io.trino.plugin.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJmxMetadata
{
    private static final String RUNTIME_OBJECT = "java.lang:type=Runtime";
    private static final String PATTERN = "java.lang:*";
    private static final SchemaTableName RUNTIME_TABLE = new SchemaTableName(JMX_SCHEMA_NAME, RUNTIME_OBJECT.toLowerCase(ENGLISH));
    private static final SchemaTableName RUNTIME_HISTORY_TABLE = new SchemaTableName(HISTORY_SCHEMA_NAME, RUNTIME_OBJECT.toLowerCase(ENGLISH));

    private final Node localNode = createTestingNode("host1");
    private final JmxMetadata metadata = new JmxMetadata(getPlatformMBeanServer(), new JmxHistoricalData(1000, ImmutableSet.of(PATTERN), getPlatformMBeanServer()));

    @Test
    public void testListSchemas()
    {
        assertThat(metadata.listSchemaNames(SESSION)).isEqualTo(ImmutableList.of(JMX_SCHEMA_NAME, HISTORY_SCHEMA_NAME));
    }

    @Test
    public void testListTables()
    {
        assertThat(metadata.listTables(SESSION, Optional.of(JMX_SCHEMA_NAME))).contains(RUNTIME_TABLE);
        assertThat(metadata.listTables(SESSION, Optional.of(HISTORY_SCHEMA_NAME))).contains(RUNTIME_HISTORY_TABLE);
    }

    @Test
    public void testGetTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, RUNTIME_TABLE);
        assertThat(handle.getObjectNames()).isEqualTo(ImmutableList.of(RUNTIME_OBJECT));

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertThat(columns).contains(new JmxColumnHandle("node", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("Name", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("StartTime", BIGINT));
    }

    @Test
    public void testGetTimeTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, RUNTIME_HISTORY_TABLE);
        assertThat(handle.getObjectNames()).isEqualTo(ImmutableList.of(RUNTIME_OBJECT));

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertThat(columns).contains(new JmxColumnHandle("timestamp", createTimestampWithTimeZoneType(3)));
        assertThat(columns).contains(new JmxColumnHandle("node", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("Name", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("StartTime", BIGINT));
    }

    @Test
    public void testGetCumulativeTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));
        assertThat(handle.getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(handle.getObjectNames()).hasSizeGreaterThan(1);

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertThat(columns).contains(new JmxColumnHandle("node", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("object_name", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("Name", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("StartTime", BIGINT));

        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*java.lang:type=Runtime*")).getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*=Runtime")).getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*")).getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*:*")).getObjectNames()).contains(RUNTIME_OBJECT);
    }

    @Test
    public void testGetCumulativeTableHandleForHistorySchema()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(HISTORY_SCHEMA_NAME, PATTERN));
        assertThat(handle.getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(handle.getObjectNames()).hasSizeGreaterThan(1);

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertThat(columns).contains(new JmxColumnHandle("timestamp", createTimestampWithTimeZoneType(3)));
        assertThat(columns).contains(new JmxColumnHandle("node", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("object_name", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("Name", createUnboundedVarcharType()));
        assertThat(columns).contains(new JmxColumnHandle("StartTime", BIGINT));

        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(HISTORY_SCHEMA_NAME, "*java.lang:type=Runtime*")).getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(HISTORY_SCHEMA_NAME, "java.lang:*=Runtime")).getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(HISTORY_SCHEMA_NAME, "*")).getObjectNames()).contains(RUNTIME_OBJECT);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName(HISTORY_SCHEMA_NAME, "*:*")).getObjectNames()).contains(RUNTIME_OBJECT);
    }

    @Test
    public void testApplyFilterWithoutConstraint()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, handle, new Constraint(TupleDomain.all()));

        assertThat(result).isNotPresent();
    }

    @Test
    public void testApplyFilterWithConstraint()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));

        JmxColumnHandle nodeColumnHandle = new JmxColumnHandle("node", createUnboundedVarcharType());
        NullableValue nodeColumnValue = NullableValue.of(createUnboundedVarcharType(), utf8Slice(localNode.getNodeIdentifier()));

        JmxColumnHandle objectNameColumnHandle = new JmxColumnHandle("object_name", createUnboundedVarcharType());
        NullableValue objectNameColumnValue = NullableValue.of(createUnboundedVarcharType(), utf8Slice("trino.memory:type=MemoryPool,name=reserved"));

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(nodeColumnHandle, nodeColumnValue, objectNameColumnHandle, objectNameColumnValue));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, handle, new Constraint(tupleDomain));

        assertThat(result).isPresent();
        assertThat(result.get().getRemainingFilter())
                .isEqualTo(TupleDomain.fromFixedValues(ImmutableMap.of(objectNameColumnHandle, objectNameColumnValue)));
        assertThat(((JmxTableHandle) result.get().getHandle()).getNodeFilter())
                .isEqualTo(TupleDomain.fromFixedValues(ImmutableMap.of(nodeColumnHandle, nodeColumnValue)));
    }

    @Test
    public void testApplyFilterWithSameConstraint()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));

        JmxColumnHandle columnHandle = new JmxColumnHandle("node", createUnboundedVarcharType());
        TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(columnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice(localNode.getNodeIdentifier()))));

        JmxTableHandle newTableHandle = new JmxTableHandle(handle.getTableName(), handle.getObjectNames(), handle.getColumnHandles(), handle.isLiveData(), nodeTupleDomain);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(SESSION, newTableHandle, new Constraint(nodeTupleDomain));
        assertThat(result).isNotPresent();
    }

    private static Node createTestingNode(String hostname)
    {
        return new InternalNode(hostname, URI.create(format("http://%s:8080", hostname)), NodeVersion.UNKNOWN, false);
    }
}
