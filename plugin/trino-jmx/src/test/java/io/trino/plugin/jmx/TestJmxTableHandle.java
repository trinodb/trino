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
import io.airlift.testing.EquivalenceTester;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingNodeManager;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.ObjectNameBuilder;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static io.trino.plugin.jmx.MetadataUtil.TABLE_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestJmxTableHandle
{
    public static final List<JmxColumnHandle> COLUMNS = ImmutableList.<JmxColumnHandle>builder()
            .add(new JmxColumnHandle("id", BIGINT))
            .add(new JmxColumnHandle("name", createUnboundedVarcharType()))
            .build();
    public static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("schema", "tableName");
    public static final JmxColumnHandle columnHandle = new JmxColumnHandle("node", createUnboundedVarcharType());
    public static final TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(columnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice("host1"))));

    @Test
    public void testJsonRoundTrip()
    {
        JmxTableHandle table = new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("objectName"), COLUMNS, true, TupleDomain.all());

        String json = TABLE_CODEC.toJson(table);
        JmxTableHandle copy = TABLE_CODEC.fromJson(json);
        assertEquals(copy, table);
    }

    @Test
    public void testEquivalence()
    {
        List<JmxColumnHandle> singleColumn = ImmutableList.of(COLUMNS.get(0));
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, true, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, true, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, false, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), COLUMNS, false, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, true, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, true, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, false, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("nameX"), COLUMNS, false, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, true, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, true, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, TupleDomain.all()),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, TupleDomain.all()))
                .addEquivalentGroup(
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, nodeTupleDomain),
                        new JmxTableHandle(SCHEMA_TABLE_NAME, ImmutableList.of("name"), singleColumn, false, nodeTupleDomain))
                .check();
    }

    @Test
    public void testObjectNamesInTableHandle()
    {
        TestMBean testMBean = new TestMBean();
        Node localNode = new InternalNode("host1", URI.create(format("http://%s:8080", "host1")), NodeVersion.UNKNOWN, false);
        Set<Node> nodes = ImmutableSet.of(localNode,
                new InternalNode("host2", URI.create(format("http://%s:8080", "host2")), NodeVersion.UNKNOWN, false),
                new InternalNode("host3", URI.create(format("http://%s:8080", "host3")), NodeVersion.UNKNOWN, false));
        NodeManager nodeManager = new TestingNodeManager(localNode, nodes);
        String testMBeanName = new ObjectNameBuilder("trino.plugin.hive.metastore.thrift").withProperties(ImmutableMap.<String, String>builder()
                        .put("type", "thrifthivemetastore")
                        .put("name", "hive")
                        .buildOrThrow())
                .build();
        MBeanExporter mBeanExporter = new MBeanExporter(ManagementFactory.getPlatformMBeanServer());
        mBeanExporter.export(testMBeanName, testMBean);
        JmxConnector jmxConnector = (JmxConnector) new JmxConnectorFactory()
                .create("test-id", ImmutableMap.of(
                                "jmx.dump-tables", "trino.plugin.hive.metastore.thrift:name=hive\\,type=thrifthivemetastore",
                                "jmx.dump-period", format("%dms", 100L),
                                "jmx.max-entries", "1000"),
                        new ConnectorContext()
                        {
                            @Override
                            public NodeManager getNodeManager()
                            {
                                return nodeManager;
                            }
                        });
        JmxMetadata metadata = jmxConnector.getMetadata(SESSION, new ConnectorTransactionHandle() {});
        SchemaTableName schemaTableName = SchemaTableName.schemaTableName(HISTORY_SCHEMA_NAME, "trino.plugin.hive.metastore.thrift:name=hive,type=thrifthivemetastore");
        JmxTableHandle tableHandle = metadata.getTableHandle(SESSION, schemaTableName);
        List<SchemaTableName> schemaTableNameList = metadata.listTables(SESSION, Optional.of(HISTORY_SCHEMA_NAME));
        List<String> tableNameList = schemaTableNameList.stream().map(SchemaTableName::getTableName).toList();
        assertNotNull(tableHandle);
        assertTrue(tableHandle.getObjectNames().stream().anyMatch(tableNameList::contains));
    }

    public static class TestMBean
    {
        private String value;

        @Managed
        public String getValue()
        {
            return value;
        }

        @Managed
        public void setValue(String value)
        {
            this.value = value;
        }
    }
}
