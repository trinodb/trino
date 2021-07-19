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
package io.trino.plugin.pulsar;

import com.google.common.base.Strings;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.testing.TestingConnectorSession;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestPulsarMetadata
        extends TestPulsarConnector
{
    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListSchemaNames(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<String> schemas = pulsarMetadata.listSchemaNames(TestingConnectorSession.SESSION);

        if (Strings.nullToEmpty(delimiter).trim().isEmpty()) {
            String[] expectedSchemas = {NAMESPACE_NAME_1.toString(), NAMESPACE_NAME_2.toString(),
                    NAMESPACE_NAME_3.toString(), NAMESPACE_NAME_4.toString()};
            assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
        }
        else {
            String[] expectedSchemas = {
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_1.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_2.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_3.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_4.toString(), pulsarConnectorConfig)};
            assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableHandle(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaTableName schemaTableName = new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName());

        ConnectorTableHandle connectorTableHandle
                = pulsarMetadata.getTableHandle(TestingConnectorSession.SESSION, schemaTableName);

        assertTrue(connectorTableHandle instanceof PulsarTableHandle);

        PulsarTableHandle pulsarTableHandle = (PulsarTableHandle) connectorTableHandle;

        assertEquals(pulsarTableHandle.getConnectorId(), pulsarConnectorId.toString());
        assertEquals(pulsarTableHandle.getSchemaName(), TOPIC_1.getNamespace());
        assertEquals(pulsarTableHandle.getTableName(), TOPIC_1.getLocalName());
        assertEquals(pulsarTableHandle.getTopicName(), TOPIC_1.getLocalName());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadata(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<TopicName> allTopics = new LinkedList<>();
        allTopics.addAll(topicNames.stream().filter(topicName -> !topicName.equals(NON_SCHEMA_TOPIC)).collect(Collectors.toList()));
        allTopics.addAll(partitionedTopicNames);

        for (TopicName topic : allTopics) {
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                    topic.toString(),
                    topic.getNamespace(),
                    topic.getLocalName(),
                    topic.getLocalName());

            List<PulsarColumnHandle> fooColumnHandles = topicsToColumnHandles.get(topic);

            ConnectorTableMetadata tableMetadata = pulsarMetadata.getTableMetadata(TestingConnectorSession.SESSION,
                    pulsarTableHandle);

            assertEquals(tableMetadata.getTable().getSchemaName(), topic.getNamespace());
            assertEquals(tableMetadata.getTable().getTableName(), topic.getLocalName());
            assertEquals(tableMetadata.getColumns().size(),
                    fooColumnHandles.size());

            List<String> fieldNames = new LinkedList<>(fooFieldNames);

            for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
                fieldNames.add(internalField.getName());
            }

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                    assertEquals(column.getComment(),
                            PulsarInternalColumn.getInternalFieldsMap()
                                    .get(column.getName()).getColumnMetadata(true).getComment());
                }

                fieldNames.remove(column.getName());
            }

            assertTrue(fieldNames.isEmpty());
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataWrongSchema(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                "wrong-tenant/wrong-ns",
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName());

        try {
            pulsarMetadata.getTableMetadata(TestingConnectorSession.SESSION,
                    pulsarTableHandle);
            fail("Invalid schema should have generated an exception");
        }
        catch (TrinoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(e.getMessage(), "Schema wrong-tenant/wrong-ns does not exist");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataWrongTable(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                "wrong-topic",
                "wrong-topic");

        try {
            pulsarMetadata.getTableMetadata(TestingConnectorSession.SESSION,
                    pulsarTableHandle);
            fail("Invalid table should have generated an exception");
        }
        catch (TableNotFoundException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(e.getMessage(), "Table 'tenant-1/ns-1.wrong-topic' not found");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableNoSchema(String delimiter) throws PulsarAdminException
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaInfo schemaInfo = topicsToSchemas.remove(TOPIC_1.getSchemaName());
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName());

        ConnectorTableMetadata tableMetadata = pulsarMetadata.getTableMetadata(TestingConnectorSession.SESSION, pulsarTableHandle);
        assertEquals(tableMetadata.getColumns().size(), PulsarInternalColumn.getInternalFields().size() + 1);
        topicsToSchemas.put(TOPIC_1.getSchemaName(), schemaInfo);
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableBlankSchema(String delimiter) throws PulsarAdminException
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaInfoImpl badSchemaInfo = new SchemaInfoImpl();
        badSchemaInfo.setSchema(new byte[0]);
        badSchemaInfo.setType(SchemaType.AVRO);
        SchemaInfo oldSchemaInfo = topicsToSchemas.remove(TOPIC_1.getSchemaName());
        topicsToSchemas.put(TOPIC_1.getSchemaName(), badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName());

        try {
            pulsarMetadata.getTableMetadata(TestingConnectorSession.SESSION,
                    pulsarTableHandle);
            fail("Table without schema should have generated an exception");
        }
        catch (TrinoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(),
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
        topicsToSchemas.put(TOPIC_1.getSchemaName(), oldSchemaInfo);
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableInvalidSchema(String delimiter) throws PulsarAdminException
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaInfoImpl badSchemaInfo = new SchemaInfoImpl();
        badSchemaInfo.setSchema("foo".getBytes(StandardCharsets.ISO_8859_1));
        badSchemaInfo.setType(SchemaType.AVRO);
        SchemaInfo oldSchemaInfo = topicsToSchemas.remove(TOPIC_1.getSchemaName());
        topicsToSchemas.put(TOPIC_1.getSchemaName(), badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName());

        try {
            pulsarMetadata.getTableMetadata(TestingConnectorSession.SESSION,
                    pulsarTableHandle);
            fail("Table without schema should have generated an exception");
        }
        catch (TrinoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(),
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
        topicsToSchemas.put(TOPIC_1.getSchemaName(), oldSchemaInfo);
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListTable(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        assertTrue(pulsarMetadata.listTables(TestingConnectorSession.SESSION, Optional.empty()).isEmpty());
        assertTrue(pulsarMetadata.listTables(TestingConnectorSession.SESSION, Optional.of("wrong-tenant/wrong-ns"))
                .isEmpty());

        SchemaTableName[] expectedTopics1 =
        {
            new SchemaTableName(
            TOPIC_4.getNamespace(), TOPIC_4.getLocalName()),
            new SchemaTableName(PARTITIONED_TOPIC_4.getNamespace(), PARTITIONED_TOPIC_4.getLocalName())
        };
        assertEquals(pulsarMetadata.listTables(TestingConnectorSession.SESSION,
                Optional.of(NAMESPACE_NAME_3.toString())), Arrays.asList(expectedTopics1));

        SchemaTableName[] expectedTopics2 = {new SchemaTableName(TOPIC_5.getNamespace(), TOPIC_5.getLocalName()),
                new SchemaTableName(TOPIC_6.getNamespace(), TOPIC_6.getLocalName()),
                new SchemaTableName(PARTITIONED_TOPIC_5.getNamespace(), PARTITIONED_TOPIC_5.getLocalName()),
                new SchemaTableName(PARTITIONED_TOPIC_6.getNamespace(), PARTITIONED_TOPIC_6.getLocalName()),
        };
        assertEquals(new HashSet<>(pulsarMetadata.listTables(TestingConnectorSession.SESSION,
                Optional.of(NAMESPACE_NAME_4.toString()))), new HashSet<>(Arrays.asList(expectedTopics2)));
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetColumnHandles(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(), TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(), TOPIC_1.getLocalName());
        Map<String, ColumnHandle> columnHandleMap
                = new HashMap<>(pulsarMetadata.getColumnHandles(TestingConnectorSession.SESSION, pulsarTableHandle));

        List<String> fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (String field : fieldNames) {
            assertNotNull(columnHandleMap.get(field));
            PulsarColumnHandle pulsarColumnHandle = (PulsarColumnHandle) columnHandleMap.get(field);
            PulsarInternalColumn pulsarInternalColumn = PulsarInternalColumn.getInternalFieldsMap().get(field);
            if (pulsarInternalColumn != null) {
                assertEquals(pulsarColumnHandle,
                        pulsarInternalColumn.getColumnHandle(pulsarConnectorId.toString(), false));
            }
            else {
                assertEquals(pulsarColumnHandle.getConnectorId(), pulsarConnectorId.toString());
                assertEquals(pulsarColumnHandle.getName(), field);
                assertFalse(pulsarColumnHandle.isHidden());
            }
            columnHandleMap.remove(field);
        }
        assertTrue(columnHandleMap.isEmpty());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListTableColumns(String delimiter)
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        Map<SchemaTableName, List<ColumnMetadata>> tableColumnsMap
                = pulsarMetadata.listTableColumns(TestingConnectorSession.SESSION,
                    new SchemaTablePrefix(TOPIC_1.getNamespace()));

        assertEquals(tableColumnsMap.size(), 4);
        List<ColumnMetadata> columnMetadataList
                = tableColumnsMap.get(new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
                topicsToColumnHandles.get(TOPIC_1).size());

        List<String> fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        assertTrue(fieldNames.isEmpty());

        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_2.getNamespace(), TOPIC_2.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
                topicsToColumnHandles.get(TOPIC_2).size());

        fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        assertTrue(fieldNames.isEmpty());

        // test table and schema
        tableColumnsMap
                = this.pulsarMetadata.listTableColumns(TestingConnectorSession.SESSION,
                    new SchemaTablePrefix(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));

        assertEquals(tableColumnsMap.size(), 1);
        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
                topicsToColumnHandles.get(TOPIC_4).size());

        fieldNames = new LinkedList<>(fooFieldNames);

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
                assertEquals(column.getComment(),
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

        assertTrue(fieldNames.isEmpty());
    }
}
