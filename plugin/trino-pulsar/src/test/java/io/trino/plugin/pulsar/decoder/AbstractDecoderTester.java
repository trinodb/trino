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
package io.trino.plugin.pulsar.decoder;

import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.pulsar.PulsarColumnHandle;
import io.trino.plugin.pulsar.PulsarConnectorConfig;
import io.trino.plugin.pulsar.PulsarDispatchingRowDecoderFactory;
import io.trino.plugin.pulsar.PulsarMetadata;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorContext;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_DATA_FORMAT;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_FORMAT_HINT;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_HANDLE_TYPE;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_INTERNAL;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_MAPPING;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_NAME_CASE_SENSITIVE;
import static org.testng.Assert.assertNotNull;

/**
 * Abstract superclass for TestXXDecoder (e.g. TestAvroDecoder 、TestJsonDecoder).
 */
public abstract class AbstractDecoderTester
{
    protected PulsarDispatchingRowDecoderFactory decoderFactory;
    protected CatalogName catalogName = new CatalogName("test-connector");
    protected SchemaInfo schemaInfo;
    protected TopicName topicName;
    protected List<PulsarColumnHandle> pulsarColumnHandle;
    protected PulsarRowDecoder pulsarRowDecoder;
    protected DecoderTestUtil decoderTestUtil;
    protected PulsarConnectorConfig pulsarConnectorConfig;
    protected PulsarMetadata pulsarMetadata;

    protected void init() throws PulsarClientException
    {
        ConnectorContext trinoConnectorContext = new TestingConnectorContext();
        decoderFactory = new PulsarDispatchingRowDecoderFactory(trinoConnectorContext.getTypeManager());
        pulsarConnectorConfig = new PulsarConnectorConfig();
        pulsarConnectorConfig.setMaxEntryReadBatchSize(1);
        pulsarConnectorConfig.setMaxSplitEntryQueueSize(10);
        pulsarConnectorConfig.setMaxSplitMessageQueueSize(100);
        pulsarConnectorConfig.setWebServiceUrl("http://localhost:8080");
        pulsarConnectorConfig.setZookeeperUri("localhost:2181");
        pulsarMetadata = new PulsarMetadata(catalogName, pulsarConnectorConfig, decoderFactory);
        topicName = TopicName.get("persistent", NamespaceName.get("tenant-1", "ns-1"), "topic-1");
    }

    protected void checkArrayValues(Block block, Type type, Object value)
    {
        decoderTestUtil.checkArrayValues(block, type, value);
    }

    protected void checkMapValues(Block block, Type type, Object value)
    {
        decoderTestUtil.checkMapValues(block, type, value);
    }

    protected void checkRowValues(Block block, Type type, Object value)
    {
        decoderTestUtil.checkRowValues(block, type, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, Slice value)
    {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, String value)
    {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long value)
    {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, double value)
    {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, boolean value)
    {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected Block getBlock(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        return provider.getBlock();
    }

    protected List<PulsarColumnHandle> getColumnColumnHandles(TopicName topicName, SchemaInfo schemaInfo,
                                                              PulsarColumnHandle.HandleKeyValueType handleKeyValueType, boolean includeInternalColumn, PulsarDispatchingRowDecoderFactory dispatchingRowDecoderFactory)
    {
        List<PulsarColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadata = pulsarMetadata.getPulsarColumns(topicName, schemaInfo,
                includeInternalColumn, true, handleKeyValueType);

        columnMetadata.forEach(columnMeta -> {
            columnHandles.add(new PulsarColumnHandle(
                    catalogName.toString(),
                    (String) columnMeta.getProperties().get(PROPERTY_KEY_NAME_CASE_SENSITIVE),
                    columnMeta.getType(),
                    columnMeta.isHidden(),
                    columnMeta.getProperties().containsKey(PROPERTY_KEY_INTERNAL) ? (Boolean) columnMeta.getProperties().get(PROPERTY_KEY_INTERNAL) : false,
                    (String) columnMeta.getProperties().get(PROPERTY_KEY_MAPPING),
                    (String) columnMeta.getProperties().get(PROPERTY_KEY_DATA_FORMAT),
                    (String) columnMeta.getProperties().get(PROPERTY_KEY_FORMAT_HINT),
                    Optional.of((PulsarColumnHandle.HandleKeyValueType) columnMeta.getProperties().get(PROPERTY_KEY_HANDLE_TYPE))));
        });
        return columnHandles;
    }
}
