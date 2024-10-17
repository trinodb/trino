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
import io.trino.plugin.pulsar.PulsarAuth;
import io.trino.plugin.pulsar.PulsarColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnMetadata;
import io.trino.plugin.pulsar.PulsarConnectorConfig;
import io.trino.plugin.pulsar.PulsarConnectorId;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.spy;
//import static org.testng.Assert.assertNotNull;

/**
 * Abstract superclass for TestXXDecoder (e.g. TestAvroDecoder 、TestJsonDecoder).
 */
public abstract class AbstractDecoderTester
{
    protected PulsarDispatchingRowDecoderFactory decoderFactory;
    protected PulsarConnectorId pulsarConnectorId = new PulsarConnectorId("test-connector");
    protected SchemaInfo schemaInfo;
    protected TopicName topicName;
    protected List<PulsarColumnHandle> pulsarColumnHandle;
    protected PulsarRowDecoder pulsarRowDecoder;
    protected DecoderTestUtil decoderTestUtil;
    protected PulsarConnectorConfig pulsarConnectorConfig;
    protected PulsarMetadata pulsarMetadata;

    protected void init()
            throws PulsarClientException
    {
        ConnectorContext prestoConnectorContext = new TestingConnectorContext();
        this.decoderFactory = new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager());
        this.pulsarConnectorConfig = spy(PulsarConnectorConfig.class);
        this.pulsarConnectorConfig.setMaxEntryReadBatchSize(1);
        this.pulsarConnectorConfig.setMaxSplitEntryQueueSize(10);
        this.pulsarConnectorConfig.setMaxSplitMessageQueueSize(100);
        this.pulsarMetadata = new PulsarMetadata(pulsarConnectorId, this.pulsarConnectorConfig, decoderFactory,
                new PulsarAuth(this.pulsarConnectorConfig));
        this.topicName = TopicName.get("persistent", NamespaceName.get("tenant-1", "ns-1"), "topic-1");
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

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, BigDecimal value)
    {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected Block getBlock(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle)
    {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        return (Block) provider.getObject();
    }

    protected List<PulsarColumnHandle> getColumnColumnHandles(TopicName topicName, SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType, boolean includeInternalColumn, PulsarDispatchingRowDecoderFactory dispatchingRowDecoderFactory)
    {
        List<PulsarColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadata = pulsarMetadata.getPulsarColumns(topicName, schemaInfo, includeInternalColumn, handleKeyValueType);

        columnMetadata.forEach(column ->
        {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) column;
            columnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(), pulsarColumnMetadata.getNameWithCase(), pulsarColumnMetadata.getType(), pulsarColumnMetadata.getDecoderExtraInfo().getMapping(), pulsarColumnMetadata.getDecoderExtraInfo().getDataFormat(), pulsarColumnMetadata.getDecoderExtraInfo().getFormatHint(), pulsarColumnMetadata.isHidden(), false, pulsarColumnMetadata.isInternal(), pulsarColumnMetadata.getHandleKeyValueType()));
        });
        return columnHandles;
    }
}
