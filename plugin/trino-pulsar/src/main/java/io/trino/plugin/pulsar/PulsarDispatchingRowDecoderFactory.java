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

import com.google.inject.Inject;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.plugin.pulsar.decoder.avro.PulsarAvroRowDecoderFactory;
import io.trino.plugin.pulsar.decoder.json.PulsarJsonRowDecoderFactory;
import io.trino.plugin.pulsar.decoder.primitive.PulsarPrimitiveRowDecoderFactory;
import io.trino.plugin.pulsar.decoder.protobufnative.PulsarProtobufNativeRowDecoderFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TypeManager;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_SCHEMA_ERROR;
import static java.lang.String.format;

/**
 * Dispatcher RowDecoderFactory for {@link SchemaType}.
 */
public class PulsarDispatchingRowDecoderFactory
{
    private TypeManager typeManager;

    @Inject
    public PulsarDispatchingRowDecoderFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns)
    {
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.createRowDecoder(topicName, schemaInfo, columns);
    }

    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType)
    {
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.extractColumnMetadata(topicName, schemaInfo, handleKeyValueType);
    }

    private PulsarRowDecoderFactory createDecoderFactory(SchemaInfo schemaInfo)
    {
        if (SchemaType.AVRO.equals(schemaInfo.getType())) {
            return new PulsarAvroRowDecoderFactory(typeManager);
        }
        else if (SchemaType.JSON.equals(schemaInfo.getType())) {
            return new PulsarJsonRowDecoderFactory(typeManager);
        }
        else if (SchemaType.PROTOBUF_NATIVE.equals(schemaInfo.getType())) {
            return new PulsarProtobufNativeRowDecoderFactory(typeManager);
        }
        else if (schemaInfo.getType().isPrimitive()) {
            return new PulsarPrimitiveRowDecoderFactory();
        }
        else {
            throw new TrinoException(PULSAR_SCHEMA_ERROR, format("'%s' is unsupported type '%s'", schemaInfo.getName(), schemaInfo.getType()), null);
        }
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }
}
