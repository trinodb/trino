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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TypeManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.lang.String.format;

/**
 * dispatcher RowDecoderFactory for {@link org.apache.pulsar.common.schema.SchemaType}.
 */
//@Slf4j
public class PulsarDispatchingRowDecoderFactory {
    private final Function<SchemaType, PulsarRowDecoderFactory> decoderFactories;
    private final TypeManager typeManager;

    @Inject
    public PulsarDispatchingRowDecoderFactory(TypeManager typeManager) {
        this.typeManager = typeManager;

        final PulsarRowDecoderFactory avro = new PulsarAvroRowDecoderFactory(typeManager);
        final PulsarRowDecoderFactory json = new PulsarJsonRowDecoderFactory(typeManager);
        final PulsarRowDecoderFactory protobufNative = new PulsarProtobufNativeRowDecoderFactory(typeManager);
        final PulsarRowDecoderFactory primitive = new PulsarPrimitiveRowDecoderFactory();
        this.decoderFactories = (schema) -> {
            if (SchemaType.AVRO.equals(schema)) {
                return avro;
            } else if (SchemaType.JSON.equals(schema)) {
                return json;
            } else if (SchemaType.PROTOBUF_NATIVE.equals(schema)) {
                return protobufNative;
            } else if (schema.isPrimitive()) {
                return primitive;
            } else {
                return null;
            }
        };
    }

    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo,
                                             Set<DecoderColumnHandle> columns) {
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.createRowDecoder(topicName, schemaInfo, columns);
    }

    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo,
                                                      PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.extractColumnMetadata(topicName, schemaInfo, handleKeyValueType);
    }

    private PulsarRowDecoderFactory createDecoderFactory(SchemaInfo schemaInfo) {
        PulsarRowDecoderFactory decoderFactory = decoderFactories.apply(schemaInfo.getType());
        if (decoderFactory == null) {
            throw new RuntimeException(format("'%s' is unsupported type '%s'",
                    schemaInfo.getName(), schemaInfo.getType()));
        }
        return decoderFactory;
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }
}
