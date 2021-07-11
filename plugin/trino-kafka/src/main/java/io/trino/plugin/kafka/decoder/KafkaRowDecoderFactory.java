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
package io.trino.plugin.kafka.decoder;

import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.avro.AvroDeserializer;
import io.trino.decoder.avro.SingleValueRowDecoder;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.decoder.avro.AvroRowDecoderFactory.DATA_SCHEMA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaRowDecoderFactory
        implements RowDecoderFactory
{
    public static final String NAME = "kafka";

    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private static final IntegerDeserializer INTEGER_DESERIALIZER = new IntegerDeserializer();
    private static final FloatDeserializer FLOAT_DESERIALIZER = new FloatDeserializer();
    private static final DoubleDeserializer DOUBLE_DESERIALIZER = new DoubleDeserializer();
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final ByteBufferDeserializer BYTE_BUFFER_DESERIALIZER = new ByteBufferDeserializer();

    @Override
    public RowDecoder create(Map<String, String> decoderParams, Set<DecoderColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        if (columns.isEmpty()) {
            // For select count(*)
            return DummyRowDecoderFactory.DECODER_INSTANCE;
        }
        String dataSchema = requireNonNull(decoderParams.get(DATA_SCHEMA), format("%s cannot be null", DATA_SCHEMA));
        Schema parsedSchema = (new Schema.Parser()).parse(dataSchema);
        AvroDeserializer<Object> avroDeserializer = null;
        switch (parsedSchema.getType()) {
            case LONG:
                avroDeserializer = data -> LONG_DESERIALIZER.deserialize(null, data);
                break;
            case INT:
                avroDeserializer = data -> INTEGER_DESERIALIZER.deserialize(null, data);
                break;
            case BOOLEAN:
                avroDeserializer = data -> data[0] != 0;
                break;
            case STRING:
                avroDeserializer = data -> STRING_DESERIALIZER.deserialize(null, data);
                break;
            case FLOAT:
                avroDeserializer = data -> FLOAT_DESERIALIZER.deserialize(null, data);
                break;
            case DOUBLE:
                avroDeserializer = data -> DOUBLE_DESERIALIZER.deserialize(null, data);
                break;
            case BYTES:
                avroDeserializer = data -> BYTE_BUFFER_DESERIALIZER.deserialize(null, data);
                break;
            default:
        }
        if (avroDeserializer == null) {
            throw new UnsupportedOperationException(format("Unexpected schema '%s'", parsedSchema));
        }

        return new SingleValueRowDecoder(new NullConvertingAvroDeserializer(avroDeserializer), getOnlyElement(columns));
    }

    private static class NullConvertingAvroDeserializer
            implements AvroDeserializer<Object>
    {
        private final AvroDeserializer<Object> delegate;

        public NullConvertingAvroDeserializer(AvroDeserializer<Object> delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Object deserialize(byte[] data)
        {
            if (data.length == 0) {
                return null;
            }
            return delegate.deserialize(data);
        }
    }
}
