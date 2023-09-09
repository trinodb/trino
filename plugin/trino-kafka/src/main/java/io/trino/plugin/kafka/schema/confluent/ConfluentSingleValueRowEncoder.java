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
package io.trino.plugin.kafka.schema.confluent;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

public class ConfluentSingleValueRowEncoder
        extends AbstractConfluentRowEncoder
{
    private static final ThreadLocal<BinaryEncoder> reuseEncoder = ThreadLocal.withInitial(() -> null);

    private Object singleValue;

    public ConfluentSingleValueRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema schema, KafkaAvroSerializer kafkaAvroSerializer, String topic)
    {
        super(session, columnHandles, schema, kafkaAvroSerializer, topic);
        checkState(schema.getType() != Schema.Type.RECORD, "Unexpected RECORD type for single value schema");
        checkState(columnHandles.size() == 1, "Unexpected size for single value field");
    }

    @Override
    protected void addValue(Block block, int position)
    {
        Type type = getOnlyElement(columnHandles).getType();
        singleValue = getValue(type, block, position, schema);
    }

    @Override
    protected Object getValue(Type type, Block block, int position, Schema fieldSchema)
    {
        Object value = super.getValue(type, block, position, schema);
        if (type instanceof VarbinaryType) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, reuseEncoder.get());
            reuseEncoder.set(encoder);
            try {
                encoder.writeBytes((ByteBuffer) value);
                encoder.flush();
                return outputStream.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return value;
    }

    @Override
    protected Object buildRow()
    {
        Object value = singleValue;
        singleValue = null;
        return value;
    }
}
