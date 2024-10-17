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
package io.trino.plugin.pulsar.decoder.protobufnative;

import com.google.protobuf.DynamicMessage;
import io.netty.buffer.ByteBuf;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.spi.TrinoException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Pulsar {@link org.apache.pulsar.common.schema.SchemaType#PROTOBUF_NATIVE} RowDecoder.
 */
public class PulsarProtobufNativeRowDecoder
            implements PulsarRowDecoder
{
    private final GenericProtobufNativeSchema genericProtobufNativeSchema;
    private final Map<DecoderColumnHandle, PulsarProtobufNativeColumnDecoder> columnDecoders;

    public PulsarProtobufNativeRowDecoder(GenericProtobufNativeSchema genericProtobufNativeSchema, Set<DecoderColumnHandle> columns)
    {
        this.genericProtobufNativeSchema = requireNonNull(genericProtobufNativeSchema, "genericProtobufNativeSchema is null");
        columnDecoders = columns.stream().collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private PulsarProtobufNativeColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new PulsarProtobufNativeColumnDecoder(columnHandle);
    }

    /**
     * Decode ByteBuf by {@link org.apache.pulsar.client.api.schema.GenericSchema}.
     */
    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf)
    {
        DynamicMessage dynamicMessage;
        try {
            GenericProtobufNativeRecord record = (GenericProtobufNativeRecord) genericProtobufNativeSchema.decode(byteBuf.array());
            dynamicMessage = record.getProtobufRecord();
        }
        catch (SchemaSerializationException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Decoding protobuf record failed.", e);
        }
        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().decodeField(dynamicMessage))));
    }
}
