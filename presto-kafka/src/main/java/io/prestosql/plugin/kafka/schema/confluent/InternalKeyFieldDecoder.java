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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.RowDecoderFactory;
import io.prestosql.decoder.avro.AvroColumnDecoder;
import io.prestosql.decoder.avro.AvroRowDecoderFactory;
import io.prestosql.decoder.dummy.DummyRowDecoderFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.kafka.schema.confluent.ConfluentInternalFields.KEY_SCHEMA_ID;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class InternalKeyFieldDecoder
        implements RowDecoder
{
    private final DecoderColumnHandle keySchemaIdHandle;

    public InternalKeyFieldDecoder(DecoderColumnHandle keySchemaIdHandle)
    {
        this.keySchemaIdHandle = requireNonNull(keySchemaIdHandle, "keySchemaIdHandle is null");
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Long schemaId = null;
        if (data.length >= 5 && buffer.get() == 0) {
            schemaId = Long.valueOf(buffer.getInt());
        }

        return Optional.of(ImmutableMap.<DecoderColumnHandle, FieldValueProvider>builder()
                .put(keySchemaIdHandle, new AvroColumnDecoder.ObjectValueProvider(schemaId, BIGINT, keySchemaIdHandle.getName()))
                .build());
    }

    public static class Factory
            implements RowDecoderFactory
    {
        @Override
        public RowDecoder create(Map<String, String> decoderParams, Set<DecoderColumnHandle> columns)
        {
            requireNonNull(columns, "columns is null");
            String dataFormat = requireNonNull(decoderParams.get("dataFormat"), "Missing required parameter 'dataFormat'");
            if (columns.isEmpty() || !dataFormat.equals(AvroRowDecoderFactory.NAME)) {
                return DummyRowDecoderFactory.DECODER_INSTANCE;
            }
            Optional<DecoderColumnHandle> keySchemaIdHandle = columns.stream().filter(handle -> handle.getName().equals(KEY_SCHEMA_ID)).findFirst();
            checkState(keySchemaIdHandle.isPresent(), "Internal field '%s' not found", KEY_SCHEMA_ID);
            return new InternalKeyFieldDecoder(keySchemaIdHandle.get());
        }
    }
}
