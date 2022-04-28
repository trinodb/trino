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
package io.trino.decoder.avro;

import com.google.common.collect.ImmutableMap;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class SingleValueRowDecoder
        implements RowDecoder
{
    private final DecoderColumnHandle column;
    private final AvroDeserializer<Object> deserializer;

    public SingleValueRowDecoder(AvroDeserializer<Object> deserializer, DecoderColumnHandle column)
    {
        this.deserializer = requireNonNull(deserializer, "deserializer is null");
        this.column = requireNonNull(column, "columns is null");
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data)
    {
        Object avroValue = deserializer.deserialize(data);
        return Optional.of(ImmutableMap.of(column, new AvroColumnDecoder.ObjectValueProvider(avroValue, column.getType(), column.getName())));
    }
}
