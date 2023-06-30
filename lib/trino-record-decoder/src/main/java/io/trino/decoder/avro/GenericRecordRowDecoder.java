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

import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import org.apache.avro.generic.GenericRecord;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

class GenericRecordRowDecoder
        implements RowDecoder
{
    private List<Map.Entry<DecoderColumnHandle, AvroColumnDecoder>> columnDecoders;
    private final AvroDeserializer<GenericRecord> deserializer;

    public GenericRecordRowDecoder(AvroDeserializer<GenericRecord> deserializer, Set<DecoderColumnHandle> columns)
    {
        this.deserializer = requireNonNull(deserializer, "deserializer is null");
        requireNonNull(columns, "columns is null");
        this.columnDecoders = columns.stream()
                .map(column -> new AbstractMap.SimpleImmutableEntry<>(column, new AvroColumnDecoder(column)))
                .collect(toImmutableList());
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data)
    {
        GenericRecord avroRecord;
        try {
            avroRecord = deserializer.deserialize(data);
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
        return Optional.of(columnDecoders.stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().decodeField(avroRecord))));
    }
}
