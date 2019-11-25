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
package io.prestosql.decoder.avro;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AvroRowDecoder
        implements RowDecoder
{
    public static final String NAME = "avro";
    private final AvroRecordReader avroRecordReader;
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;

    public AvroRowDecoder(AvroRecordReader avroRecordReader,
        Set<DecoderColumnHandle> columns)
    {
        this.avroRecordReader = avroRecordReader;
        requireNonNull(columns, "columns is null");
        columnDecoders = columns.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new AvroColumnDecoder(columnHandle);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(avroRecordReader.read(data)))));
    }

    private void closeQuietly(DataFileStream<GenericRecord> stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        }
        catch (IOException ignored) {
        }
    }
}
