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

import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.RowDecoderFactory;
import io.prestosql.decoder.dummy.DummyRowDecoderFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import javax.inject.Inject;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroRowDecoderFactory
        implements RowDecoderFactory
{
    public static final String NAME = "avro";
    public static final String DATA_SCHEMA = "dataSchema";
    public static final String DATA_ENCODING = "dataEncoding";
    public static final String AVRO_READER = "avroReader";

    private final AvroReaderSupplierFactory avroReaderSupplierFactory;
    private final AvroDataDecoderFactory avroDataDecoderFactory;

    @Inject
    public AvroRowDecoderFactory(Map<String, AvroReaderSupplierFactory> avroReaderSupplierFactories, Map<String, AvroDataDecoderFactory> avroDataDecoderFactories)
    {
        requireNonNull(avroReaderSupplierFactories, "avroReaderSupplierFactories is null");
        requireNonNull(avroDataDecoderFactories, "avroDataReaderFactories is null");
        this.avroReaderSupplierFactory = new DispatchingAvroReaderSupplierFactory(avroReaderSupplierFactories);
        this.avroDataDecoderFactory = new DispatchingAvroDataDecoderFactory(avroDataDecoderFactories);
    }

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
        if (parsedSchema.getType().equals(Schema.Type.RECORD)) {
            AvroReaderSupplier<GenericRecord> avroReaderSupplier = avroReaderSupplierFactory.create(decoderParams, parsedSchema);
            AvroDataDecoder<GenericRecord> dataDecoder = avroDataDecoderFactory.create(decoderParams, avroReaderSupplier);
            return new GenericRecordRowDecoder(dataDecoder, columns);
        }
        else {
            AvroReaderSupplier<Object> avroReaderSupplier = avroReaderSupplierFactory.create(decoderParams, parsedSchema);
            AvroDataDecoder<Object> dataDecoder = avroDataDecoderFactory.create(decoderParams, avroReaderSupplier);
            return new SingleValueRowDecoder(dataDecoder, getOnlyElement(columns));
        }
    }

    private static class GenericRecordRowDecoder
            implements RowDecoder
    {
        private List<Map.Entry<DecoderColumnHandle, AvroColumnDecoder>> columnDecoders;
        private final AvroDataDecoder<GenericRecord> dataDecoder;

        public GenericRecordRowDecoder(AvroDataDecoder<GenericRecord> dataDecoder, Set<DecoderColumnHandle> columns)
        {
            this.dataDecoder = requireNonNull(dataDecoder, "dataReader is null");
            requireNonNull(columns, "columns is null");
            this.columnDecoders = columns.stream()
                    .map(column -> new AbstractMap.SimpleImmutableEntry<>(column, new AvroColumnDecoder(column)))
                    .collect(toImmutableList());
        }

        @Override
        public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
        {
            GenericRecord avroRecord = dataDecoder.read(data);
            return Optional.of(columnDecoders.stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().decodeField(avroRecord))));
        }
    }

    private static class SingleValueRowDecoder
            implements RowDecoder
    {
        private final DecoderColumnHandle column;
        private final AvroDataDecoder<Object> dataDecoder;

        public SingleValueRowDecoder(AvroDataDecoder<Object> dataDecoder, DecoderColumnHandle column)
        {
            this.dataDecoder = requireNonNull(dataDecoder, "dataReader is null");
            this.column = requireNonNull(column, "columns is null");
        }

        @Override
        public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
        {
            Object avroValue = dataDecoder.read(data);
            return Optional.of(ImmutableMap.of(column, new AvroColumnDecoder.ObjectValueProvider(avroValue, column.getType(), column.getName())));
        }
    }

    private static class DispatchingAvroDataDecoderFactory
            implements AvroDataDecoderFactory
    {
        // For backwards compatibility
        private static final String DEFAULT_DATA_ENCODING = AvroFileDecoder.NAME;

        private final Map<String, AvroDataDecoderFactory> factories;

        public DispatchingAvroDataDecoderFactory(Map<String, AvroDataDecoderFactory> factories)
        {
            this.factories = requireNonNull(factories, "factories is null");
        }

        @Override
        public <T> AvroDataDecoder<T> create(Map<String, String> decoderParams, AvroReaderSupplier<T> avroReaderSupplier)
        {
            String dataEncoding = decoderParams.getOrDefault(DATA_ENCODING, DEFAULT_DATA_ENCODING);
            AvroDataDecoderFactory factory = requireNonNull(factories.get(dataEncoding), format("%s Factory '%s' not found", DATA_ENCODING, dataEncoding));
            return factory.create(decoderParams, avroReaderSupplier);
        }
    }

    private static class DispatchingAvroReaderSupplierFactory
            implements AvroReaderSupplierFactory
    {
        // For backwards compatibility
        private static final String DEFAULT_AVRO_READER = FixedSchemaAvroReaderSupplier.NAME;

        private final Map<String, AvroReaderSupplierFactory> factories;

        public DispatchingAvroReaderSupplierFactory(Map<String, AvroReaderSupplierFactory> factories)
        {
            this.factories = requireNonNull(factories, "factories is null");
        }

        @Override
        public <T> AvroReaderSupplier<T> create(Map<String, String> decoderParams, Schema schema)
        {
            String avroReaderName = decoderParams.getOrDefault(AVRO_READER, DEFAULT_AVRO_READER);
            AvroReaderSupplierFactory factory = requireNonNull(factories.get(avroReaderName), format("%s Factory '%s' not found", AVRO_READER, avroReaderName));
            return factory.create(decoderParams, schema);
        }
    }
}
