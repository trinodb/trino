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

import com.google.inject.Inject;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.RowDecoderSpec;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroRowDecoderFactory
        implements RowDecoderFactory
{
    public static final String NAME = "avro";
    public static final String DATA_SCHEMA = "dataSchema";

    private final AvroReaderSupplier.Factory avroReaderSupplierFactory;
    private final AvroDeserializer.Factory avroDeserializerFactory;

    @Inject
    public AvroRowDecoderFactory(AvroReaderSupplier.Factory avroReaderSupplierFactory, AvroDeserializer.Factory avroDeserializerFactory)
    {
        this.avroReaderSupplierFactory = requireNonNull(avroReaderSupplierFactory, "avroReaderSupplierFactory is null");
        this.avroDeserializerFactory = requireNonNull(avroDeserializerFactory, "avroDeserializerFactory is null");
    }

    @Override
    public RowDecoder create(ConnectorSession session, RowDecoderSpec rowDecoderSpec)
    {
        if (rowDecoderSpec.columns().isEmpty()) {
            // For select count(*)
            return DummyRowDecoderFactory.DECODER_INSTANCE;
        }

        String dataSchema = requireNonNull(rowDecoderSpec.decoderParams().get(DATA_SCHEMA), format("%s cannot be null", DATA_SCHEMA));
        Schema parsedSchema = (new Schema.Parser()).parse(dataSchema);
        if (parsedSchema.getType().equals(Schema.Type.RECORD)) {
            AvroReaderSupplier<GenericRecord> avroReaderSupplier = avroReaderSupplierFactory.create(parsedSchema);
            AvroDeserializer<GenericRecord> dataDecoder = avroDeserializerFactory.create(avroReaderSupplier);
            return new GenericRecordRowDecoder(dataDecoder, rowDecoderSpec.columns());
        }
        AvroReaderSupplier<Object> avroReaderSupplier = avroReaderSupplierFactory.create(parsedSchema);
        AvroDeserializer<Object> dataDecoder = avroDeserializerFactory.create(avroReaderSupplier);
        return new SingleValueRowDecoder(dataDecoder, getOnlyElement(rowDecoderSpec.columns()));
    }
}
