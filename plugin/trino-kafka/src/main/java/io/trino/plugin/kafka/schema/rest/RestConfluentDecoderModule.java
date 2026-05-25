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
package io.trino.plugin.kafka.schema.rest;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.avro.AvroBytesDeserializer;
import io.trino.decoder.avro.AvroDeserializer;
import io.trino.decoder.avro.AvroReaderSupplier;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.decoder.csv.CsvRowDecoder;
import io.trino.decoder.csv.CsvRowDecoderFactory;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import io.trino.decoder.json.JsonRowDecoder;
import io.trino.decoder.json.JsonRowDecoderFactory;
import io.trino.decoder.protobuf.ProtobufDecoderModule;
import io.trino.decoder.raw.RawRowDecoder;
import io.trino.decoder.raw.RawRowDecoderFactory;
import io.trino.plugin.kafka.schema.confluent.ConfluentAvroReaderSupplier;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;

/**
 * Decoder module that uses Confluent Schema Registry Avro decoder (magic byte + schema ID + bytes)
 * instead of the default raw-Avro decoder. Use when kafka.confluent-schema-registry-url is set
 * so that messages written in Confluent wire format can be read back.
 */
public class RestConfluentDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        com.google.inject.multibindings.MapBinder<String, RowDecoderFactory> mapBinder =
                newMapBinder(binder, String.class, RowDecoderFactory.class);
        mapBinder.addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
        mapBinder.addBinding(CsvRowDecoder.NAME).to(CsvRowDecoderFactory.class).in(SINGLETON);
        mapBinder.addBinding(JsonRowDecoder.NAME).to(JsonRowDecoderFactory.class).in(SINGLETON);
        mapBinder.addBinding(RawRowDecoder.NAME).to(RawRowDecoderFactory.class).in(SINGLETON);
        mapBinder.addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(SINGLETON);

        binder.bind(AvroReaderSupplier.Factory.class).to(ConfluentAvroReaderSupplier.Factory.class).in(SINGLETON);
        binder.bind(AvroDeserializer.Factory.class).to(AvroBytesDeserializer.Factory.class).in(SINGLETON);

        binder.install(new ProtobufDecoderModule());
        binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
    }
}
