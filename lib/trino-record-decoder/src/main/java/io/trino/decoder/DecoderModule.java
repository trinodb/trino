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
package io.trino.decoder;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.trino.decoder.avro.AvroDecoderModule;
import io.trino.decoder.csv.CsvRowDecoder;
import io.trino.decoder.csv.CsvRowDecoderFactory;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import io.trino.decoder.json.JsonRowDecoder;
import io.trino.decoder.json.JsonRowDecoderFactory;
import io.trino.decoder.protobuf.ProtobufDecoderModule;
import io.trino.decoder.raw.RawRowDecoder;
import io.trino.decoder.raw.RawRowDecoderFactory;

import static com.google.inject.Scopes.SINGLETON;

/**
 * Default decoder module. Installs the registry and all known decoder submodules.
 */
public class DecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        MapBinder<String, RowDecoderFactory> decoderFactoriesByName = MapBinder.newMapBinder(binder, String.class, RowDecoderFactory.class);
        decoderFactoriesByName.addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(CsvRowDecoder.NAME).to(CsvRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(JsonRowDecoder.NAME).to(JsonRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(RawRowDecoder.NAME).to(RawRowDecoderFactory.class).in(SINGLETON);
        binder.install(new AvroDecoderModule());
        binder.install(new ProtobufDecoderModule());
        binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
    }
}
