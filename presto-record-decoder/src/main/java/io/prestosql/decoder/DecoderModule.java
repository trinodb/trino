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
package io.prestosql.decoder;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.decoder.avro.AvroRowDecoder;
import io.prestosql.decoder.avro.AvroRowDecoderFactory;
import io.prestosql.decoder.csv.CsvRowDecoder;
import io.prestosql.decoder.csv.CsvRowDecoderFactory;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.decoder.dummy.DummyRowDecoderFactory;
import io.prestosql.decoder.json.JsonRowDecoder;
import io.prestosql.decoder.json.JsonRowDecoderFactory;
import io.prestosql.decoder.raw.RawRowDecoder;
import io.prestosql.decoder.raw.RawRowDecoderFactory;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;

/**
 * Default decoder module. Installs the registry and all known decoder submodules.
 */
public class DecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindDecoderFactory(binder, DummyRowDecoder.NAME, DummyRowDecoderFactory.class);
        bindDecoderFactory(binder, CsvRowDecoder.NAME, CsvRowDecoderFactory.class);
        bindDecoderFactory(binder, JsonRowDecoder.NAME, JsonRowDecoderFactory.class);
        bindDecoderFactory(binder, RawRowDecoder.NAME, RawRowDecoderFactory.class);
        bindDecoderFactory(binder, AvroRowDecoder.NAME, AvroRowDecoderFactory.class);

        binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
    }

    public static MapBinder<String, RowDecoderFactory> decoderFactoriesByName(Binder binder)
    {
        return newMapBinder(binder, String.class, RowDecoderFactory.class);
    }

    public static void bindDecoderFactory(Binder binder, String name, Class<? extends RowDecoderFactory> decoderFactory)
    {
        decoderFactoriesByName(binder).addBinding(name).to(decoderFactory).in(Scopes.SINGLETON);
    }
}
