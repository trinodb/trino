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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.decoder.RowDecoderFactory;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class AvroDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newMapBinder(binder, String.class, AvroReaderSupplierFactory.class).addBinding(FixedSchemaAvroReaderSupplier.NAME).to(FixedSchemaAvroReaderSupplier.Factory.class).in(Scopes.SINGLETON);
        MapBinder<String, AvroDataDecoderFactory> avroRowDecoderFactoriesByName = newMapBinder(binder, String.class, AvroDataDecoderFactory.class);
        avroRowDecoderFactoriesByName.addBinding(AvroFileDecoder.NAME).to(AvroFileDecoder.Factory.class).in(Scopes.SINGLETON);
        avroRowDecoderFactoriesByName.addBinding(AvroBytesDecoder.NAME).to(AvroBytesDecoder.Factory.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(Scopes.SINGLETON);
    }
}
