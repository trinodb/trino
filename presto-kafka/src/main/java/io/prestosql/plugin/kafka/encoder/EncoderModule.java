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
package io.prestosql.plugin.kafka.encoder;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.plugin.kafka.encoder.avro.AvroRowEncoder;
import io.prestosql.plugin.kafka.encoder.avro.AvroRowEncoderFactory;
import io.prestosql.plugin.kafka.encoder.csv.CsvRowEncoder;
import io.prestosql.plugin.kafka.encoder.csv.CsvRowEncoderFactory;
import io.prestosql.plugin.kafka.encoder.json.JsonRowEncoder;
import io.prestosql.plugin.kafka.encoder.json.JsonRowEncoderFactory;
import io.prestosql.plugin.kafka.encoder.raw.RawRowEncoder;
import io.prestosql.plugin.kafka.encoder.raw.RawRowEncoderFactory;

import static com.google.inject.Scopes.SINGLETON;

public class EncoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        MapBinder<String, RowEncoderFactory> encoderFactoriesByName = MapBinder.newMapBinder(binder, String.class, RowEncoderFactory.class);

        encoderFactoriesByName.addBinding(AvroRowEncoder.NAME).to(AvroRowEncoderFactory.class).in(SINGLETON);
        encoderFactoriesByName.addBinding(CsvRowEncoder.NAME).to(CsvRowEncoderFactory.class).in(SINGLETON);
        encoderFactoriesByName.addBinding(RawRowEncoder.NAME).to(RawRowEncoderFactory.class).in(SINGLETON);
        encoderFactoriesByName.addBinding(JsonRowEncoder.NAME).to(JsonRowEncoderFactory.class).in(SINGLETON);

        binder.bind(DispatchingRowEncoderFactory.class).in(SINGLETON);
    }
}
