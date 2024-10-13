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
package io.trino.plugin.kafka.schema.dynamic;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.decoder.DecoderModule;
import io.trino.plugin.kafka.encoder.EncoderModule;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DynamicTableDescriptionSupplierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DynamicTableDescriptionSupplierConfig.class);
        binder.bind(TableDescriptionSupplier.class)
        .toProvider(DynamicTableDescriptionSupplier.Factory.class)
                .in(Scopes.SINGLETON);
        install(new DecoderModule());
        install(new EncoderModule());
        binder.bind(ContentSchemaProvider.class).to(DynamicTableContentSchemaProvider.class).in(Scopes.SINGLETON);
    }
}
