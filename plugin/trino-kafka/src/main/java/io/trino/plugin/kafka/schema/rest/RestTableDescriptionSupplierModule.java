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
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.decoder.protobuf.DescriptorProvider;
import io.trino.decoder.protobuf.FileDescriptorProvider;
import io.trino.plugin.kafka.encoder.EncoderModule;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.plugin.kafka.schema.ProtobufAnySupportConfig;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.plugin.kafka.schema.file.FileReadContentSchemaProvider;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class RestTableDescriptionSupplierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(RestTableDescriptionSupplierConfig.class);

        binder.bind(TableDescriptionSupplier.class).to(RestTableDescriptionSupplier.class).in(SINGLETON);

        install(new RestPlainAvroDecoderModule());
        binder.bind(ContentSchemaProvider.class).to(FileReadContentSchemaProvider.class).in(SINGLETON);

        install(new EncoderModule());

        configBinder(binder).bindConfig(ProtobufAnySupportConfig.class);
        if (buildConfigObject(ProtobufAnySupportConfig.class).isProtobufAnySupportEnabled()) {
            install(new FileDescriptorProviderModule());
        }
    }

    private static class FileDescriptorProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(DescriptorProvider.class).to(FileDescriptorProvider.class).in(SINGLETON);
        }
    }
}
