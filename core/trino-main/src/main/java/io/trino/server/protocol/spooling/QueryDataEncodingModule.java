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
package io.trino.server.protocol.spooling;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.protocol.spooling.encoding.ArrowCompressionFactory;
import io.trino.server.protocol.spooling.encoding.ArrowQueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.JsonQueryDataEncoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class QueryDataEncodingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        Multibinder<QueryDataEncoder.Factory> encoderFactories = newSetBinder(binder, QueryDataEncoder.Factory.class);
        QueryDataEncodingConfig config = buildConfigObject(QueryDataEncodingConfig.class);

        // json + compressed variants
        if (config.isJsonEnabled()) {
            encoderFactories.addBinding().to(JsonQueryDataEncoder.Factory.class).in(Scopes.SINGLETON);
        }
        if (config.isJsonZstdEnabled()) {
            encoderFactories.addBinding().to(JsonQueryDataEncoder.ZstdFactory.class).in(Scopes.SINGLETON);
        }
        if (config.isJsonLz4Enabled()) {
            encoderFactories.addBinding().to(JsonQueryDataEncoder.Lz4Factory.class).in(Scopes.SINGLETON);
        }
        if (config.isArrowIpcEnabled() || config.isArrowIpcZstdEnabled()) {
            binder.bind(BufferAllocator.class).toInstance(new RootAllocator());
            binder.bind(CompressionCodec.Factory.class).to(ArrowCompressionFactory.class).in(Scopes.SINGLETON);

            if (config.isArrowIpcEnabled()) {
                encoderFactories.addBinding().to(ArrowQueryDataEncoder.Factory.class).in(Scopes.SINGLETON);
            }
            if (config.isArrowIpcZstdEnabled()) {
                encoderFactories.addBinding().to(ArrowQueryDataEncoder.ZstdFactory.class).in(Scopes.SINGLETON);
            }
        }
        binder.bind(QueryDataEncoders.class).in(Scopes.SINGLETON);
    }
}
