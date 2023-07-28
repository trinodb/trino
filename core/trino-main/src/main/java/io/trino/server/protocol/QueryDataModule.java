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
package io.trino.server.protocol;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.client.QueryDataJsonModule;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class QueryDataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(QueryDataProducerFactory.class)
                .to(ProtocolSwitchingQueryDataProducer.class)
                .in(Scopes.SINGLETON);

        binder.bind(LegacyQueryDataProducer.class)
                .in(Scopes.SINGLETON);

        binder.bind(EncodedQueryDataProducer.class)
                .in(Scopes.SINGLETON);

        newSetBinder(binder, QueryDataEncoder.class)
                .addBinding()
                .to(JsonQueryDataEncoder.class);
    }

    @ProvidesIntoSet
    @Singleton
    // Fully qualified so not to confuse with Guice's Module
    public static com.fasterxml.jackson.databind.Module queryDataJsonModule()
    {
        return new QueryDataJsonModule();
    }
}
