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
package io.trino.server.protocol.data;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.client.QueryData;
import io.trino.client.QueryDataFormatResolver;
import io.trino.client.QueryDataJsonSerializationModule;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

public class QueryDataFormatsModule
        extends AbstractConfigurationAwareModule
{
    @ProvidesIntoSet
    @Singleton
    public static Module queryDataSerializationModule(QueryDataFormatResolver formatResolver)
    {
        return new QueryDataJsonSerializationModule(formatResolver);
    }

    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, QueryDataProducer.class).addBinding()
                .to(InlineJsonQueryDataProducer.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, QueryDataProducerFactory.class)
                .setDefault()
                .to(DefaultQueryDataProducerFactory.class)
                .in(Scopes.SINGLETON);
    }

    @Provides
    private QueryDataFormatResolver getResultSetProducers(Set<QueryDataProducer> handlers)
    {
        List<Class<? extends QueryData>> classes = handlers.stream()
                .map(QueryDataProducer::produces)
                .collect(Collectors.toList());
        return new QueryDataFormatResolver(classes);
    }
}
