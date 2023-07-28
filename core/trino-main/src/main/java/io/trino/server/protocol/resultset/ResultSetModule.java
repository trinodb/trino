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
package io.trino.server.protocol.resultset;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.client.QueryResultSetFormatResolver;
import io.trino.client.QueryResults;
import io.trino.client.ResultSetJsonModule;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

public class ResultSetModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, ResultSetProducer.class).addBinding()
                .to(JsonArrayResultSetProducer.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, ResultSetProducerFactory.class)
                .setDefault()
                .to(DefaultResultSetProducerFactory.class)
                .in(Scopes.SINGLETON);
    }

    @Provides
    private QueryResultSetFormatResolver getResultSetProducers(Set<ResultSetProducer> handlers)
    {
        List<Class<? extends QueryResults>> classes = handlers.stream()
                .map(ResultSetProducer::produces)
                .collect(Collectors.toList());
        return new QueryResultSetFormatResolver(classes);
    }

    @ProvidesIntoSet
    @Singleton
    public static Module resultSetJacksonModule(QueryResultSetFormatResolver formatResolver)
    {
        return new ResultSetJsonModule(formatResolver);
    }
}
