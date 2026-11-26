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
package io.trino.plugin.cassandra.tls;

import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.trino.plugin.cassandra.CassandraClientConfig;
import io.trino.plugin.cassandra.CassandraSessionConfigurator;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.cassandra.tls.CassandraTlsEngineFactory.createSslEngineFactory;

public class CassandraTlsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(CassandraTlsConfig.class);
    }

    @ProvidesIntoSet
    @Singleton
    public CassandraSessionConfigurator tlsConfigurator(CassandraTlsConfig config, CassandraClientConfig clientConfig)
    {
        SslEngineFactory sslEngineFactory = createSslEngineFactory(config, clientConfig.getContactPoints());
        return builder -> builder.withSslEngineFactory(sslEngineFactory);
    }
}
