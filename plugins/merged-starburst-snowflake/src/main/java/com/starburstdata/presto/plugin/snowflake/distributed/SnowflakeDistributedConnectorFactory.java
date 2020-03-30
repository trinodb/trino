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
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.plugin.jdbc.JdbcDiagnosticModule;
import io.prestosql.plugin.jdbc.credential.CredentialProviderModule;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SnowflakeDistributedConnectorFactory
        implements ConnectorFactory
{
    private final String name;

    public SnowflakeDistributedConnectorFactory(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new SnowflakeHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            Bootstrap app = new Bootstrap(
                    new SnowflakeDistributedModule(catalogName),
                    new CredentialProviderModule(),
                    new JdbcDiagnosticModule(catalogName),
                    new MBeanServerModule(),
                    new MBeanModule(),
                    binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(Connector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
