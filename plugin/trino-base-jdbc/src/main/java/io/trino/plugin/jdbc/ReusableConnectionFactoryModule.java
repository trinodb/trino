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
package io.trino.plugin.jdbc;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.spi.NodeManager;

import javax.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public final class ReusableConnectionFactoryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ConnectionFactory.class)
                .annotatedWith(ForReusableConnectionFactory.class)
                .to(LazyConnectionFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(ReusableConnectionFactory.class).in(Scopes.SINGLETON);
        newSetBinder(binder, JdbcQueryEventListener.class)
                .addBinding()
                .to(Key.get(ReusableConnectionFactory.class))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static ConnectionFactory getReusableConnectionFactory(
            NodeManager nodeManager,
            @ForReusableConnectionFactory ConnectionFactory delegate,
            ReusableConnectionFactory reusableConnectionFactory)
    {
        if (nodeManager.getCurrentNode().isCoordinator()) {
            return reusableConnectionFactory;
        }

        return delegate;
    }
}
