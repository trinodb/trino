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
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.inject.Decorator;
import io.trino.spi.NodeManager;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.trino.plugin.base.inject.Decorator.nonModifyingDecorator;

public final class ReusableConnectionFactoryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, JdbcQueryEventListener.class)
                .addBinding()
                .to(ReusableConnectionFactory.DelegatingListener.class)
                .in(Scopes.SINGLETON);

        binder.bind(ReusableConnectionFactory.DelegatingListener.class)
                .in(Scopes.SINGLETON); // this will be the same instance as provided into set
    }

    @ProvidesIntoSet
    @Singleton
    @ForBaseJdbc
    public static Decorator<ConnectionFactory> getReusableConnectionFactoryDecorator(NodeManager nodeManager, ReusableConnectionFactory.DelegatingListener listener)
    {
        if (nodeManager.getCurrentNode().isCoordinator()) {
            return new ReusableConnectionFactory.FactoryDecorator(listener);
        }

        return nonModifyingDecorator();
    }
}
