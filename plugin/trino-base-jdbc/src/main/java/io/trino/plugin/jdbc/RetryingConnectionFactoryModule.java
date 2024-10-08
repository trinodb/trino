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

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.trino.plugin.jdbc.RetryingConnectionFactory.DefaultRetryStrategy;
import io.trino.plugin.jdbc.RetryingConnectionFactory.RetryStrategy;
import io.trino.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class RetryingConnectionFactoryModule
        extends AbstractModule
{
    @Override
    public void configure()
    {
        bind(ConnectionFactory.class).annotatedWith(ForRetrying.class).to(Key.get(StatisticsAwareConnectionFactory.class)).in(Scopes.SINGLETON);
        bind(RetryingConnectionFactory.class).in(Scopes.SINGLETON);
        newSetBinder(binder(), RetryStrategy.class).addBinding().to(DefaultRetryStrategy.class).in(Scopes.SINGLETON);
    }
}
