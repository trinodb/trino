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
package io.trino.security;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.util.LoggingInvocationHandler;
import io.trino.spi.security.GroupProvider;
import io.trino.tracing.ForTracing;
import io.trino.tracing.TracingAccessControl;

import static com.google.common.reflect.Reflection.newProxy;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AccessControlModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(AccessControlConfig.class);
        newOptionalBinder(binder, Key.get(String.class, DefaultSystemAccessControlName.class)).setDefault().toInstance(DefaultSystemAccessControl.NAME);
        binder.bind(AccessControlManager.class).in(Scopes.SINGLETON);
        binder.bind(AccessControl.class).to(TracingAccessControl.class);
        binder.bind(GroupProviderManager.class).in(Scopes.SINGLETON);
        binder.bind(GroupProvider.class).to(GroupProviderManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(AccessControlManager.class).withGeneratedName();
    }

    @Provides
    @Singleton
    @ForTracing
    public AccessControl createAccessControl(AccessControlManager accessControlManager)
    {
        Logger logger = Logger.get(AccessControl.class);

        AccessControl loggingInvocationsAccessControl = newProxy(
                AccessControl.class,
                new LoggingInvocationHandler(accessControlManager, logger::debug));

        return ForwardingAccessControl.of(() -> {
            if (logger.isDebugEnabled()) {
                return loggingInvocationsAccessControl;
            }
            return accessControlManager;
        });
    }
}
