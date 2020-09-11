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
package io.prestosql.tests.product.launcher.env;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Kafka;
import io.prestosql.tests.product.launcher.env.common.Kerberos;
import io.prestosql.tests.product.launcher.env.common.KerberosKms;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.testcontainers.PortBinder;

import java.io.File;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.prestosql.tests.product.launcher.env.Environments.nameForConfigClass;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public final class EnvironmentModule
        implements Module
{
    public static final String BASE_PACKAGE = "io.prestosql.tests.product.launcher.env.environment";
    public static final String BASE_CONFIG_PACKAGE = "io.prestosql.tests.product.launcher.env.configs";
    private final EnvironmentOptions environmentOptions;
    private final Module additionalEnvironments;

    public EnvironmentModule(EnvironmentOptions environmentOptions, Module additionalEnvironments)
    {
        this.environmentOptions = requireNonNull(environmentOptions, "environmentOptions is null");
        this.additionalEnvironments = requireNonNull(additionalEnvironments, "additionalEnvironments is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(PortBinder.class);
        binder.bind(EnvironmentFactory.class);
        binder.bind(EnvironmentConfigFactory.class);
        binder.bind(Standard.class);
        binder.bind(Hadoop.class);
        binder.bind(Kerberos.class);
        binder.bind(KerberosKms.class);
        binder.bind(Kafka.class);
        binder.bind(EnvironmentOptions.class).toInstance(environmentOptions);

        MapBinder<String, EnvironmentProvider> environments = newMapBinder(binder, String.class, EnvironmentProvider.class);
        Environments.findByBasePackage(BASE_PACKAGE).forEach(clazz -> environments.addBinding(Environments.nameForClass(clazz)).to(clazz));

        MapBinder<String, EnvironmentConfig> environmentConfigs = newMapBinder(binder, String.class, EnvironmentConfig.class);
        Environments.findConfigsByBasePackage(BASE_CONFIG_PACKAGE).forEach(clazz -> environmentConfigs.addBinding(nameForConfigClass(clazz)).to(clazz));

        binder.install(additionalEnvironments);
    }

    @Inject
    @Provides
    @Singleton
    public EnvironmentConfig provideEnvironmentConfig(EnvironmentOptions options, EnvironmentConfigFactory factory)
    {
        return factory.getConfig(options.config);
    }

    @Provides
    @Singleton
    @Inject
    @ServerPackage
    public File provideServerPackage(EnvironmentOptions options)
    {
        // fallback to dummy - nonNull to prevent injection errors when listing environments
        return requireNonNullElse(options.serverPackage, new File("dummy.tar.gz"));
    }
}
