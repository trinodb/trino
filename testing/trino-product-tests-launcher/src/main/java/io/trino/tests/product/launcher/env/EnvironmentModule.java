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
package io.trino.tests.product.launcher.env;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.HadoopKerberos;
import io.trino.tests.product.launcher.env.common.HadoopKerberosKms;
import io.trino.tests.product.launcher.env.common.HadoopKerberosKmsWithImpersonation;
import io.trino.tests.product.launcher.env.common.HydraIdentityProvider;
import io.trino.tests.product.launcher.env.common.Kafka;
import io.trino.tests.product.launcher.env.common.KafkaSaslPlaintext;
import io.trino.tests.product.launcher.env.common.KafkaSsl;
import io.trino.tests.product.launcher.env.common.Kerberos;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.jdk.JdkProvider;
import io.trino.tests.product.launcher.env.jdk.JdkProviderFactory;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import java.io.File;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.tests.product.launcher.Configurations.findConfigsByBasePackage;
import static io.trino.tests.product.launcher.Configurations.findEnvironmentsByBasePackage;
import static io.trino.tests.product.launcher.Configurations.findJdkProvidersByBasePackage;
import static io.trino.tests.product.launcher.Configurations.nameForConfigClass;
import static io.trino.tests.product.launcher.Configurations.nameForEnvironmentClass;
import static io.trino.tests.product.launcher.Configurations.nameForJdkProvider;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public final class EnvironmentModule
        implements Module
{
    private static final String LAUNCHER_PACKAGE = "io.trino.tests.product.launcher";
    private static final String ENVIRONMENT_PACKAGE = LAUNCHER_PACKAGE + ".env.environment";
    private static final String CONFIG_PACKAGE = LAUNCHER_PACKAGE + ".env.configs";
    private static final String JDK_PACKAGE = LAUNCHER_PACKAGE + ".env.jdk";

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
        binder.bind(EnvironmentFactory.class).in(SINGLETON);
        binder.bind(EnvironmentConfigFactory.class).in(SINGLETON);
        binder.bind(EnvironmentOptions.class).toInstance(environmentOptions);

        binder.bind(Hadoop.class).in(SINGLETON);
        binder.bind(HadoopKerberos.class).in(SINGLETON);
        binder.bind(HadoopKerberosKms.class).in(SINGLETON);
        binder.bind(HadoopKerberosKmsWithImpersonation.class).in(SINGLETON);
        binder.bind(HydraIdentityProvider.class).in(SINGLETON);
        binder.bind(Kafka.class).in(SINGLETON);
        binder.bind(KafkaSsl.class).in(SINGLETON);
        binder.bind(KafkaSaslPlaintext.class).in(SINGLETON);
        binder.bind(Standard.class).in(SINGLETON);
        binder.bind(StandardMultinode.class).in(SINGLETON);
        binder.bind(Kerberos.class).in(SINGLETON);
        binder.bind(Minio.class).in(SINGLETON);

        MapBinder<String, EnvironmentProvider> environments = newMapBinder(binder, String.class, EnvironmentProvider.class);
        findEnvironmentsByBasePackage(ENVIRONMENT_PACKAGE).forEach(clazz -> environments.addBinding(nameForEnvironmentClass(clazz)).to(clazz).in(SINGLETON));

        MapBinder<String, EnvironmentConfig> environmentConfigs = newMapBinder(binder, String.class, EnvironmentConfig.class);
        findConfigsByBasePackage(CONFIG_PACKAGE).forEach(clazz -> environmentConfigs.addBinding(nameForConfigClass(clazz)).to(clazz).in(SINGLETON));

        binder.install(additionalEnvironments);

        binder.bind(JdkProviderFactory.class).in(SINGLETON);
        MapBinder<String, JdkProvider> providers = newMapBinder(binder, String.class, JdkProvider.class);
        findJdkProvidersByBasePackage(JDK_PACKAGE).forEach(clazz -> providers.addBinding(nameForJdkProvider(clazz)).to(clazz).in(SINGLETON));
    }

    @Provides
    @Singleton
    public EnvironmentConfig provideEnvironmentConfig(EnvironmentOptions options, EnvironmentConfigFactory factory)
    {
        return factory.getConfig(options.config);
    }

    @Provides
    @Singleton
    public PortBinder providePortBinder(EnvironmentOptions options)
    {
        if (options.bindPorts) {
            if (options.bindPortsBase > 0) {
                return new PortBinder.ShiftingPortBinder(new PortBinder.FixedPortBinder(), options.bindPortsBase);
            }

            return new PortBinder.FixedPortBinder();
        }

        return new PortBinder.DefaultPortBinder();
    }

    @Provides
    @Singleton
    @ServerPackage
    public File provideServerPackage(EnvironmentOptions options)
    {
        // fallback to dummy - nonNull to prevent injection errors when listing environments
        return requireNonNullElse(options.serverPackage, new File("dummy.tar.gz"));
    }

    @Provides
    @Singleton
    public JdkProvider provideJdkProvider(JdkProviderFactory factory, EnvironmentOptions options)
    {
        return factory.get(requireNonNull(options.jdkProvider, "options.jdkProvider is null"));
    }

    @Provides
    @Singleton
    @Debug
    public boolean provideDebug(EnvironmentOptions options)
    {
        return options.debug;
    }
}
