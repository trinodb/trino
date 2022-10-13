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
package io.trino.plugin.hive.rubix;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.qubole.rubix.prestosql.CachingPrestoDistributedFileSystem;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.authentication.HdfsAuthenticationConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class RubixModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RubixConfig.class);
        configBinder(binder).bindConfig(HdfsAuthenticationConfig.class);
        binder.bind(RubixConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(RubixInitializer.class).in(Scopes.SINGLETON);
        // Make initialization of Rubix happen just once.
        // Alternative initialization via @PostConstruct in RubixInitializer
        // would be called multiple times by Guice (RubixInitializer is transient
        // dependency for many objects) whenever initialization error happens
        // (Guice doesn't fail-fast)
        binder.bind(RubixStarter.class).asEagerSingleton();
        newOptionalBinder(binder, RubixHdfsInitializer.class)
                .setDefault().to(DefaultRubixHdfsInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().to(RubixConfigurationInitializer.class).in(Scopes.SINGLETON);
    }

    private static class RubixStarter
    {
        @Inject
        private RubixStarter(RubixInitializer rubixInitializer, Set<DynamicConfigurationProvider> configProviders)
        {
            checkArgument(configProviders.size() == 1, "Rubix cache does not work with dynamic configuration providers");
            rubixInitializer.initializeRubix();
        }
    }

    @VisibleForTesting
    static class DefaultRubixHdfsInitializer
            implements RubixHdfsInitializer
    {
        private static final String RUBIX_DISTRIBUTED_FS_CLASS_NAME = CachingPrestoDistributedFileSystem.class.getName();

        private final boolean hdfsImpersonationEnabled;

        @Inject
        public DefaultRubixHdfsInitializer(HdfsAuthenticationConfig authenticationConfig)
        {
            this.hdfsImpersonationEnabled = requireNonNull(authenticationConfig, "authenticationConfig is null").isHdfsImpersonationEnabled();
        }

        @Override
        public void initializeConfiguration(Configuration config)
        {
            checkArgument(!hdfsImpersonationEnabled, "HDFS impersonation is not compatible with Hive caching");
            config.set("fs.hdfs.impl", RUBIX_DISTRIBUTED_FS_CLASS_NAME);
        }
    }
}
