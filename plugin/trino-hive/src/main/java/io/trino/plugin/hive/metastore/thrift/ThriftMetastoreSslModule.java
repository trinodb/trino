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
package io.trino.plugin.hive.metastore.thrift;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.ForHiveMetastore;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;

public class ThriftMetastoreSslModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                ThriftMetastoreConfig.class,
                ThriftMetastoreConfig::isTlsEnabled,
                new SslContextModule(),
                new NoSslContextModule()));
    }

    public static class SslContextModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(ThriftMetastoreSslConfig.class);
        }

        @Provides
        @Singleton
        @ForHiveMetastore
        public Optional<SSLContext> provideSslContext(ThriftMetastoreSslConfig config)
                throws GeneralSecurityException, IOException
        {
            return Optional.of(createSSLContext(
                    Optional.ofNullable(config.getKeystorePath()),
                    Optional.ofNullable(config.getKeystorePassword()),
                    Optional.ofNullable(config.getTruststorePath()),
                    Optional.ofNullable(config.getTruststorePassword())));
        }
    }

    public static class NoSslContextModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        @ForHiveMetastore
        public Optional<SSLContext> provideSslContext()
        {
            return Optional.empty();
        }
    }
}
