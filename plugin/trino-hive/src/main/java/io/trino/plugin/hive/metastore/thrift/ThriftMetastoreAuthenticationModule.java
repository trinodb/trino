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
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.authentication.HadoopAuthentication;
import io.trino.plugin.base.authentication.KerberosConfiguration;
import io.trino.plugin.hive.ForHiveMetastore;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.hdfs.authentication.AuthenticationModules.createCachingKerberosHadoopAuthentication;

public class ThriftMetastoreAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(getAuthenticationModule());
    }

    private Module getAuthenticationModule()
    {
        return switch (buildConfigObject(ThriftMetastoreAuthenticationConfig.class).getAuthenticationType()) {
            case NONE -> new NoHiveMetastoreAuthenticationModule();
            case KERBEROS -> new KerberosHiveMetastoreAuthenticationModule();
        };
    }

    public static class NoHiveMetastoreAuthenticationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(IdentityAwareMetastoreClientFactory.class).to(UgiBasedMetastoreClientFactory.class).in(SINGLETON);
            binder.bind(HiveMetastoreAuthentication.class).to(NoHiveMetastoreAuthentication.class).in(SINGLETON);
        }
    }

    public static class KerberosHiveMetastoreAuthenticationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(IdentityAwareMetastoreClientFactory.class).to(TokenFetchingMetastoreClientFactory.class).in(SINGLETON);
            binder.bind(HiveMetastoreAuthentication.class).to(KerberosHiveMetastoreAuthentication.class).in(SINGLETON);
            configBinder(binder).bindConfig(MetastoreKerberosConfig.class);
        }

        @Provides
        @Singleton
        @ForHiveMetastore
        public HadoopAuthentication createHadoopAuthentication(MetastoreKerberosConfig config, HdfsConfigurationInitializer updater)
        {
            String principal = config.getHiveMetastoreClientPrincipal();
            KerberosConfiguration.Builder builder = new KerberosConfiguration.Builder()
                    .withKerberosPrincipal(principal);
            config.getHiveMetastoreClientKeytab().ifPresent(builder::withKeytabLocation);
            config.getHiveMetastoreClientCredentialCacheLocation().ifPresent(builder::withCredentialCacheLocation);
            return createCachingKerberosHadoopAuthentication(builder.build(), updater);
        }
    }
}
