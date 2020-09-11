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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.hive.ForHiveMetastore;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.authentication.HadoopAuthentication;
import io.prestosql.plugin.hive.authentication.HiveMetastoreAuthentication;
import io.prestosql.plugin.hive.authentication.MetastoreKerberosConfig;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationConfig.ThriftMetastoreAuthenticationType;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.hive.authentication.AuthenticationModules.createCachingKerberosHadoopAuthentication;

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
        ThriftMetastoreAuthenticationType type = buildConfigObject(ThriftMetastoreAuthenticationConfig.class).getAuthenticationType();
        switch (type) {
            case NONE:
                return new NoHiveMetastoreAuthenticationModule();
            case KERBEROS:
                return new KerberosHiveMetastoreAuthenticationModule();
        }
        throw new AssertionError("Unknown authentication type: " + type);
    }

    public static class NoHiveMetastoreAuthenticationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(HiveMetastoreAuthentication.class).to(NoHiveMetastoreAuthentication.class).in(SINGLETON);
        }
    }

    public static class KerberosHiveMetastoreAuthenticationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(HiveMetastoreAuthentication.class).to(KerberosHiveMetastoreAuthentication.class).in(SINGLETON);
            configBinder(binder).bindConfig(MetastoreKerberosConfig.class);
        }

        @Provides
        @Singleton
        @ForHiveMetastore
        public HadoopAuthentication createHadoopAuthentication(MetastoreKerberosConfig config, HdfsConfigurationInitializer updater)
        {
            String principal = config.getHiveMetastoreClientPrincipal();
            String keytabLocation = config.getHiveMetastoreClientKeytab();
            return createCachingKerberosHadoopAuthentication(principal, keytabLocation, updater);
        }
    }
}
