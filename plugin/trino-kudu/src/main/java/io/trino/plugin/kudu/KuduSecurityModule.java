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
package io.trino.plugin.kudu;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.authentication.CachingKerberosAuthentication;
import io.trino.plugin.base.authentication.KerberosAuthentication;
import io.trino.plugin.base.authentication.KerberosConfiguration;
import io.trino.plugin.kudu.schema.NoSchemaEmulation;
import io.trino.plugin.kudu.schema.SchemaEmulation;
import io.trino.plugin.kudu.schema.SchemaEmulationByTableNameConvention;
import org.apache.kudu.client.KuduClient;

import java.util.function.Function;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.SystemProperties.setJavaSecurityKrb5Conf;
import static io.trino.plugin.kudu.KuduAuthenticationConfig.KuduAuthenticationType.KERBEROS;
import static io.trino.plugin.kudu.KuduAuthenticationConfig.KuduAuthenticationType.NONE;
import static org.apache.kudu.client.KuduClient.KuduClientBuilder;

public class KuduSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(KuduAuthenticationConfig.class);

        install(conditionalModule(
                KuduAuthenticationConfig.class,
                authenticationConfig -> authenticationConfig.getAuthenticationType() == NONE,
                new NoneAuthenticationModule()));

        install(conditionalModule(
                KuduAuthenticationConfig.class,
                authenticationConfig -> authenticationConfig.getAuthenticationType() == KERBEROS,
                new KerberosAuthenticationModule()));
    }

    private static class NoneAuthenticationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
        }

        @Provides
        @Singleton
        public static KuduClientSession createKuduClientSession(KuduClientConfig config)
        {
            return KuduSecurityModule.createKuduClientSession(config,
                    builder -> new PassthroughKuduClient(builder.build()));
        }
    }

    private static class KerberosAuthenticationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            configBinder(binder).bindConfig(KuduKerberosConfig.class);
        }

        @Provides
        @Singleton
        public static KuduClientSession createKuduClientSession(KuduClientConfig config, KuduKerberosConfig kuduKerberosConfig)
        {
            return KuduSecurityModule.createKuduClientSession(config,
                    builder -> {
                        kuduKerberosConfig.getKuduPrincipalPrimary().ifPresent(builder::saslProtocolName);
                        setJavaSecurityKrb5Conf(kuduKerberosConfig.getConfig().getAbsolutePath());
                        KerberosAuthentication kerberosAuthentication = new KerberosAuthentication(
                                new KerberosConfiguration.Builder()
                                        .withKerberosPrincipal(kuduKerberosConfig.getClientPrincipal())
                                        .withKeytabLocation(kuduKerberosConfig.getClientKeytab().getAbsolutePath())
                                        .build());
                        CachingKerberosAuthentication cachingKerberosAuthentication = new CachingKerberosAuthentication(kerberosAuthentication);
                        return new KerberizedKuduClient(builder, cachingKerberosAuthentication);
                    });
        }
    }

    private static KuduClientSession createKuduClientSession(KuduClientConfig config, Function<KuduClientBuilder, KuduClientWrapper> kuduClientFactory)
    {
        KuduClient.KuduClientBuilder builder = new KuduClientBuilder(config.getMasterAddresses());
        builder.defaultAdminOperationTimeoutMs(config.getDefaultAdminOperationTimeout().toMillis());
        builder.defaultOperationTimeoutMs(config.getDefaultOperationTimeout().toMillis());
        if (config.isDisableStatistics()) {
            builder.disableStatistics();
        }
        KuduClientWrapper client = kuduClientFactory.apply(builder);

        SchemaEmulation strategy;
        if (config.isSchemaEmulationEnabled()) {
            strategy = new SchemaEmulationByTableNameConvention(config.getSchemaEmulationPrefix());
        }
        else {
            strategy = new NoSchemaEmulation();
        }
        return new KuduClientSession(client, strategy, config.isAllowLocalScheduling());
    }
}
