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
package io.trino.plugin.hive.metastore.dlf;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.DefaultThriftMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.MetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.StaticMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.StaticMetastoreLocator;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationModule;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.TokenDelegationThriftMetastoreFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AlibabaDlfMetaStoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindHiveClient(binder);
        configBinder(binder).bindConfig(AlibabaDlfMetaStoreConfig.class);

        binder.bind(AlibabaDlfMetaStoreFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(AlibabaDlfMetaStoreFactory.class)
                .as(generator -> generator.generatedNameOf(AlibabaDlfMetaStoreClient.class));
        binder.bind(AlibabaDlfMetaStoreClient.class).in(Scopes.SINGLETON);

        binder.bind(ProxyTrinoClientFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ProxyTrinoClientFactory.class)
                .as(generator -> generator.generatedNameOf(ProxyTrinoClient.class));
        binder.bind(ProxyTrinoClient.class).in(Scopes.SINGLETON);

        binder.bind(HiveMetastoreFactory.class)
                .annotatedWith(RawHiveMetastoreFactory.class)
                .to(ProxyTrinoClientFactory.class)
                .in(Scopes.SINGLETON);
    }

    private void bindHiveClient(Binder binder)
    {
        OptionalBinder.newOptionalBinder(binder, ThriftMetastoreClientFactory.class)
                .setDefault().to(DefaultThriftMetastoreClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(MetastoreLocator.class).to(StaticMetastoreLocator.class).in(Scopes.SINGLETON);
        binder.bind(TokenDelegationThriftMetastoreFactory.class);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);
        configBinder(binder).bindConfig(ThriftMetastoreConfig.class);

        binder.bind(ThriftMetastoreFactory.class).to(ThriftHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ThriftMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(ThriftHiveMetastore.class));

        binder.bind(BridgingHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(BridgingHiveMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(BridgingHiveMetastore.class));

        install(new ThriftMetastoreAuthenticationModule());
    }
}
