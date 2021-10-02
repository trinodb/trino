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
import com.google.inject.Scopes;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.ForRecordingHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastoreModule;
import io.trino.plugin.hive.metastore.recording.RecordingHiveMetastoreModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ThriftMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        OptionalBinder.newOptionalBinder(binder, ThriftMetastoreClientFactory.class)
                .setDefault().to(DefaultThriftMetastoreClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(MetastoreLocator.class).to(StaticMetastoreLocator.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);
        configBinder(binder).bindConfig(ThriftMetastoreConfig.class);

        binder.bind(ThriftMetastore.class).to(ThriftHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ThriftMetastore.class)
                .as(generator -> generator.generatedNameOf(ThriftHiveMetastore.class));

        binder.bind(HiveMetastore.class)
                .annotatedWith(ForRecordingHiveMetastore.class)
                .to(BridgingHiveMetastore.class)
                .in(Scopes.SINGLETON);

        install(new RecordingHiveMetastoreModule());
        install(new CachingHiveMetastoreModule());

        install(new ThriftMetastoreAuthenticationModule());
    }
}
