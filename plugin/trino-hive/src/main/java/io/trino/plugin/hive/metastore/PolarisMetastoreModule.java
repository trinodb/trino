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
package io.trino.plugin.hive.metastore;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.metastore.polaris.PolarisMetastoreFactory;
import io.trino.plugin.hive.AllowHiveTableRename;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

/**
 * Hive-specific Guice module for configuring Polaris metastore dependencies.
 * This module installs the core Polaris metastore module and adds Hive-specific bindings.
 */
public class PolarisMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Install the core Polaris metastore module from lib/trino-metastore
        install(new io.trino.metastore.polaris.PolarisMetastoreModule());

        // Add Hive-specific bindings
        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(PolarisMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        // Polaris supports table rename operations
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);
    }
}
