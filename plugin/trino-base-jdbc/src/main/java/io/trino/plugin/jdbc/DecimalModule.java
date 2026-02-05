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
package io.trino.plugin.jdbc;

import com.google.inject.Binder;
import com.google.inject.Module;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

// TODO this class should eventually be removed
public class DecimalModule
        implements Module
{
    private final boolean deprecated;

    /**
     * Creates a module that installs decimal configs with user-visible deprecation.
     * This should be used when connector wants to migrate away from the decimal configs,
     * most likely towards the new high-precision decimal type.
     */
    public static DecimalModule withDeprecatedConfigs()
    {
        return new DecimalModule(true);
    }

    /**
     * Creates a module that installs decimal configs without user-visible deprecation.
     * This constructor is deprecated, connectors should migrate to {@link #withDeprecatedConfigs()}
     * and eventually stop using this class.
     */
    @Deprecated
    public DecimalModule()
    {
        this(false);
    }

    private DecimalModule(boolean deprecated)
    {
        this.deprecated = deprecated;
    }

    @Override
    public void configure(Binder binder)
    {
        Class<? extends DecimalConfig> configClass = deprecated ? LegacyDecimalConfig.class : DeprecatedDecimalConfig.class;
        configBinder(binder).bindConfig(configClass);
        binder.bind(DecimalConfig.class).to(configClass);
        bindSessionPropertiesProvider(binder, DecimalSessionSessionProperties.class);
    }
}
