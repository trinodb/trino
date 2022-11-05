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
package io.trino.plugin.hudi.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.ConnectorAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;
import io.trino.plugin.hive.security.AllowAllSecurityModule;
import io.trino.plugin.hudi.HudiConfig;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class HudiSecurityModule
        extends AbstractConfigurationAwareModule
{
    public static final String FILE = "file";
    public static final String READ_ONLY = "read-only";
    public static final String ALLOW_ALL = "allow-all";

    public static final String SYSTEM = "system";

    @Override
    protected void setup(Binder binder)
    {
        install(new ConnectorAccessControlModule());
        bindSecurityModule(ALLOW_ALL, combine(
                new AllowAllSecurityModule(),
                new StaticAccessControlMetadataModule()));
        bindSecurityModule(READ_ONLY, combine(
                new ReadOnlySecurityModule(),
                new StaticAccessControlMetadataModule()));
        bindSecurityModule(FILE, combine(
                new FileBasedAccessControlModule(),
                new StaticAccessControlMetadataModule()));
        newOptionalBinder(binder, HudiAccessControlMetadataFactory.class).setDefault().toInstance(HudiAccessControlMetadataFactory.SYSTEM);
    }

    private void bindSecurityModule(String name, Module module)
    {
        install(conditionalModule(
                HudiConfig.class,
                config -> name.equalsIgnoreCase(config.getSecuritySystem()),
                module));
    }

    private static class StaticAccessControlMetadataModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(HudiAccessControlMetadataFactory.class).toInstance(HudiAccessControlMetadataFactory.DEFAULT);
        }
    }
}
